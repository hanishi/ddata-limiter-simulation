package com.example

import org.apache.pekko.actor.Address
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ ActorRef, Behavior }
import org.apache.pekko.cluster.ddata.typed.scaladsl.{ DistributedData, Replicator }
import org.apache.pekko.cluster.ddata.{
  PNCounterMap,
  PNCounterMapKey,
  ReplicatedData,
  SelfUniqueAddress
}
import org.apache.pekko.cluster.typed.Cluster

import java.lang.management.{ ManagementFactory, MemoryMXBean }
import scala.concurrent.duration.*

object RateLimiterDData {
  def apply(prefix: String, settings: Settings): Behavior[Command] = Behaviors.setup { ctx =>
    require(
      settings.buckets > 0 && (settings.buckets & (settings.buckets - 1)) == 0,
      "buckets must be a power of two"
    )
    require(settings.keepWindows >= 1, "keepWindows must be >= 1")
    require(settings.windowMs > 0, "windowMs must be > 0")
    require(settings.capacity >= 0, "capacity must be >= 0")
    require(settings.maxKeysPerBucket > 0, "maxKeysPerBucket must be > 0")
    require(settings.deleteBatchPerTick >= 0, "deleteBatchPerTick must be >= 0")
    require(settings.sweepEvery > Duration.Zero, "sweepEvery must be > 0")
    require(settings.deleteTimeout >= Duration.Zero, "deleteTimeout must be >= 0")

    given SelfUniqueAddress = DistributedData(ctx.system).selfUniqueAddress

    val cluster: Cluster = Cluster(ctx.system)

    val memoryMBean: MemoryMXBean = ManagementFactory.getMemoryMXBean

    val deleteConsistency = Replicator.WriteMajority(settings.deleteTimeout)

    def nodeKey(address: Address): String = address.host.get + ":" + address.port.get

    def wid(now: Long) = now / settings.windowMs

    def bucketOf(s: String) = java.lang.Integer.rotateLeft(s.hashCode, 13) & (settings.buckets - 1)

    def keyOf(w: Long, b: Int) = PNCounterMapKey[String](s"$prefix-$w-$b")

    def updateSucceeded[A <: ReplicatedData](rsp: Replicator.UpdateResponse[A]): Boolean =
      rsp match {
        case _: Replicator.UpdateSuccess[_] => true
        case _ => false
      }

    val node = nodeKey(cluster.selfMember.address)

    Behaviors.withTimers { timers =>
      timers.startTimerAtFixedRate(Tick, settings.sweepEvery)

      DistributedData.withReplicatorMessageAdapter[Command, PNCounterMap[String]] { repl =>
        Behaviors.receiveMessage {
          case Allow(k, replyTo) =>
            val now = System.currentTimeMillis()
            val w   = wid(now)
            val b   = bucketOf(k)
            repl.askGet(
              ask => Replicator.Get(keyOf(w, b), Replicator.ReadLocal, ask),
              rsp => InternalGetResponse(k, w, b, rsp, replyTo)
            )
            Behaviors.same
          case InternalGetResponse(k, w, b, rsp, replyTo) => // compute key from window+bucket
            val shard = keyOf(w, b)
            // current: the current counter-value for a key in this bucket
            // distinct: the number of distinct keys recorded in this bucket
            val (current, distinct) =
              rsp match { // The Replicator responses, "I have a value for PNCounterMap[String] under this key!"
                case g @ Replicator.GetSuccess(
                      `shard`
                    ) => //  so you call g.get(key) to extract the actual PNCounterMap[String] for which the keys are your rate-limiting keys
                  //  +-----------------+---------+
                  //  |      entryKey   |  count  |
                  //  +-----------------+---------+
                  //  | "ip:1.2.3.4"    |    3    |
                  //  | "ip:10.0.0.7"   |    5    |
                  //  | "imp|pubA|slot1"|    1    |
                  //  | "imp|pubB|slot2"|    7    |
                  //  +-----------------+---------+
                  val m     = g.get(shard)
                  val count = m.get(k).sum.toInt
                  val keys  = m.entries.size
                  (
                    count, // here the sum is just a neat way to say BingInt(0) or the value of Option[BigInt]
                    keys // how many distinct keys in this bucket
                  )
                case _ => (0, 0) // no value yet, so both counts and distinct keys are zero
              }
            if (current >= settings.capacity) {
              ctx.log.info("shard: {}, key: {}, count {}, num of keys: {}", shard, k, current, distinct)
              // if the current counter-value for this key is already at or above capacity, reject
              replyTo ! false
              Behaviors.same
            } else if (distinct >= settings.maxKeysPerBucket && current == 0) {
              // if the number of distinct keys in this bucket is at or above maxKeysPerBucket, and this key is not yet present (current == 0), reject
              ctx.log.info("shard: {}, key: {}, count {}, num of keys: {} ‼️", shard, k, current, distinct)
              replyTo ! false
              Behaviors.same
            } else {
              // otherwise, increment the counter for this key
              val wc =
                if (settings.writeMajority) Replicator.WriteMajority(timeout = 300.millis)
                else Replicator.WriteLocal
              repl.askUpdate(
                ask =>
                  Replicator.Update(shard, PNCounterMap.empty[String], wc, ask)(
                    _.incrementBy(k, 1)
                  ),
                rsp => InternalUpdateResponse(w, b, rsp, replyTo)
              )
              Behaviors.same
            }
          case Tick => // rotation by *not reading old windows*; no per-entry removals
            // We only handle current/previous windows. Older windows are safe to delete.
            val nowW         = wid(System.currentTimeMillis())
            val oldestToKeep = nowW - (settings.keepWindows - 1).max(0)

            val pairs = Iterator
              .iterate((oldestToKeep - 1, 0)) { case (w, b) =>
                if (b + 1 < settings.buckets) (w, b + 1) else (w - 1, 0)
              }
              .takeWhile { case (w, _) => w >= 0 }
              .take(settings.deleteBatchPerTick)

            pairs.foreach { case (w, b) =>
              repl.askDelete(
                ask =>
                  Replicator
                    .Delete(keyOf(w, b), deleteConsistency, ask),
                rsp => InternalDeleteResponse(w, b, rsp)
              )
            }
            Behaviors.same
          case InternalUpdateResponse(_, _, rsp, replyTo) =>
            replyTo ! updateSucceeded(rsp)
            Behaviors.same
          case InternalDeleteResponse(wi, bi, rsp) =>
            Behaviors.same
        }
      }
    }
  }

  sealed trait Command

  final case class Settings(
      capacity: Int, // max per key per window
      windowMs: Long                = 60_000L, // size of rolling window
      buckets: Int                  = 256, // power of 2
      writeMajority: Boolean        = false, // toggle for experiment run
      sweepEvery: FiniteDuration    = 2.minutes,
      maxKeysPerBucket: Int         = 50_000, // safety under ~100k guidance
      keepWindows: Int              = 2, //  current + previous
      deleteBatchPerTick: Int       = 64, // throttle deletes per sweep
      deleteTimeout: FiniteDuration = 500.millis
  )

  final case class Allow(key: String, replyTo: ActorRef[Boolean]) extends Command

  private final case class InternalGetResponse(
      k: String,
      w: Long,
      b: Int,
      rsp: Replicator.GetResponse[PNCounterMap[String]],
      replyTo: ActorRef[Boolean]
  ) extends Command

  private final case class InternalUpdateResponse(
      w: Long,
      b: Int,
      rsp: Replicator.UpdateResponse[PNCounterMap[String]],
      replyTo: ActorRef[Boolean]
  ) extends Command

  private final case class InternalDeleteResponse(
      w: Long,
      b: Int,
      rsp: Replicator.DeleteResponse[PNCounterMap[String]]
  ) extends Command

  private case object Tick extends Command
}
