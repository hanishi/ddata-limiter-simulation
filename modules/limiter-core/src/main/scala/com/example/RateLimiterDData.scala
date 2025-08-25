package com.example

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ ActorRef, Behavior }
import org.apache.pekko.cluster.ddata.typed.scaladsl.{ DistributedData, Replicator }
import org.apache.pekko.cluster.ddata.{
  PNCounterMap,
  PNCounterMapKey,
  ReplicatedData,
  SelfUniqueAddress
}

import scala.concurrent.duration.*

object RateLimiterDData {
  def apply(prefix: String, settings: Settings): Behavior[Command] = Behaviors.setup { ctx =>
    given SelfUniqueAddress = DistributedData(ctx.system).selfUniqueAddress

    def wid(now: Long) = now / settings.windowMs

    def bucketOf(s: String) = java.lang.Integer.rotateLeft(s.hashCode, 13) & (settings.buckets - 1)

    def keyOf(w: Long, b: Int) = PNCounterMapKey[String](s"$prefix-$w-$b")

    def updateSucceeded[A <: ReplicatedData](rsp: Replicator.UpdateResponse[A]): Boolean =
      rsp match {
        case _: Replicator.UpdateSuccess[_] => true
        case _ => false
      }

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
              rsp => GotGet(k, w, b, rsp, replyTo)
            )
            Behaviors.same
          case GotGet(k, w, b, rsp, replyTo) => // compute key from window+bucket
            val key = keyOf(w, b)
            // current: the current counter-value for a key in this bucket
            // distinct: the number of distinct keys recorded in this bucket
            val (current, distinct) =
              rsp match { // The Replicator responses, "I have a value for PNCounterMap[String] under this key!"
                case g @ Replicator.GetSuccess(
                      key
                    ) => //  so you call g.get(key) to extract the actual PNCounterMap[String] for which the keys are your rate-limiting keys
                  //  +-----------------+---------+
                  //  |      entryKey   |  count  |
                  //  +-----------------+---------+
                  //  | "ip:1.2.3.4"    |    3    |
                  //  | "ip:10.0.0.7"   |    5    |
                  //  | "imp|pubA|slot1"|    1    |
                  //  | "imp|pubB|slot2"|    7    |
                  //  +-----------------+---------+
                  val m = g.get(key)
                  (
                    m.get(k).sum.toInt, // here the sum is just a neat way to say BingInt(0) or the value of Option[BigInt]
                    m.entries.size
                  ) // how many distinct keys in this bucket
                case _ => (0, 0) // no value yet, so both counts and distinct keys are zero
              } // if the current counter-value for this key is already at or above capacity, reject
            if (current >= settings.capacity) {
//              ctx.log.debug("DENY capacity key={} current={} cap={}", k, current, cfg.capacity)
              replyTo ! false
              Behaviors.same // if the number of distinct keys in this bucket is at or above maxKeysPerBucket, and this key is not yet present (current == 0), reject
            } else if (distinct >= settings.maxKeysPerBucket && current == 0) {
//              ctx.log.debug(
//                "DENY distinct-cap key={} distinct={} cap={}",
//                k,
//                distinct,
//                cfg.maxKeysPerBucket
//              )
              replyTo ! false
              Behaviors.same // otherwise, increment the counter for this key
            } else {
              val wc =
                if (settings.writeMajority) Replicator.WriteMajority(timeout = 300.millis)
                else Replicator.WriteLocal
              repl.askUpdate(
                ask =>
                  Replicator.Update(key, PNCounterMap.empty[String], wc, ask)(
                    _.incrementBy(k, 1)
                  ),
                rsp => GotUpd(w, b, rsp, replyTo)
              )
              Behaviors.same
            }
          case Tick => // rotation by *not reading old windows*; no per-entry removals
            // We only handle current/previous windows. Older windows are safe to delete.
            val nowW        = wid(System.currentTimeMillis())
            val purgeBefore = nowW - (settings.keepWindows - 1).max(1) // keep at least current
            // Throttle how many keys we delete this sweep:
            var budget = settings.deleteBatchPerTick

            var b = 0
            var w = purgeBefore - 1 // Delete oldest first, stop when budget is spent.
            while (budget > 0 && w >= 0) {
              while (budget > 0 && b < settings.buckets) {
                val k  = keyOf(w, b)
                val wc = Replicator.WriteMajority(settings.deleteTimeout)
//                  ctx.log.info(
//                    "askDelete scheduling window={} bucket={} key={}",
//                    Long.box(w),
//                    Int.box(b),
//                    keyOf(w, b).id
//                  )
                repl.askDelete(ask => Replicator.Delete(k, wc, ask), rsp => GotDel(w, b, rsp))
                budget -= 1
                b += 1
              }
              w -= 1
              b = 0
            }
            Behaviors.same
          case GotUpd(_, _, rsp, replyTo) =>
            replyTo ! updateSucceeded(rsp)
            Behaviors.same
          case GotDel(wi, bi, rsp) =>
//              rsp match {
//                case _: Replicator.DeleteSuccess[_] =>
//                  ctx.log.info("askDelete SUCCESS window={} bucket={}", Long.box(wi), Int.box(bi))
//                case _: Replicator.DataDeleted[_] =>
//                  ctx.log.info(
//                    "askDelete DATA_DELETED (already gone) window={} bucket={}",
//                    Long.box(wi),
//                    Int.box(bi)
//                  )
//                case other =>
//                  ctx.log.warn(
//                    "askDelete NON-FINAL rsp={} window={} bucket={}",
//                    other.getClass.getSimpleName,
//                    Long.box(wi),
//                    Int.box(bi)
//                  )
//              }
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

  private final case class GotGet(
      k: String,
      w: Long,
      b: Int,
      rsp: Replicator.GetResponse[PNCounterMap[String]],
      replyTo: ActorRef[Boolean]
  ) extends Command

  private final case class GotUpd(
      w: Long,
      b: Int,
      rsp: Replicator.UpdateResponse[PNCounterMap[String]],
      replyTo: ActorRef[Boolean]
  ) extends Command

  private final case class GotDel(
      w: Long,
      b: Int,
      rsp: Replicator.DeleteResponse[PNCounterMap[String]]
  ) extends Command

  private case object Tick extends Command
}
