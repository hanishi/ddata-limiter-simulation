package com.example.bloom

// Updated imports for refactored components
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ ActorRef, ActorSystem, Behavior, Scheduler }
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ ClusterSharding, Entity }
import org.apache.pekko.cluster.typed.{ Cluster, SelfUp, Subscribe }
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.{ Failure, Success }

/** WindowedBloomReplayGuard sharding driver:
  * - Boots N ActorSystems (ports in args or defaults)
  * - Initializes Cluster Sharding with GuardShard.initBehavior
  * - Hammers entities by picking partitions and asking ValidateReplay
  * - Logs grants/denials + heap every 5s
  */
object WindowedBloomReplayGuardDriver {

  def main(args: Array[String]): Unit = {
    val ports = if (args.isEmpty) Seq(17356, 17357, 17358, 17359, 0) else args.toSeq.map(_.toInt)
    ports.foreach(startNode)
  }

  private def startNode(port: Int): Unit = {
    val config = ConfigFactory
      .parseString(s"pekko.remote.artery.canonical.port=$port")
      .withFallback(ConfigFactory.load())

    ActorSystem[DriverCmd](Driver(), "replay-cluster", config)
  }

  def Driver(): Behavior[DriverCmd] = Behaviors.setup { ctx =>
    implicit val sys: ActorSystem[?]  = ctx.system
    implicit val ec: ExecutionContext = ctx.executionContext
    implicit val sch: Scheduler       = ctx.system.scheduler
    implicit val to: Timeout          = 2.seconds

    val sharding = ClusterSharding(ctx.system)

    // ---- GuardShard configuration (tune as you like) ----
    val config = GuardConfiguration(
      expectedPerPart = 50_000,
      bucketMs        = 1.minutes.toMillis, // fast rotation for demo
      publishEvery    = 3.seconds,
      publishMinAdds  = 1,
      bootMaxWait     = 300.millis,
      scaling = ScalingConfig(
        enabled               = true,
        maxMemoryPerPartBytes = 128L * 1024L * 1024L
      )
    ).forHighLoad

    // Configurable observability + backpressure knobs
    val appCfg = ctx.system.settings.config
    val logWindow: FiniteDuration =
      scala.util
        .Try(
          java.time.Duration.ofMillis(appCfg.getDuration("guardshard.driver.log-window").toMillis)
        )
        .map(_.toMillis.millis)
        .getOrElse(1.minutes)
    val hitInterval: FiniteDuration =
      scala.util
        .Try(
          java.time.Duration.ofMillis(appCfg.getDuration("guardshard.driver.hit-interval").toMillis)
        )
        .map(_.toMillis.millis)
        .getOrElse(125.millis)
    val maxInFlight: Int =
      scala.util.Try(appCfg.getInt("guardshard.driver.max-inflight")).getOrElse(0)

    // ---- Sharding init using initBehavior ----
    sharding.init(
      Entity(WindowedBloomReplayGuard.TypeKey) { entityCtx =>
        WindowedBloomReplayGuard.initBehavior(entityCtx, config)
      }
    )

    // Wait until this node is Up before starting traffic
    val cluster                         = Cluster(ctx.system)
    val selfUpAdapter: ActorRef[SelfUp] = ctx.messageAdapter[SelfUp](OnSelfUp.apply)
    cluster.subscriptions ! Subscribe(selfUpAdapter, classOf[SelfUp])

    Behaviors.withTimers { timers =>
      val started                                           = System.nanoTime()
      var last                                              = started
      var grants                                            = 0L
      var denials                                           = 0L
      var workerRef: Option[ActorRef[Worker.Msg | Boolean]] = None

      Behaviors.receiveMessage {
        case OnSelfUp(_) =>
          val w =
            ctx.spawn(
              Worker(
                sharding,
                config,
                parts       = 256,
                replyTo     = ctx.self,
                hitEvery    = hitInterval,
                maxInFlight = maxInFlight
              ),
              "replay-worker"
            )
          workerRef = Some(w)
          w ! Worker.Hit
          Behaviors.same
        case Granted => grants += 1; Behaviors.same
        case Denied => denials += 1; Behaviors.same
        case StartWork => Behaviors.same // unused but needed for exhaustive match
        case Tick =>
          val now     = System.nanoTime()
          val winS    = (now - last).toDouble / 1e9
          val rt      = Runtime.getRuntime
          val usedMB  = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)
          val totalMB = rt.totalMemory() / (1024 * 1024)
          val maxMB   = rt.maxMemory() / (1024 * 1024)
          ctx.log.info("Heap used={}MB total={}MB max={}MB", usedMB, totalMB, maxMB)
          val total = grants + denials
          if (total > 0)
            ctx.log.info(
              "guardshard: qps~{} (over {}s)",
              Long.box((total / winS).toLong),
              f"$winS%.3f"
            )
          grants = 0; denials = 0; last = now
          Behaviors.same
      }
    }
  }

  sealed trait DriverCmd

  private final case class OnSelfUp(ev: SelfUp) extends DriverCmd

  private case object Tick extends DriverCmd

  private case object StartWork extends DriverCmd

  private case object Granted extends DriverCmd

  private case object Denied extends DriverCmd

  private object Worker {
    def apply(
        sharding: ClusterSharding,
        cfg: GuardConfiguration,
        parts: Int,
        replyTo: ActorRef[DriverCmd],
        hitEvery: FiniteDuration,
        maxInFlight: Int
    ): Behavior[Msg | Boolean] = Behaviors.setup { ctx =>
      implicit val to: Timeout          = 2.seconds
      implicit val sch: Scheduler       = ctx.system.scheduler
      implicit val ec: ExecutionContext = ctx.executionContext

      // use the passed-in parts count
      val totalParts = parts
      var seq        = 0
      var inFlight   = 0

//      def nextKey(): String = {
//        val i = seq
//        seq = (seq + 1) & 0xffff
//        s"ip:${i / 256}.${i % 256}.0.1"
//      }

      def nextKey(): String = {
        val lastOctet = (seq % 32) + 1
        seq += 1
        s"ip:1.2.3.$lastOctet"
      }

      // 64-bit FNV-1a hash folded to 32 bits for partitioning
      def hash64Fold32(s: String): Int = {
        val data  = s.getBytes("UTF-8")
        var hash  = 0xcbf29ce484222325L // offset basis
        val prime = 0x100000001b3L // FNV prime
        var i     = 0
        while (i < data.length) {
          hash ^= (data(i) & 0xff).toLong
          hash *= prime
          i += 1
        }
        val h32 = (hash ^ (hash >>> 32)).toInt
        h32
      }

      def scheduleNext(): Unit =
        ctx.scheduleOnce(hitEvery, ctx.self, Hit)

      Behaviors.receiveMessage {
        case Hit =>
          // Optional backpressure: limit concurrent in-flight asks
          if (maxInFlight == 0 || inFlight < maxInFlight) {
            val key  = nextKey()
            val h32  = hash64Fold32(key)
            val part = Math.floorMod(h32, totalParts)
            // GuardShard expects entityId "replay|<part>"
            val entityId = s"replay|$part"
            val ref      = sharding.entityRefFor(WindowedBloomReplayGuard.TypeKey, entityId)

            inFlight += 1
            // Ask ValidateReplay; true means "first time" (grant), false = duplicate (deny)
            ctx.ask(
              ref,
              (reply: ActorRef[Boolean]) =>
                WindowedBloomReplayGuard
                  .ValidateReplayAt(reply, java.lang.Integer.toUnsignedLong(h32))
            ) {
              case Success(result) => result
              case Failure(_) => false
            }
          }
          scheduleNext()
          Behaviors.same

        case true =>
          if (inFlight > 0) inFlight -= 1
          replyTo ! Granted
          Behaviors.same
        case false =>
          if (inFlight > 0) inFlight -= 1
          replyTo ! Denied
          Behaviors.same
      }
    }

    sealed trait Msg
    case object Hit extends Msg
  }
}
