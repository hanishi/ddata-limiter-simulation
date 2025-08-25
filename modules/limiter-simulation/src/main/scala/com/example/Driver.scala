package com.example

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ ActorRef, ActorSystem, Behavior, Scheduler }
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.{ Failure, Success }

object Driver {
  def main(args: Array[String]): Unit = {
    val ports =
      if (args.isEmpty)
        Seq(17356, 17357, 0)
      else
        args.toSeq.map(_.toInt)
    ports.foreach(startup)
  }

  private def startup(port: Int) = {
    val config = ConfigFactory
      .parseString(s"""
       pekko.remote.artery.canonical.port=$port
       """)
      .withFallback(ConfigFactory.load())
    ActorSystem(Driver(), "ddata-rate-limiter", config)
  }

  def apply(): Behavior[DriverCommand] = Behaviors.setup { ctx =>
    val limiter = ctx.spawn(
      RateLimiterDData(
        prefix = "ddata-rate-limiter",
        settings = RateLimiterDData.Settings(
          capacity = 1,
          // Maximum allowed count per key per window.
          // With capacity = 1, each unique key (e.g. "ip:1.2.3.4") can only be granted once per window.
          // The next attempt in the same window is denied.

          windowMs = 60_000L,
          // Size of the rolling window in milliseconds.
          // Here: 60 seconds. Counters reset every minute.
          // Tip: if you set this shorter (e.g. 2000L), you’ll see cycles of “grant burst → denials → reset.”

          buckets = 4,
          // How many CRDT shards per window.
          // Each bucket is a separate PNCounterMap, keyed by hash(key) % buckets.
          // More buckets → lower per-map cardinality, but more maps in gossip.
          // Fewer buckets → easier to hit maxKeysPerBucket and see denials.

          writeMajority = false,
          // Consistency level for updates.
          // false = WriteLocal (fast, microsecond latency, eventual convergence).
          // true = WriteMajority (slower, but stricter consistency).
          // For rate limiting where slight overshoot is OK, WriteLocal is typical.

          sweepEvery = 1.second,
          // How often we run the "Tick" that cleans up old windows.
          // Smaller = more frequent delete passes, useful for tests.
          // In prod you might sweep every 1–2 minutes.

          maxKeysPerBucket = 50_000,
          // Safety cap: max number of distinct keys per bucket per window.
          // Protects against the "100k entries per CRDT" guideline.
          // Once full, *new* keys get denied, but existing ones can continue until they hit capacity.

          keepWindows = 1,
          // How many windows to retain concurrently.
          // = 1 means only the current window is kept; once it rolls over, the old window is deletable.
          // = 2 would let you accept stragglers from the previous window.

          deleteBatchPerTick = 64,
          // Throttle: how many CRDT entries (window+bucket pairs) to try to delete per sweep.
          // Avoids overloading gossip with a flood of deletes at once.
          // Higher = faster cleanup, Lower = less replication churn.

          deleteTimeout = 500.millis
          // Timeout for a delete operation to be acknowledged (for WriteMajority).
          // For tests, kept short so logs are immediate.
        )
      ),
      name = "ddata-rate-limiter"
    )

    val worker = ctx.spawn(Worker(limiter, replyTo = ctx.self), "worker")

    worker ! Worker.Hit
    val started = System.nanoTime()

    Behaviors.withTimers { timers =>
      timers.startTimerAtFixedRate(Tick, 5.seconds)
      var last    = System.nanoTime()
      var grants  = 0L
      var denials = 0L

      Behaviors.receiveMessage {
        case Granted =>
          grants += 1
          Behaviors.same
        case Denied =>
          denials += 1
          Behaviors.same
        case Tick =>
          val now  = System.nanoTime()
          val secs = (now - last).toDouble / 1e9
          ctx.log.info(
            "throughput: grants={} denials={} ({}s window of {} total seconds)",
            grants,
            denials,
            secs,
            (now - started).toDouble / 1e9
          )
          grants  = 0L
          denials = 0L
          last    = now
          val rt      = Runtime.getRuntime
          val usedMB  = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)
          val totalMB = rt.totalMemory() / (1024 * 1024)
          val maxMB   = rt.maxMemory() / (1024 * 1024)
          ctx.log.info("Heap used={}MB total={}MB max={}MB", usedMB, totalMB, maxMB)
          Behaviors.same
      }
    }
  }

  sealed trait DriverCommand

  private case object Granted extends DriverCommand

  private case object Denied extends DriverCommand

  private case object Tick extends DriverCommand

  private object Worker {
    def apply(
        limiter: ActorRef[RateLimiterDData.Command],
        replyTo: ActorRef[DriverCommand]
    ): Behavior[WorkerCommand | Boolean] = Behaviors.setup { ctx =>
      val keySpace = 32
      var seq      = 0

      def nextKey(): String = {
        val i = seq
        seq = (seq + 1) % keySpace
        s"ip:${i / 256}.${i % 256}.0.1"
      }

      given Timeout          = 300.millis
      given Scheduler        = ctx.system.scheduler
      given ExecutionContext = ctx.executionContext

      Behaviors.receiveMessage {
        case Hit =>
          val key = nextKey()
//          ctx.log.info("Worker checking key={}", key)
          ctx.ask[RateLimiterDData.Allow, Boolean](limiter, RateLimiterDData.Allow(key, _)) {
            case Success(result) => result
            case Failure(_) => false
          }
          ctx.scheduleOnce(125.millis, ctx.self, Hit)
          Behaviors.same
        case true =>
          replyTo ! Driver.Granted
          Behaviors.same
        case false =>
          replyTo ! Driver.Denied
          Behaviors.same
      }
    }

    sealed trait WorkerCommand

    case object Hit extends WorkerCommand

  }
}
