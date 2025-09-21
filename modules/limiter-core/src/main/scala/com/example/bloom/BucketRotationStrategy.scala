package com.example.bloom

import org.apache.pekko.actor.typed.scaladsl.ActorContext

import scala.concurrent.duration.FiniteDuration

/** Manages time-based bucket rotation for windowed bloom filters. */
final class BucketRotationStrategy(val bucketMs: Long) {

  /** Calculate bucket ID for a given timestamp. */
  def bucketOf(timestampMs: Long): Long = bucketStartMs(timestampMs) / bucketMs

  /** Check if rotation is needed and return rotation info. */
  def checkRotation(currentBucketId: Long): Option[RotationInfo] = {
    val nowBucket = currentBucket
    if (nowBucket > currentBucketId) {
      val gap = nowBucket - currentBucketId
      Some(
        RotationInfo(
          newBucket             = nowBucket,
          previousBucket        = if (gap == 1) currentBucketId else nowBucket - 1,
          gap                   = gap,
          shouldPreserveCurrent = gap == 1
        )
      )
    } else None
  }

  /** Calculate the current bucket ID based on system time. */
  def currentBucket: Long = bucketStartMs(System.currentTimeMillis()) / bucketMs

  /** Get the start time (aligned to bucket boundary) for a given timestamp. */
  def bucketStartMs(timestampMs: Long): Long = timestampMs - (timestampMs % bucketMs)

  /** Format elapsed time in human-readable format. */
  def formatElapsed(elapsedMs: Long): String = {
    val s   = elapsedMs / 1000
    val msR = elapsedMs % 1000
    val m   = s / 60
    val sR  = s         % 60
    if (m > 0) f"${m}m${sR}%02ds" else f"$s%d.${msR}%03d s"
  }
}

/** Information about a bucket rotation event. */
final case class RotationInfo(
    newBucket: Long,
    previousBucket: Long,
    gap: Long,
    shouldPreserveCurrent: Boolean
) {

  /** Log rotation details to the actor context. */
  def logRotation(
      ctx: ActorContext[_],
      partitionId: Int,
      elapsedMs: Long,
      strategy: BucketRotationStrategy
  ): Unit = {
    val nowMs       = System.currentTimeMillis()
    val lastStartMs = strategy.bucketStartMs(nowMs) - strategy.bucketMs
    val nextStartMs = strategy.bucketStartMs(nowMs)

    ctx.log.info(
      "🪟 GuardShard[part={}] WINDOW ROTATE: {} -> {} (gap={}) | elapsed={} | last_start={} next_start={}",
      Int.box(partitionId),
      Long.box(previousBucket),
      Long.box(newBucket),
      Long.box(gap),
      strategy.formatElapsed(elapsedMs),
      Long.box(lastStartMs),
      Long.box(nextStartMs)
    )
  }
}

/** Classifies the timing of an event relative to current time buckets. */
sealed trait TimingClassification
object TimingClassification {
  case object Current extends TimingClassification
  case object Previous extends TimingClassification
  case object EarlyNext extends TimingClassification
  case object TooFuture extends TimingClassification
  case object TooLate extends TimingClassification
  case object OutOfWindow extends TimingClassification
}

/** Analyzes event timing against bucket windows and skew tolerances. */
final class EventTimingAnalyzer(
    strategy: BucketRotationStrategy,
    extendedSkew: FiniteDuration
) {

  /** Classify an event's timing relative to current bucket state. */
  def classifyTiming(
      eventTimeMs: Long,
      currentBucket: Long,
      previousBucket: Long
  ): TimingClassification = {
    val nowMs       = System.currentTimeMillis()
    val eventBucket = strategy.bucketOf(eventTimeMs)
    val skewMs      = extendedSkew.toMillis

    val tooFuture = eventTimeMs - nowMs > skewMs
    val tooLate   = nowMs - eventTimeMs > skewMs

    if (tooFuture) TimingClassification.TooFuture
    else if (tooLate) TimingClassification.TooLate
    else if (eventBucket == currentBucket) TimingClassification.Current
    else if (eventBucket == previousBucket) TimingClassification.Previous
    else if (eventBucket == currentBucket + 1 && (eventTimeMs - nowMs) <= skewMs) {
      // Slightly-ahead clocks within bound: treat as current to avoid reserving the future
      TimingClassification.EarlyNext
    } else TimingClassification.OutOfWindow
  }
}
