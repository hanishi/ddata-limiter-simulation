package com.example.bloom

import org.apache.pekko.actor.typed.scaladsl.ActorContext

/** Immutable metrics snapshot for replay guard statistics. */
final case class MetricsSnapshot(
    grantsInBucket: Int     = 0,
    denialsInBucket: Int    = 0,
    earlyGrants: Int        = 0,
    earlyDenials: Int       = 0,
    lateGrants: Int         = 0,
    lateDenials: Int        = 0,
    tooFutureDenials: Int   = 0,
    tooLateDenials: Int     = 0,
    outOfWindowDenials: Int = 0,
    statsBucketId: Long     = 0
) {

  /** Record a granted request. */
  def grant(timing: RequestTiming): MetricsSnapshot = timing match {
    case RequestTiming.Current => copy(grantsInBucket = grantsInBucket + 1)
    case RequestTiming.Early =>
      copy(grantsInBucket = grantsInBucket + 1, earlyGrants = earlyGrants + 1)
    case RequestTiming.Late =>
      copy(grantsInBucket = grantsInBucket + 1, lateGrants = lateGrants + 1)
  }

  /** Record a denied request. */
  def deny(reason: DenialReason): MetricsSnapshot = reason match {
    case DenialReason.Duplicate(RequestTiming.Current) =>
      copy(denialsInBucket = denialsInBucket + 1)
    case DenialReason.Duplicate(RequestTiming.Early) =>
      copy(denialsInBucket = denialsInBucket + 1, earlyDenials = earlyDenials + 1)
    case DenialReason.Duplicate(RequestTiming.Late) =>
      copy(denialsInBucket = denialsInBucket + 1, lateDenials = lateDenials + 1)
    case DenialReason.TooFuture =>
      copy(denialsInBucket = denialsInBucket + 1, tooFutureDenials = tooFutureDenials + 1)
    case DenialReason.TooLate =>
      copy(denialsInBucket = denialsInBucket + 1, tooLateDenials = tooLateDenials + 1)
    case DenialReason.OutOfWindow =>
      copy(denialsInBucket = denialsInBucket + 1, outOfWindowDenials = outOfWindowDenials + 1)
  }

  /** Reset counters for new bucket period. */
  def resetForNewBucket(bucketId: Long): MetricsSnapshot = MetricsSnapshot(statsBucketId = bucketId)

  /** Log current metrics to the actor context. */
  def logStats(ctx: ActorContext[_], partitionId: Int, bucketMs: Long): Unit =
    ctx.log.info(
      "🍎guardshard[partition={}]: grants={} denials={} (bucket={}, period={}ms) | early_grants={} early_denials={} late_grants={} late_denials={} too_future={} too_late={} out_of_window={}",
      Int.box(partitionId),
      Int.box(grantsInBucket),
      Int.box(denialsInBucket),
      Long.box(statsBucketId),
      Long.box(bucketMs),
      Int.box(earlyGrants),
      Int.box(earlyDenials),
      Int.box(lateGrants),
      Int.box(lateDenials),
      Int.box(tooFutureDenials),
      Int.box(tooLateDenials),
      Int.box(outOfWindowDenials)
    )
}

/** Classification of request timing relative to current time bucket. */
sealed trait RequestTiming
object RequestTiming {
  case object Current extends RequestTiming
  case object Early extends RequestTiming
  case object Late extends RequestTiming
}

/** Reason for denying a replay validation request. */
sealed trait DenialReason
object DenialReason {
  final case class Duplicate(timing: RequestTiming) extends DenialReason
  case object TooFuture extends DenialReason
  case object TooLate extends DenialReason
  case object OutOfWindow extends DenialReason
}

/** Result of a replay validation request. */
sealed trait ValidationResult
object ValidationResult {
  final case class Granted(timing: RequestTiming) extends ValidationResult
  final case class Denied(reason: DenialReason) extends ValidationResult
}
