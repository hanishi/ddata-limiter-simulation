package com.example.bloom

import org.apache.pekko.actor.typed.scaladsl.ActorContext

/** Immutable state container for the replay guard actor. */
final case class ReplayGuardState(
    currentBucket: Long,
    previousBucket: Long,
    bloomCurrent: BloomFilter,
    bloomPrevious: BloomFilter,
    metrics: MetricsSnapshot,
    scaling: ScalingState,
    publishing: PublishingState,
    rotatedAtNs: Long       = System.nanoTime(),
    firstGrantDone: Boolean = false
) {

  /** Process a validation request and return updated state with result. */
  def processValidation(
      nonce: Long,
      eventTimeMs: Long,
      analyzer: EventTimingAnalyzer
  ): (ReplayGuardState, ValidationResult) = {
    val timing = analyzer.classifyTiming(eventTimeMs, currentBucket, previousBucket)

    timing match {
      case TimingClassification.TooFuture =>
        val newState = copy(metrics = metrics.deny(DenialReason.TooFuture))
        (newState, ValidationResult.Denied(DenialReason.TooFuture))

      case TimingClassification.TooLate =>
        val newState = copy(metrics = metrics.deny(DenialReason.TooLate))
        (newState, ValidationResult.Denied(DenialReason.TooLate))

      case TimingClassification.Current =>
        if (bloomCurrent.maybeContains(nonce)) {
          val newState = copy(metrics = metrics.deny(DenialReason.Duplicate(RequestTiming.Current)))
          (newState, ValidationResult.Denied(DenialReason.Duplicate(RequestTiming.Current)))
        } else {
          bloomCurrent.add(nonce)
          val updatedFirstGrant = if (!firstGrantDone) {
            bloomPrevious.add(nonce)
            true
          } else firstGrantDone

          val newState = copy(
            metrics        = metrics.grant(RequestTiming.Current),
            publishing     = publishing.incrementAdds,
            firstGrantDone = updatedFirstGrant
          )
          (newState, ValidationResult.Granted(RequestTiming.Current))
        }

      case TimingClassification.Previous =>
        if (bloomPrevious.maybeContains(nonce)) {
          val newState = copy(metrics = metrics.deny(DenialReason.Duplicate(RequestTiming.Late)))
          (newState, ValidationResult.Denied(DenialReason.Duplicate(RequestTiming.Late)))
        } else {
          bloomPrevious.add(nonce)
          val newState = copy(
            metrics    = metrics.grant(RequestTiming.Late),
            publishing = publishing.incrementAdds
          )
          (newState, ValidationResult.Granted(RequestTiming.Late))
        }

      case TimingClassification.EarlyNext =>
        val seenCurrent  = bloomCurrent.maybeContains(nonce)
        val seenPrevious = bloomPrevious.maybeContains(nonce)

        if (seenCurrent || seenPrevious) {
          val newState = copy(metrics = metrics.deny(DenialReason.Duplicate(RequestTiming.Early)))
          (newState, ValidationResult.Denied(DenialReason.Duplicate(RequestTiming.Early)))
        } else {
          bloomCurrent.add(nonce)
          val newState = copy(
            metrics    = metrics.grant(RequestTiming.Early),
            publishing = publishing.incrementAdds
          )
          (newState, ValidationResult.Granted(RequestTiming.Early))
        }

      case TimingClassification.OutOfWindow =>
        val newState = copy(metrics = metrics.deny(DenialReason.OutOfWindow))
        (newState, ValidationResult.Denied(DenialReason.OutOfWindow))
    }
  }

  /** Apply bucket rotation, returning new state. */
  def applyRotation(
      rotation: RotationInfo,
      appliedScaling: Option[AppliedScaling],
      expectedPerPart: Int,
      fpr: Double
  ): ReplayGuardState = {
    // Handle previous bucket preservation or clearing
    val newBloomPrevious = if (rotation.shouldPreserveCurrent) {
      bloomPrevious.loadFromArray(bloomCurrent.snapshotWords)
      bloomPrevious
    } else {
      bloomPrevious.clear()
      bloomPrevious
    }

    // Handle current bucket - either scale or clear
    val newBloomCurrent = appliedScaling match {
      case Some(scaling) if scaling.shapeChanged =>
        BloomFilter.create(scaling.newExpectedPerPart, fpr)
      case _ =>
        bloomCurrent.clear()
        bloomCurrent
    }

    // Update scaling state
    val newScaling = appliedScaling match {
      case Some(scaling) => this.scaling.applyScaling(scaling.newExpectedPerPart)
      case None => this.scaling.incrementRotations
    }

    copy(
      currentBucket  = rotation.newBucket,
      previousBucket = rotation.previousBucket,
      bloomCurrent   = newBloomCurrent,
      bloomPrevious  = newBloomPrevious,
      metrics        = metrics.resetForNewBucket(rotation.newBucket),
      scaling        = newScaling,
      publishing     = publishing.resetForRotation,
      rotatedAtNs    = System.nanoTime()
    )
  }

  /** Update with scaling proposal. */
  def withScalingProposal(expectedPerPart: Int): ReplayGuardState =
    copy(scaling = scaling.proposePendingScale(expectedPerPart))

  /** Reset metrics counters. */
  def resetMetrics: ReplayGuardState =
    copy(metrics = metrics.resetForNewBucket(metrics.statsBucketId))

  /** Update publishing state after publish completion. */
  def publishCompleted: ReplayGuardState =
    copy(publishing = publishing.publishCompleted(System.nanoTime()))

  /** Set force initial publish flag. */
  def forceInitialPublish: ReplayGuardState =
    copy(publishing = publishing.copy(forceInitial = true))
}

/** State related to publishing snapshots to DData. */
final case class PublishingState(
    addsSincePublish: Int = 0,
    inFlight: Boolean     = false,
    lastPublishAtNs: Long = 0L,
    forceInitial: Boolean = true
) {

  def incrementAdds: PublishingState = copy(addsSincePublish = addsSincePublish + 1)

  def startPublish: PublishingState = copy(inFlight = true, forceInitial = false)

  def publishCompleted(nowNs: Long): PublishingState = copy(
    inFlight         = false,
    lastPublishAtNs  = nowNs,
    addsSincePublish = 0
  )

  def resetForRotation: PublishingState = copy(
    addsSincePublish = 0,
    lastPublishAtNs  = 0L
  )

  def shouldPublish(config: GuardConfiguration): Boolean = {
    if (inFlight) return false

    if (forceInitial) return true

    val nowNs            = System.nanoTime()
    val due              = (nowNs - lastPublishAtNs) >= config.publishEvery.toNanos
    val enoughAdds       = addsSincePublish >= config.publishMinAdds
    val hasEverPublished = lastPublishAtNs != 0L
    val stale = hasEverPublished && (nowNs - lastPublishAtNs) >= (config.publishEvery.toNanos * 4)

    due && (enoughAdds || stale)
  }
}

object ReplayGuardState {

  /** Load state from a snapshot, with fallback to initial state on mismatch. */
  def fromSnapshot(
      snapshot: SnapshotEnvelope,
      config: GuardConfiguration,
      bucketStrategy: BucketRotationStrategy,
      ctx: ActorContext[_],
      partitionId: Int
  ): ReplayGuardState = {
    val bloomCurrent  = BloomFilter.create(config.expectedPerPart, config.fpr)
    val bloomPrevious = BloomFilter.create(config.expectedPerPart, config.fpr)

    if (snapshot.mBits == bloomCurrent.mBits && snapshot.k == bloomCurrent.k) {
      // Shapes match, load from snapshot
      bloomCurrent.loadFromArray(snapshot.current)
      bloomPrevious.loadFromArray(snapshot.previous)

      ReplayGuardState(
        currentBucket  = snapshot.currentBucket,
        previousBucket = snapshot.prevBucket,
        bloomCurrent   = bloomCurrent,
        bloomPrevious  = bloomPrevious,
        metrics        = MetricsSnapshot(statsBucketId = snapshot.currentBucket),
        scaling        = ScalingState(currentExpectedPerPart = config.expectedPerPart),
        publishing     = PublishingState(forceInitial = true),
        rotatedAtNs    = snapshot.rotatedAtNanos
      )
    } else {
      // Shape mismatch, use initial state
      ctx.log.warn(
        s"GuardShard[part=$partitionId] boot: snapshot shape mismatch (mBits/k) — using initial state"
      )
      initial(config, bucketStrategy).forceInitialPublish
    }
  }

  /** Create initial state for a new replay guard. */
  def initial(
      config: GuardConfiguration,
      bucketStrategy: BucketRotationStrategy
  ): ReplayGuardState = {
    val currentBucket = bucketStrategy.currentBucket
    val bloomCurrent  = BloomFilter.create(config.expectedPerPart, config.fpr)
    val bloomPrevious = BloomFilter.create(config.expectedPerPart, config.fpr)

    ReplayGuardState(
      currentBucket  = currentBucket,
      previousBucket = currentBucket - 1,
      bloomCurrent   = bloomCurrent,
      bloomPrevious  = bloomPrevious,
      metrics        = MetricsSnapshot(statsBucketId = currentBucket),
      scaling        = ScalingState(currentExpectedPerPart = config.expectedPerPart),
      publishing     = PublishingState()
    )
  }
}
