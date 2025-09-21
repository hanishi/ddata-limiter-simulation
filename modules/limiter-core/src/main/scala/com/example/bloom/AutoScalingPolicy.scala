package com.example.bloom

import org.apache.pekko.actor.typed.scaladsl.ActorContext

/** Configuration for auto-scaling behavior. */
final case class ScalingConfig(
    enabled: Boolean                = false,
    minExpectedPerPart: Int         = 10_000,
    maxExpectedPerPart: Int         = 2_000_000,
    cooldownRotations: Int          = 2,
    scaleUpFillRatio: Double        = 0.70,
    capacityGrowthFactor: Double    = 1.5,
    memoryPressureThreshold: Double = 0.8,
    maxMemoryPerPartBytes: Long     = 128L * 1024L * 1024L
)

/** Tracks auto-scaling state and decisions. */
final case class ScalingState(
    currentExpectedPerPart: Int,
    rotationsSinceScale: Int  = 0,
    pendingScaleExpected: Int = 0
) {

  /** Apply a scaling decision, updating the state. */
  def applyScaling(newExpected: Int): ScalingState =
    copy(
      currentExpectedPerPart = newExpected,
      pendingScaleExpected   = 0,
      rotationsSinceScale    = 0
    )

  /** Increment rotation counter (used for cooldown). */
  def incrementRotations: ScalingState = copy(rotationsSinceScale = rotationsSinceScale + 1)

  /** Set a pending scaling proposal. */
  def proposePendingScale(expected: Int): ScalingState = copy(pendingScaleExpected = expected)
}

/** Auto-scaling policy for bloom filters based on occupancy and memory pressure. */
final class AutoScalingPolicy(config: ScalingConfig, fpr: Double) {

  /** Determine if scaling should be applied and what the new size should be. */
  def shouldScale(
      bloomFilter: BloomFilter,
      state: ScalingState
  ): Option[ScalingProposal] = {
    if (!config.enabled) return None

    // Cooldown: require minimum rotations since last scale
    if (state.rotationsSinceScale < config.cooldownRotations) return None

    // Estimate current occupancy
    val estimatedInserts = bloomFilter.estimateInsertCount
    val nearCapacity =
      estimatedInserts >= math.ceil(config.scaleUpFillRatio * state.currentExpectedPerPart).toInt

    if (!nearCapacity || isMemoryPressure) return None

    // Calculate proposed scaling
    val growthByFactor =
      math.round(state.currentExpectedPerPart.toDouble * config.capacityGrowthFactor).toInt
    val growthByEstimate = math.max(state.currentExpectedPerPart + 1, estimatedInserts * 2)
    val proposedRaw      = math.max(growthByFactor, growthByEstimate)

    // Apply bounds
    val proposedClamped = math.max(
      config.minExpectedPerPart,
      math.min(config.maxExpectedPerPart, proposedRaw)
    )

    // Check memory constraints
    val memoryNeeded = BloomFilter.memoryUsage(proposedClamped, fpr) * 2 // current + previous
    if (memoryNeeded > config.maxMemoryPerPartBytes) return None

    Some(
      ScalingProposal(
        newExpectedPerPart = proposedClamped,
        currentFillRatio   = estimatedInserts.toDouble / state.currentExpectedPerPart,
        estimatedInserts   = estimatedInserts,
        memoryNeeded       = memoryNeeded
      )
    )
  }

  /** Check if the system is under memory pressure. */
  def isMemoryPressure: Boolean = {
    if (!config.enabled) return false

    try {
      val runtime = Runtime.getRuntime
      val used    = runtime.totalMemory() - runtime.freeMemory()
      val max     = runtime.maxMemory()
      (used.toDouble / max) > config.memoryPressureThreshold
    } catch {
      case _: Exception => false // fail safely
    }
  }

  /** Check if a pending scaling should be applied during rotation. */
  def shouldApplyPendingScale(
      state: ScalingState,
      currentMBits: Int,
      currentK: Int
  ): Option[AppliedScaling] = {
    if (state.pendingScaleExpected <= 0) return None

    val (newMBits, newK) = BloomFilter.sizing(state.pendingScaleExpected, fpr)
    val shapeChanged     = newMBits != currentMBits || newK != currentK

    Some(
      AppliedScaling(
        newExpectedPerPart = state.pendingScaleExpected,
        shapeChanged       = shapeChanged,
        oldMBits           = currentMBits,
        oldK               = currentK,
        newMBits           = newMBits,
        newK               = newK
      )
    )
  }
}

/** A proposal to scale the bloom filter. */
final case class ScalingProposal(
    newExpectedPerPart: Int,
    currentFillRatio: Double,
    estimatedInserts: Int,
    memoryNeeded: Long
) {

  /** Log the scaling proposal. */
  def logProposal(
      ctx: ActorContext[_],
      partitionId: Int,
      currentExpected: Int,
      bloomFilter: BloomFilter
  ): Unit = {
    val setBits = bloomFilter.countSetBits
    val fillPct =
      if (bloomFilter.mBits == 0) 0.0 else (setBits.toDouble / bloomFilter.mBits.toDouble) * 100.0
    val fillPctStr = f"${fillPct}%.2f"

    ctx.log.info(
      "GuardShard[part={}] autoscale proposed expectedPerPart {} -> {} (fill~{}/{} bits, {}%)",
      Int.box(partitionId),
      Int.box(currentExpected),
      Int.box(newExpectedPerPart),
      Long.box(setBits),
      Int.box(bloomFilter.mBits),
      fillPctStr
    )
  }
}

/** Details of an applied scaling operation. */
final case class AppliedScaling(
    newExpectedPerPart: Int,
    shapeChanged: Boolean,
    oldMBits: Int,
    oldK: Int,
    newMBits: Int,
    newK: Int
) {

  /** Log the scaling application. */
  def logScaling(ctx: ActorContext[_], partitionId: Int, oldExpected: Int): Unit =
    if (shapeChanged) {
      ctx.log.info(
        "GuardShard[part={}] rotate+scale: expectedPerPart {} -> {} (mBits {} -> {}, k {} -> {})",
        Int.box(partitionId),
        Int.box(oldExpected),
        Int.box(newExpectedPerPart),
        Int.box(oldMBits),
        Int.box(newMBits),
        Int.box(oldK),
        Int.box(newK)
      )
    }
}
