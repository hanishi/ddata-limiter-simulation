package com.example.bloom

import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success, Try }

sealed trait ConfigValidationResult
object ConfigValidationResult {
  def error(message: String): Invalid = Invalid(List(message))

  def combine(results: ConfigValidationResult*): ConfigValidationResult = {
    val errors = results.collect { case Invalid(errs) => errs }.flatten.toList
    if (errors.isEmpty) Valid else Invalid(errors)
  }

  final case class Invalid(errors: List[String]) extends ConfigValidationResult

  case object Valid extends ConfigValidationResult
}

final case class GuardConfiguration(
    expectedPerPart: Int         = 50_000,
    fpr: Double                  = 1e-4,
    bucketMs: Long               = 60_000L,
    maxSkew: FiniteDuration      = FiniteDuration(5, "seconds"),
    publishEvery: FiniteDuration = FiniteDuration(250, "milliseconds"),
    publishMinAdds: Int          = 64,
    bootMaxWait: FiniteDuration  = FiniteDuration(150, "milliseconds"),
    scaling: ScalingConfig       = ScalingConfig(),
    clockSkewMonitoring: Boolean = false,
    degradedModeEnabled: Boolean = true,
    metricsEnabled: Boolean      = false
) {

  lazy val extendedSkew: FiniteDuration = if (clockSkewMonitoring) maxSkew * 6 else maxSkew

  /** Check if configuration is valid (simple boolean). */
  def isValid: Boolean = validate == ConfigValidationResult.Valid

  /** Get validation errors as list of strings. */
  def validationErrors: List[String] = validate match {
    case ConfigValidationResult.Valid => Nil
    case ConfigValidationResult.Invalid(errors) => errors
  }

  /** Validate this configuration and return detailed results. */
  def validate: ConfigValidationResult = {
    import ConfigValidationResult.*

    val basicValidation = combine(
      validatePositive("expectedPerPart", expectedPerPart),
      validateRange("fpr", fpr, 1e-9, 0.5),
      validatePositive("bucketMs", bucketMs),
      validatePositiveDuration("maxSkew", maxSkew),
      validatePositiveDuration("publishEvery", publishEvery),
      validatePositive("publishMinAdds", publishMinAdds),
      validatePositiveDuration("bootMaxWait", bootMaxWait)
    )

    val scalingValidation = if (scaling.enabled) {
      combine(
        validatePositive("scaling.minExpectedPerPart", scaling.minExpectedPerPart),
        validatePositive("scaling.maxExpectedPerPart", scaling.maxExpectedPerPart),
        validateRange("scaling.scaleUpFillRatio", scaling.scaleUpFillRatio, 0.1, 0.95),
        validateRange("scaling.capacityGrowthFactor", scaling.capacityGrowthFactor, 1.1, 10.0),
        validateRange(
          "scaling.memoryPressureThreshold",
          scaling.memoryPressureThreshold,
          0.1,
          0.95
        ),
        validatePositive("scaling.cooldownRotations", scaling.cooldownRotations),
        validatePositive("scaling.maxMemoryPerPartBytes", scaling.maxMemoryPerPartBytes),
        validateLogical("scaling bounds", scaling.minExpectedPerPart <= scaling.maxExpectedPerPart),
        validateLogical(
          "expected within scaling bounds",
          expectedPerPart >= scaling.minExpectedPerPart && expectedPerPart <= scaling.maxExpectedPerPart
        )
      )
    } else Valid

    val memoryValidation = validateMemoryUsage()

    combine(basicValidation, scalingValidation, memoryValidation)
  }

  private def validatePositive(name: String, value: Int): ConfigValidationResult =
    if (value > 0) ConfigValidationResult.Valid
    else ConfigValidationResult.error(s"$name must be positive, got: $value")

  private def validatePositive(name: String, value: Long): ConfigValidationResult =
    if (value > 0) ConfigValidationResult.Valid
    else ConfigValidationResult.error(s"$name must be positive, got: $value")

  private def validateRange(
      name: String,
      value: Double,
      min: Double,
      max: Double
  ): ConfigValidationResult =
    if (value >= min && value <= max) ConfigValidationResult.Valid
    else ConfigValidationResult.error(s"$name must be between $min and $max, got: $value")

  private def validatePositiveDuration(
      name: String,
      duration: FiniteDuration
  ): ConfigValidationResult =
    if (duration.toNanos > 0) ConfigValidationResult.Valid
    else ConfigValidationResult.error(s"$name must be positive duration, got: $duration")

  private def validateLogical(name: String, condition: Boolean): ConfigValidationResult =
    if (condition) ConfigValidationResult.Valid
    else ConfigValidationResult.error(s"$name constraint failed")

  private def validateMemoryUsage(): ConfigValidationResult =
    Try {
      val (mBits, _)         = BloomFilter.sizing(expectedPerPart, fpr)
      val memoryPerPartBytes = (mBits / 8) * 2 // current + previous bloom

      if (scaling.enabled && memoryPerPartBytes > scaling.maxMemoryPerPartBytes) {
        ConfigValidationResult.error(
          s"Calculated memory usage ${memoryPerPartBytes / 1024 / 1024}MB exceeds limit ${scaling.maxMemoryPerPartBytes / 1024 / 1024}MB"
        )
      } else ConfigValidationResult.Valid
    } match {
      case Success(result) => result
      case Failure(exception) =>
        ConfigValidationResult.error(s"Memory validation failed: ${exception.getMessage}")
    }

  /** Create a copy with high-load optimizations. */
  def forHighLoad: GuardConfiguration = copy(
    scaling             = scaling.copy(enabled = true),
    clockSkewMonitoring = true,
    metricsEnabled      = true,
    maxSkew             = FiniteDuration(30, "seconds"),
    publishMinAdds      = 128
  )
}

object GuardConfiguration {

  /** Default configuration optimized for typical production use. */
  val production: GuardConfiguration = GuardConfiguration().copy(
    expectedPerPart     = 100_000,
    fpr                 = 1e-5,
    bucketMs            = 60_000L,
    scaling             = ScalingConfig(enabled = true),
    clockSkewMonitoring = true,
    metricsEnabled      = true
  )
  /** Configuration suitable for testing and development. */
  val development: GuardConfiguration = GuardConfiguration().copy(
    expectedPerPart = 1_000,
    bucketMs        = 5_000L,
    publishEvery    = FiniteDuration(100, "milliseconds"),
    metricsEnabled  = true
  )

  /** Create configuration with validation, throwing on invalid config. */
  def validated(
      expectedPerPart: Int         = 50_000,
      fpr: Double                  = 1e-4,
      bucketMs: Long               = 60_000L,
      maxSkew: FiniteDuration      = FiniteDuration(5, "seconds"),
      publishEvery: FiniteDuration = FiniteDuration(250, "milliseconds"),
      publishMinAdds: Int          = 64,
      bootMaxWait: FiniteDuration  = FiniteDuration(150, "milliseconds"),
      scaling: ScalingConfig       = ScalingConfig(),
      clockSkewMonitoring: Boolean = false,
      degradedModeEnabled: Boolean = true,
      metricsEnabled: Boolean      = false
  ): GuardConfiguration = {
    val config = GuardConfiguration(
      expectedPerPart,
      fpr,
      bucketMs,
      maxSkew,
      publishEvery,
      publishMinAdds,
      bootMaxWait,
      scaling,
      clockSkewMonitoring,
      degradedModeEnabled,
      metricsEnabled
    )

    config.validate match {
      case ConfigValidationResult.Valid => config
      case ConfigValidationResult.Invalid(errors) =>
        throw new IllegalArgumentException(s"Invalid configuration: ${errors.mkString(", ")}")
    }
  }
}
