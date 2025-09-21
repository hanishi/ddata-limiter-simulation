package com.example.bloom

import com.example.CborSerializable
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.pekko.actor.typed.scaladsl.{ ActorContext, Behaviors }
import org.apache.pekko.actor.typed.{ ActorRef, Behavior }
import org.apache.pekko.cluster.ddata.Replicator.GetSuccess
import org.apache.pekko.cluster.ddata.typed.scaladsl.{ DistributedData, Replicator }
import org.apache.pekko.cluster.ddata.{ LWWRegister, LWWRegisterKey, SelfUniqueAddress }
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ EntityContext, EntityTypeKey }

import java.util.zip.CRC32
import scala.concurrent.duration.*

/** Snapshot envelope stored in DData (one key per partition). */
final case class SnapshotEnvelope(
    version: Int,
    mBits: Int,
    k: Int,
    rotatedAtNanos: Long,
    currentBucket: Long,
    prevBucket: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long]) current: Array[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long]) previous: Array[Long],
    crc32: Long
) extends CborSerializable

object WindowedBloomReplayGuard {

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("windowed-bloom-replay-guard")

  def initBehavior(
      entityCtx: EntityContext[Command],
      config: GuardConfiguration
  ): Behavior[Command] = {
    val part = parsePart(entityCtx.entityId)
    WindowedBloomReplayGuard(part, config)
  }

  /** Parse entityId of the form "replay|<part>" */
  private def parsePart(entityId: String): Int = {
    val parts = entityId.split("\\|")
    require(
      parts.length == 2 && parts(0) == "replay",
      s"Invalid entityId '$entityId', expected replay|<part>"
    )
    parts(1).toInt
  }

  def apply(partitionId: Int, config: GuardConfiguration): Behavior[Command] = {
    // Validate configuration
    config.validate match {
      case ConfigValidationResult.Invalid(errors) =>
        throw new IllegalArgumentException(s"Invalid guard configuration: ${errors.mkString(", ")}")
      case ConfigValidationResult.Valid => // proceed
    }

    val key = snapKey(partitionId)

    // Initialize components
    val bucketStrategy = new BucketRotationStrategy(config.bucketMs)
    val timingAnalyzer = new EventTimingAnalyzer(bucketStrategy, config.extendedSkew)
    val scalingPolicy  = new AutoScalingPolicy(config.scaling, config.fpr)

    Behaviors.setup { ctx =>
      val node: SelfUniqueAddress = DistributedData(ctx.system).selfUniqueAddress

      DistributedData.withReplicatorMessageAdapter[Command, LWWRegister[SnapshotEnvelope]] { repl =>

        repl.askGet(
          ask => Replicator.Get(key, Replicator.ReadLocal, ask),
          InternalGetResponse.apply
        )

        Behaviors.withTimers { timers =>
          var bootLoaded = false

          timers.startSingleTimer("boot-deadline", BootDeadline, config.bootMaxWait)
          timers.startTimerAtFixedRate("publish", Publish, config.publishEvery)
          val statsEvery = (config.bucketMs / 4).millis
          timers.startTimerAtFixedRate("stats", StatsTick, statsEvery)

          Behaviors.withStash(Int.MaxValue) { buffer =>
            def loading: Behavior[Command] =
              Behaviors.receiveMessage {
                case InternalGetResponse(success @ Replicator.GetSuccess(`key`)) =>
                  val snapshotEnvelope = success.get(key).value
                  val initialState = ReplayGuardState.fromSnapshot(
                    snapshotEnvelope,
                    config,
                    bucketStrategy,
                    ctx,
                    partitionId
                  )
                  bootLoaded = true
                  timers.cancel("boot-deadline")
                  timers.cancel("boot-finalize")
                  buffer.unstashAll(active(initialState))

                case InternalGetResponse(_: Replicator.NotFound[_] | _: Replicator.GetFailure[_]) =>
                  Behaviors.same

                case BootDeadline =>
                  ctx.log.debug(
                    s"WindowedBloomReplayGuard[part=$partitionId] boot: local read deadline reached, trying ReadMajority"
                  )
                  repl.askGet(
                    ask => Replicator.Get(key, Replicator.ReadMajority(config.bootMaxWait), ask),
                    InternalGetResponse.apply
                  )
                  timers.startSingleTimer("boot-finalize", BootFinalize, config.bootMaxWait)
                  Behaviors.same

                case BootFinalize =>
                  ctx.log.debug(
                    s"WindowedBloomReplayGuard[part=$partitionId] boot: majority grace elapsed, proceeding"
                  )
                  val initialState =
                    ReplayGuardState.initial(config, bucketStrategy).forceInitialPublish
                  buffer.unstashAll(active(initialState))

                case other =>
                  buffer.stash(other)
                  Behaviors.same
              }

            def active(state: ReplayGuardState): Behavior[Command] =
              Behaviors
                .receive[Command] { (ctx, msg) =>
                  msg match {
                    case ValidateReplayAt(replyTo, nonce, eventTimeMs) =>
                      // Check for bucket rotation
                      val rotatedState = bucketStrategy.checkRotation(state.currentBucket) match {
                        case Some(rotation) =>
                          val elapsedMs = (System.nanoTime() - state.rotatedAtNs) / 1_000_000L
                          rotation.logRotation(ctx, partitionId, elapsedMs, bucketStrategy)

                          // Check for pending scaling
                          val appliedScaling = scalingPolicy.shouldApplyPendingScale(
                            state.scaling,
                            state.bloomCurrent.mBits,
                            state.bloomCurrent.k
                          )
                          appliedScaling.foreach(
                            _.logScaling(ctx, partitionId, state.scaling.currentExpectedPerPart)
                          )

                          state.applyRotation(
                            rotation,
                            appliedScaling,
                            config.expectedPerPart,
                            config.fpr
                          )
                        case None => state
                      }

                      // Process validation
                      val (newState, result) =
                        rotatedState.processValidation(nonce, eventTimeMs, timingAnalyzer)

                      val success = result match {
                        case ValidationResult.Granted(_) => true
                        case ValidationResult.Denied(_) => false
                      }

                      replyTo ! success
                      active(newState)

                    case StatsTick =>
                      state.metrics.logStats(ctx, partitionId, config.bucketMs)
                      active(state.resetMetrics)

                    case Publish =>
                      var updatedState = state

                      // Check for auto-scaling proposal
                      if (state.scaling.pendingScaleExpected == 0) {
                        scalingPolicy.shouldScale(state.bloomCurrent, state.scaling).foreach {
                          proposal =>
                            proposal.logProposal(
                              ctx,
                              partitionId,
                              state.scaling.currentExpectedPerPart,
                              state.bloomCurrent
                            )
                            updatedState =
                              updatedState.withScalingProposal(proposal.newExpectedPerPart)
                        }
                      }

                      // Check if should publish
                      if (updatedState.publishing.shouldPublish(config)) {
                        val curV  = updatedState.bloomCurrent.snapshotWords
                        val prevV = updatedState.bloomPrevious.snapshotWords
                        val crc = computeCrc32(
                          curV,
                          prevV,
                          updatedState.bloomCurrent.mBits,
                          updatedState.bloomCurrent.k,
                          updatedState.currentBucket,
                          updatedState.previousBucket
                        )
                        val snap = SnapshotEnvelope(
                          version        = 1,
                          mBits          = updatedState.bloomCurrent.mBits,
                          k              = updatedState.bloomCurrent.k,
                          rotatedAtNanos = updatedState.rotatedAtNs,
                          currentBucket  = updatedState.currentBucket,
                          prevBucket     = updatedState.previousBucket,
                          current        = curV,
                          previous       = prevV,
                          crc32          = crc
                        )
                        repl.askUpdate(
                          ask =>
                            Replicator.Update(
                              key,
                              LWWRegister(node, snap),
                              Replicator.WriteLocal,
                              ask
                            )(reg => reg.withValue(node, snap)),
                          InternalUpdateResponse.apply
                        )

                        updatedState =
                          updatedState.copy(publishing = updatedState.publishing.startPublish)
                      }
                      active(updatedState)

                    case InternalUpdateResponse(_) =>
                      active(state.publishCompleted)

                    case InternalGetResponse(_) | BootDeadline | BootFinalize =>
                      active(state)
                  }
                }
                .receiveSignal { (_, PostStop) =>
                  ctx.log.info("WindowedBloomReplayGuard stopped (part={})", Int.box(partitionId))
                  Behaviors.same
                }
                .narrow
            loading
          }
        }
      }
    }
  }

  /** DData key for this partition. Using LWWRegister since we have a single writer (the owner). */
  private def snapKey(part: Int): LWWRegisterKey[SnapshotEnvelope] =
    LWWRegisterKey[SnapshotEnvelope](s"replay|$part")

  private def computeCrc32(
      cur: Array[Long],
      prev: Array[Long],
      mBits: Int,
      k: Int,
      curB: Long,
      prevB: Long
  ): Long = {
    val crc = new CRC32()
    putInt(crc, mBits); putInt(crc, k); putLong(crc, curB); putLong(crc, prevB)
    cur.foreach(putLong(crc, _))
    prev.foreach(putLong(crc, _))
    crc.getValue
  }

  private def putInt(crc: CRC32, v: Int): Unit = {
    crc.update((v >>> 24) & 0xff); crc.update((v >>> 16) & 0xff); crc.update((v >>> 8) & 0xff)
    crc.update(v & 0xff)
  }

  private def putLong(crc: CRC32, v: Long): Unit = {
    crc.update(((v >>> 56) & 0xff).toInt); crc.update(((v >>> 48) & 0xff).toInt)
    crc.update(((v >>> 40) & 0xff).toInt); crc.update(((v >>> 32) & 0xff).toInt)
    crc.update(((v >>> 24) & 0xff).toInt); crc.update(((v >>> 16) & 0xff).toInt)
    crc.update(((v >>> 8) & 0xff).toInt); crc.update((v & 0xff).toInt)
  }

  sealed trait Command

  final case class ValidateReplayAt(
      replyTo: ActorRef[Boolean],
      nonce: Long,
      eventTimeMs: Long = System.currentTimeMillis()
  ) extends Command,
        CborSerializable

  private final case class InternalGetResponse(
      rsp: Replicator.GetResponse[LWWRegister[SnapshotEnvelope]]
  ) extends Command

  private final case class InternalUpdateResponse(
      rsp: Replicator.UpdateResponse[LWWRegister[SnapshotEnvelope]]
  ) extends Command

  private case object Publish extends Command

  private case object BootDeadline extends Command

  private case object BootFinalize extends Command

  private case object StatsTick extends Command
}
