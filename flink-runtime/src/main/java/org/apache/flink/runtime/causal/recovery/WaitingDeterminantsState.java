/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal.recovery;

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * When transitioning into this state, we send out Determinant Requests on all output channels and wait for all
 * responses to arrive.
 * When all responses arrive we transition to state {@link ReplayingState}
 */
public class WaitingDeterminantsState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(WaitingDeterminantsState.class);

	int numResponsesReceived;
	int numResponsesExpected;
	final VertexCausalLogDelta delta;

	public WaitingDeterminantsState(RecoveryManager context) {
		super(context);
		this.numResponsesReceived = 0;
		this.numResponsesExpected = context.vertexGraphInformation.getNumberOfDirectDownstreamNeighbours();
		delta = new VertexCausalLogDelta(context.vertexGraphInformation.getThisTasksVertexID());
		try {
			//Send all Determinant requests
			if (context.vertexGraphInformation.hasDownstream()) {
				LOG.info("Sending determinant requests");
				DeterminantRequestEvent determinantRequestEvent = new DeterminantRequestEvent(context.vertexGraphInformation.getThisTasksVertexID());
				broadcastDeterminantRequest(determinantRequestEvent);
			}

			//Send all Replay requests
			if (context.vertexGraphInformation.hasUpstream()) {
				for (SingleInputGate singleInputGate : context.inputGate.getInputGates()) {
					int consumedIndex = singleInputGate.getConsumedSubpartitionIndex();
					for (int i = 0; i < singleInputGate.getNumberOfInputChannels(); i++) {
						RemoteInputChannel inputChannel = (RemoteInputChannel) singleInputGate.getInputChannel(i);
						InFlightLogRequestEvent inFlightLogRequestEvent = new InFlightLogRequestEvent(inputChannel.getPartitionId().getPartitionId(), consumedIndex, context.getFinalRestoreStateCheckpointId());
						LOG.info("Sending inFlightLog request {} through input gate {}, channel {}.", inFlightLogRequestEvent, singleInputGate, i);
						inputChannel.sendTaskEvent(inFlightLogRequestEvent);
					}
				}
			}


		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		//If we are a sink
		if (!context.vertexGraphInformation.hasDownstream()) {
			/**
			 * With the transactional strategy, all determinants are dropped and we immediately switch to replaying
			 */
			if (RecoveryManager.sinkRecoveryStrategy == RecoveryManager.SinkRecoveryStrategy.TRANSACTIONAL) {
				goToRunningState();
			} else if (RecoveryManager.sinkRecoveryStrategy == RecoveryManager.SinkRecoveryStrategy.KAFKA) {
				numResponsesExpected = 1;
			}
		}

	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		if (e.getVertexCausalLogDelta().getVertexId().equals(context.vertexGraphInformation.getThisTasksVertexID())) {

			LOG.info("Received a DeterminantResponseEvent that is a direct response to my request: {}", e);
			numResponsesReceived++;
			synchronized (delta) {
				delta.merge(e.getVertexCausalLogDelta());
			}
			if (numResponsesReceived == numResponsesExpected) {
				gotToReplayingState();
			}

		} else
			super.notifyDeterminantResponseEvent(e);
	}

	public void gotToReplayingState() {
		while (context.isRestoringState())
			LOG.info("Ready to replay, but waiting for restore state to finish"); //spin waiting
		LOG.info("Received all determinants, transitioning to Replaying state!");


		context.setState(new ReplayingState(context, delta));
		context.readyToReplayFuture.complete(null);//allow task to start running
	}

	public void goToRunningState() {
		while (context.isRestoringState())
			LOG.info("Ready to go to RunningState, but waiting for restore state to finish"); //spin waiting
		LOG.info("Received all determinants, transitioning directly to Running state!");
		context.setState(new RunningState(context));
		context.readyToReplayFuture.complete(null);//allow task to start running
	}

	@Override
	public void notifyNewInputChannel(RemoteInputChannel inputChannel, int channelIndex, int numBuffersRemoved) {
		//we got notified of a new input channel while we were recovering
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		IntermediateResultPartitionID requestReplayFor = inputChannel.getPartitionId().getPartitionId();
		try {
			inputChannel.sendTaskEvent(new InFlightLogRequestEvent(requestReplayFor, channelIndex, context.finalRestoredCheckpointId));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID, int index) {
		try {
			RecordWriter recordWriter = context.getRecordWriterByIntermediateResultPartitionID(intermediateResultPartitionID);
			PipelinedSubpartition subpartition = (PipelinedSubpartition) recordWriter.getResultPartition().getResultSubpartitions()[index];
			DeterminantRequestEvent event = new DeterminantRequestEvent(context.vertexGraphInformation.getThisTasksVertexID());
			subpartition.bypassDeterminantRequest(EventSerializer.toBufferConsumer(event));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
