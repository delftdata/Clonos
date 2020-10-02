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
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * When transitioning into this state, we send out Determinant Requests on all output channels and wait for all
 * responses to arrive.
 * When all responses arrive we transition to state {@link ReplayingState}
 */
public class WaitingDeterminantsState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(WaitingDeterminantsState.class);

	int numResponsesReceived;
	int numResponsesExpected;
	final DeterminantResponseEvent determinantAccumulator;

	public WaitingDeterminantsState(RecoveryManager context) {
		super(context);
		determinantAccumulator = new DeterminantResponseEvent(context.vertexGraphInformation.getThisTasksVertexID());
	}

	@Override
	public void executeEnter() {
		//Send all Replay requests, regardless of how we recover (causally or not), ensuring at-least-once processing
		sendInFlightLogReplayRequests();

		//By default, we should expect as many determinant responses as we have downstream neighbours
		numResponsesExpected = context.getNumberOfDirectDownstreamNeighbourVertexes();

		//If determinant sharing depth is 0, then we are not recovering causally, we can skip to the next state
		if (context.determinantSharingDepth == 0) {
			numResponsesExpected = 0;
			maybeGoToReplayingState();
			return;
		}

		//If we are a sink
		if (!context.vertexGraphInformation.hasDownstream()) {
			//With the transactional strategy, all determinants are dropped and we immediately switch to replaying
			if (RecoveryManager.sinkRecoveryStrategy == RecoveryManager.SinkRecoveryStrategy.TRANSACTIONAL) {
				numResponsesExpected = 0;
				maybeGoToReplayingState();
				return;
			}
			//This strategy (not implemented yet), will obtain a single message from kafka, containing determinants
			else if (RecoveryManager.sinkRecoveryStrategy == RecoveryManager.SinkRecoveryStrategy.KAFKA) {
				numResponsesExpected = 1;
				return;
			}
		}

		//Send all Determinant requests
		sendDeterminantRequests();

	}


	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		if (e.getVertexID().equals(context.vertexGraphInformation.getThisTasksVertexID())) {

			LOG.info("Received a DeterminantResponseEvent that is a direct response to my request: {}", e);
			numResponsesReceived++;
			synchronized (determinantAccumulator) {
				determinantAccumulator.merge(e);
			}

			maybeGoToReplayingState();


		} else
			super.notifyDeterminantResponseEvent(e);
	}


	@Override
	public void notifyNewInputChannel(RemoteInputChannel inputChannel, int channelIndex, int numBuffersRemoved) {
		//we got notified of a new input channel while we were recovering
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		IntermediateResultPartitionID requestReplayFor = inputChannel.getPartitionId().getPartitionId();
		try {
			inputChannel.sendTaskEvent(new InFlightLogRequestEvent(requestReplayFor, channelIndex,
				context.epochProvider.getCurrentEpochID()));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID, int index) {
		try {
			PipelinedSubpartition subpartition = context.subpartitionTable.get(intermediateResultPartitionID, index);
			DeterminantRequestEvent event =
				new DeterminantRequestEvent(context.vertexGraphInformation.getThisTasksVertexID(),
					context.epochProvider.getCurrentEpochID());
			subpartition.bypassDeterminantRequest(EventSerializer.toBufferConsumer(event));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		super.notifyStateRestorationComplete(checkpointId);
		maybeGoToReplayingState();
	}

	private void sendInFlightLogReplayRequests() {
		try {
			if (context.vertexGraphInformation.hasUpstream()) {
				for (SingleInputGate singleInputGate : context.inputGate.getInputGates()) {
					int consumedIndex = singleInputGate.getConsumedSubpartitionIndex();
					for (int i = 0; i < singleInputGate.getNumberOfInputChannels(); i++) {
						RemoteInputChannel inputChannel = (RemoteInputChannel) singleInputGate.getInputChannel(i);
						InFlightLogRequestEvent inFlightLogRequestEvent =
							new InFlightLogRequestEvent(inputChannel.getPartitionId().getPartitionId(), consumedIndex,
								context.epochProvider.getCurrentEpochID());
						LOG.info("Sending inFlightLog request {} through input gate {}, channel {}.",
							inFlightLogRequestEvent, singleInputGate, i);
						inputChannel.sendTaskEvent(inFlightLogRequestEvent);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendDeterminantRequests() {
		if (context.vertexGraphInformation.hasDownstream()) {
			LOG.info("Sending determinant requests");
			DeterminantRequestEvent determinantRequestEvent =
				new DeterminantRequestEvent(context.vertexGraphInformation.getThisTasksVertexID(),
					context.epochProvider.getCurrentEpochID());
			LOG.info("Sending determinant request: {}", determinantRequestEvent );
			broadcastDeterminantRequest(determinantRequestEvent);
		}
	}

	private void maybeGoToReplayingState() {
		if (numResponsesReceived == numResponsesExpected && !context.isRestoringState()) {
			LOG.info("Received all determinants, transitioning to Replaying state!");
			context.setState(new ReplayingState(context, determinantAccumulator));
		}
	}

}
