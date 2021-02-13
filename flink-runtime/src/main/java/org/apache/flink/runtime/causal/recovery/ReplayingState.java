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

import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * In this state we do the actual process of recovery. Once done transition to {@link RunningState}
 * <p>
 * A downstream failure in this state does not matter, requests simply get added to the queue of unanswered requests.
 * Only when we finish our recovery do we answer those requests.
 * <p>
 * Upstream failures in this state mean that perhaps the upstream will not have sent all buffers. This means we must
 * resend
 * replay requests.
 */
public class ReplayingState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(ReplayingState.class);


	public ReplayingState(RecoveryManager recoveryManager, RecoveryManagerContext context) {
		super(recoveryManager, context);
	}

	public void executeEnter() {
		//Send all Replay requests, regardless of how we recover (causally or not), ensuring at-least-once processing
		sendInFlightLogReplayRequests();
		context.readyToReplayFuture.complete(null);//allow task to start running
	}

	@Override
	public void notifyNewInputChannel(InputChannel inputChannel, int consumedSubpartitionIndex,
									  int numberOfBuffersRemoved) {
		//we got notified of a new input channel while we were replaying
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, we have to resend the inflight log request, and ask to skip X buffers

		logInfoWithVertexID("Got notified of new input channel event, while in state " + this.getClass()
			+ " requesting upstream to replay and skip numberOfBuffersRemoved");
		if (!(inputChannel instanceof RemoteInputChannel))
			return;
		IntermediateResultPartitionID id = inputChannel.getPartitionId().getPartitionId();
		try {
			inputChannel.sendTaskEvent(new InFlightLogRequestEvent(id, consumedSubpartitionIndex));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}


	@Override
	public String toString() {
		return "ReplayingState{}";
	}


	private void sendInFlightLogReplayRequests() {
		try {
			if (context.vertexGraphInformation.hasUpstream()) {
				for (SingleInputGate singleInputGate : context.inputGate.getInputGates()) {
					int consumedIndex = singleInputGate.getConsumedSubpartitionIndex();
					for (int i = 0; i < singleInputGate.getNumberOfInputChannels(); i++) {
						InputChannel inputChannel = singleInputGate.getInputChannel(i);
						InFlightLogRequestEvent inFlightLogRequestEvent =
							new InFlightLogRequestEvent(inputChannel.getPartitionId().getPartitionId(),
								consumedIndex
							);
						logInfoWithVertexID("Sending inFlightLog request {} through input gate {}, channel {}.",
							inFlightLogRequestEvent, singleInputGate, i);
						inputChannel.sendTaskEvent(inFlightLogRequestEvent);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
