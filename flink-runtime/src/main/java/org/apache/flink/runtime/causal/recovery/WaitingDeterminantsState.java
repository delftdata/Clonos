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
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
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
	byte[] determinants;

	public WaitingDeterminantsState(RecoveryManager context) {
		super(context);
		this.numResponsesReceived = 0;
		try {
			//Send all Determinant requests
			DeterminantRequestEvent determinantRequestEvent = new DeterminantRequestEvent(context.vertexGraphInformation.getThisTasksVertexId());
			for (RecordWriter recordWriter : context.recordWriters) {
				LOG.info("Sending determinant request to RecordWriter {}", recordWriter);
				recordWriter.broadcastEvent(determinantRequestEvent);
			}

			//Send all Replay requests
			for (SingleInputGate singleInputGate : context.inputGate.getInputGates()) {
				InFlightLogRequestEvent inFlightLogRequestEvent = new InFlightLogRequestEvent(singleInputGate.getConsumedResultId(), singleInputGate.getConsumedSubpartitionIndex(), context.getFinalRestoreStateCheckpointId());
				LOG.info("Sending inFlightLog request {} through input gate {}.", inFlightLogRequestEvent, singleInputGate);
				singleInputGate.sendTaskEvent(inFlightLogRequestEvent);
			}


		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		if (e.getVertexId().equals(context.vertexGraphInformation.getThisTasksVertexId())) {

			LOG.info("Received a DeterminantResponseEvent that is a direct response to my request: {}", e);
			numResponsesReceived++;

			if (determinants == null || determinants.length < e.getDeterminants().length)
				this.determinants = e.getDeterminants();

			if (numResponsesReceived == context.vertexGraphInformation.getNumberOfDirectDownstreamNeighbours()) {
				while (context.isRestoringState())
					LOG.info("Ready to replay, but waiting for restore state to finish"); //spin waiting
				context.setState(new ReplayingState(context, determinants));
			}

		} else
			super.notifyDeterminantResponseEvent(e);
	}

	@Override
	public void notifyNewChannel(InputGate gate, int channelIndex, int numberOfBuffersRemoved){
		//we got notified of a new input channel while we were recovering
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//todo do we have to purge the input gate or channel?
		try {
			gate.getInputChannel(channelIndex).sendTaskEvent(new InFlightLogRequestEvent(((SingleInputGate)gate).getConsumedResultId(), ((SingleInputGate) gate).getConsumedSubpartitionIndex(), context.finalRestoredCheckpointId));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

}
