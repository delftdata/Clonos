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
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractState implements State {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractState.class);
	protected final RecoveryManager context;


	public AbstractState(RecoveryManager context){
		this.context = context;
	}


	@Override
	public void notifyNewInputChannel(InputGate gate, int channelIndex, int numberOfBuffersRemoved){
		//we got notified of a new input channel while we were recovering.
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, if we have already sent an inflight log request for this channel, we now have to send it again.
		LOG.info("Got notified of unexpected NewInputChannel event, while in state " + this.getClass());
	}

	@Override
	public void notifyNewOutputChannel(IntermediateDataSetID intermediateDataSetID, int subpartitionIndex){
		LOG.info("Got notified of unexpected NewOutputChannel event, while in state " + this.getClass());

	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		//we got an inflight log request while still recovering. Since we must finish recovery first before
		//answering, we store it, and when we enter the running state we immediately process it.
		context.unansweredInFlighLogRequests.add(e);
	}

	@Override
	public void notifyStateRestorationStart(long checkpointId) {
		LOG.info("Started restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.put(checkpointId, true);
		if(checkpointId > context.finalRestoredCheckpointId)
			context.finalRestoredCheckpointId = checkpointId;
		//throw new RuntimeException("Unexpected notification StateRestorationStart in state " + this.getClass());

	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		LOG.info("Completed restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.remove(checkpointId);
		//throw new RuntimeException("Unexpected notification StateRestorationComplete in state " + this.getClass());
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		LOG.info("Received a DeterminantResponseEvent, but am not recovering: {}", e);
		RecoveryManager.UnansweredDeterminantRequest udr = context.unansweredDeterminantRequests.get(e.getVertexId());
		if(udr != null) {
			udr.incResponsesReceived();
			if (e.getDeterminants().length > udr.determinants.length)
				udr.setDeterminants(e.getDeterminants());

			if (udr.getNumResponsesReceived() == context.vertexGraphInformation.getNumberOfDirectDownstreamNeighbours()) {
				context.unansweredDeterminantRequests.remove(e.getVertexId());
				try {
					context.inputGate.getInputChannel(udr.getRequestingChannel()).sendTaskEvent(new DeterminantResponseEvent(udr.getVertexId(), udr.getDeterminants()));
				} catch (IOException | InterruptedException ex) {
					ex.printStackTrace();
				}
			}
		} else
			LOG.info("Do not know whta this determinant response event refers to...");

	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e,int channelRequestArrivedFrom) {
		context.unansweredDeterminantRequests.put(e.getFailedVertex(), new RecoveryManager.UnansweredDeterminantRequest(e.getFailedVertex(), channelRequestArrivedFrom));
		for (RecordWriter recordWriter : context.recordWriters) {
			LOG.info("Recurring determinant request to RecordWriter {}", recordWriter);
			try {
				recordWriter.broadcastEvent(e);
			} catch (IOException | InterruptedException ex) {
				ex.printStackTrace();
			}
		}
	}

	@Override
	public void notifyStartRecovery() {
		LOG.info("Unexpected notification StartRecovery in state " + this.getClass());
	}

	//==============================================================

	@Override
	public int replayRandomInt() {
		throw new RuntimeException("Unexpected replayRandomInt request in state " + this.getClass());
	}

	@Override
	public byte replayNextChannel() {
		throw new RuntimeException("Unexpected replayNextChannel request in state " + this.getClass());
	}

	@Override
	public long replayNextTimestamp() {
		throw new RuntimeException("Unexpected replayNextTimestamp request in state " + this.getClass());
	}
}
