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
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractState implements State {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractState.class);
	protected final RecoveryManager context;


	public AbstractState(RecoveryManager context) {
		this.context = context;
	}


	@Override
	public void notifyNewInputChannel(RemoteInputChannel remoteInputChannel, int consumedSubpartitionIndex,
									  int numBuffersRemoved) {
		//we got notified of a new input channel while we were recovering.
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, if we have already sent an inflight log request for this channel, we now have to send it again.
		LOG.info("Got notified of unexpected NewInputChannel event, while in state " + this.getClass());
	}

	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID,
									   int subpartitionIndex) {
		LOG.info("Got notified of unexpected NewOutputChannel event, while in state " + this.getClass());

	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		//we got an inflight log request while still recovering. Since we must finish recovery first before
		//answering, we store it, and when we enter the running state we immediately process it.
		context.unansweredInFlighLogRequests.put(e.getIntermediateResultPartitionID(), e.getSubpartitionIndex(), e);
	}

	@Override
	public void notifyStateRestorationStart(long checkpointId) {
		LOG.info("Started restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.put(checkpointId, true);
		if (checkpointId > context.epochProvider.getCurrentEpochID())
			context.epochProvider.setCurrentEpochID(checkpointId);

		for (PipelinedSubpartition ps : context.subpartitionTable.values())
			ps.setStartingEpoch(context.epochProvider.getCurrentEpochID());

		context.epochProvider.setCurrentEpochID(context.epochProvider.getCurrentEpochID());
	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		LOG.info("Completed restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.remove(checkpointId);
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		LOG.info("Received a DeterminantResponseEvent: {}", e);
		RecoveryManager.UnansweredDeterminantRequest udr =
			context.unansweredDeterminantRequests.get(e.getVertexCausalLogDelta().getVertexId());
		if (udr != null) {
			synchronized (udr.getLock()) {
				udr.incResponsesReceived();
				udr.getVertexCausalLogDelta().merge(e.getVertexCausalLogDelta());
				if (udr.getNumResponsesReceived() == context.vertexGraphInformation.getNumberOfDirectDownstreamNeighbours()) {
					context.unansweredDeterminantRequests.remove(e.getVertexCausalLogDelta().getVertexId());
					try {
						context.inputGate.getInputChannel(udr.getRequestingChannel()).sendTaskEvent(new DeterminantResponseEvent(udr.vertexCausalLogDelta));
						//TODO udr.getVertexCausalLogDelta().release();
					} catch (IOException | InterruptedException ex) {
						ex.printStackTrace();
					}
				}
			}
		} else
			LOG.info("Do not know whta this determinant response event refers to...");

	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		LOG.info("Received determinant request!");
		//If we are a sink and doing transactional recovery, just answer with what we have
		if (!context.vertexGraphInformation.hasDownstream() && RecoveryManager.sinkRecoveryStrategy == RecoveryManager.SinkRecoveryStrategy.TRANSACTIONAL) {
			try {
				context.inputGate.getInputChannel(channelRequestArrivedFrom).sendTaskEvent(new DeterminantResponseEvent(new VertexCausalLogDelta(e.getFailedVertex())));
			} catch (IOException | InterruptedException ex) {
				ex.printStackTrace();
			}
		} else {
			context.unansweredDeterminantRequests.put(e.getFailedVertex(),
				new RecoveryManager.UnansweredDeterminantRequest(e, channelRequestArrivedFrom));
			LOG.info("Recurring determinant request");
			broadcastDeterminantRequest(e);
		}
	}

	protected void broadcastDeterminantRequest(DeterminantRequestEvent e) {
		try (BufferConsumer event = EventSerializer.toBufferConsumer(e)) {
			for (PipelinedSubpartition ps : context.subpartitionTable.values())
				ps.bypassDeterminantRequest(event.copy());
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}


	@Override
	public void notifyStartRecovery() {
		LOG.info("Unexpected notification StartRecovery in state " + this.getClass());
	}

	@Override
	public void checkAsyncEvent() {
		throw new RuntimeException("Unexpected check for Async event in state" + this.getClass());
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
