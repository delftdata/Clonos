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

import org.apache.flink.runtime.causal.CausalLoggingManager;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RecoveryManager implements IRecoveryManager{

	private static final Logger LOG = LoggerFactory.getLogger(RecoveryManager.class);

	final VertexGraphInformation vertexGraphInformation;
	final CompletableFuture<Void> readyToReplayFuture;
	final CausalLoggingManager causalLoggingManager;

	final Map<Integer, DeterminantRequestEvent> inputChannelToUnansweredDeterminantRequestEvent;
	final Map<Integer, InFlightLogRequestEvent> outputChannelToUnansweredInFlightLogRequestEvent;

	final Map<Long, Boolean> incompleteStateRestorations;

	State currentState;
	InputGate inputGate;
	List<RecordWriter> recordWriters;

	long finalRestoredCheckpointId;

	Map<IntermediateDataSetID, ResultPartitionWriter> intermediateDataSetIDResultPartitionMap;

	public RecoveryManager(CausalLoggingManager causalLoggingManager, CompletableFuture<Void> readyToReplayFuture, VertexGraphInformation vertexGraphInformation) {
		this.causalLoggingManager = causalLoggingManager;
		this.readyToReplayFuture = readyToReplayFuture;
		this.vertexGraphInformation = vertexGraphInformation;

		this.inputChannelToUnansweredDeterminantRequestEvent = new HashMap<>();
		this.outputChannelToUnansweredInFlightLogRequestEvent = new HashMap<>();
		this.incompleteStateRestorations = new HashMap<>();

		this.finalRestoredCheckpointId = 0;

		this.currentState = readyToReplayFuture == null ? new RunningState(this) : new StandbyState(this);

		LOG.info("Starting recovery manager in state {}", currentState);

		this.intermediateDataSetIDResultPartitionMap = new HashMap<>();
	}

	public void setRecordWriters(List<RecordWriter> recordWriters) {
		this.recordWriters = recordWriters;

		for(RecordWriter recordWriter : recordWriters){
			this.intermediateDataSetIDResultPartitionMap.put(recordWriter.getResultPartition().getIntermediateDataSetID(), recordWriter.getResultPartition());
		}
	}

	public void setInputGate(InputGate inputGate){
		this.inputGate = inputGate;
	}

//====================== State Machine Messages ========================================

	@Override
	public void notifyStartRecovery(){
		this.currentState.notifyStartRecovery();
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		this.currentState.notifyDeterminantResponseEvent(e);
	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e,int channelRequestArrivedFrom) {
		this.currentState.notifyDeterminantRequestEvent(e, channelRequestArrivedFrom);
	}

	@Override
	public void notifyStateRestorationStart(long checkpointId) {
		this.currentState.notifyStateRestorationStart(checkpointId);
	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		this.currentState.notifyStateRestorationComplete(checkpointId);
	}


	@Override
	public void notifyNewChannel(InputGate gate, int channelIndex) {
		this.currentState.notifyNewChannel(gate, channelIndex);
	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		this.currentState.notifyInFlightLogRequestEvent(e);
	}


	@Override
	public void setState(State state) {
		this.currentState = state;
	}

	//============== Check state ==========================
	@Override
	public boolean isRunning() {
		return currentState instanceof RunningState;
	}

	@Override
	public boolean isReplaying() {
		return currentState instanceof ReplayingState;
	}

	@Override
	public boolean isRestoringState() {
		return !incompleteStateRestorations.isEmpty();
	}

	@Override
	public long getFinalRestoreStateCheckpointId() {
		return finalRestoredCheckpointId;
	}

	//=============== Consult determinants ==============================
	@Override
	public int replayRandomInt() {
		return currentState.replayRandomInt();
	}

	@Override
	public byte replayNextChannel() {
		return currentState.replayNextChannel();
	}

	@Override
	public long replayNextTimestamp() {
		return currentState.replayNextTimestamp();
	}
}
