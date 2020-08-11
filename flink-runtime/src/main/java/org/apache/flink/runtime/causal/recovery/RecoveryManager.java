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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.flink.runtime.causal.*;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RecoveryManager implements IRecoveryManager {

	private static final Logger LOG = LoggerFactory.getLogger(RecoveryManager.class);

	public final VertexGraphInformation vertexGraphInformation;
	final CompletableFuture<Void> readyToReplayFuture;
	final JobCausalLog jobCausalLog;

	final ConcurrentMap<VertexID, UnansweredDeterminantRequest> unansweredDeterminantRequests;

	Table<IntermediateResultPartitionID, Integer, InFlightLogRequestEvent> unansweredInFlighLogRequests;

	final ConcurrentMap<Long, Boolean> incompleteStateRestorations;

	State currentState;

	InputGate inputGate;

	Table<IntermediateResultPartitionID, Integer, PipelinedSubpartition> subpartitionTable;

	EpochProvider epochProvider;

	RecordCountProvider recordCountProvider;

	static final SinkRecoveryStrategy sinkRecoveryStrategy = SinkRecoveryStrategy.TRANSACTIONAL;

	ProcessingTimeForceable processingTimeForceable;
	CheckpointForceable checkpointForceable;

	public enum SinkRecoveryStrategy {
		TRANSACTIONAL,
		KAFKA
	}

	public AtomicInteger numberOfRecoveringSubpartitions;

	public RecoveryManager(EpochProvider epochProvider, JobCausalLog jobCausalLog,
						   CompletableFuture<Void> readyToReplayFuture, VertexGraphInformation vertexGraphInformation,
						   RecordCountProvider recordCountProvider, CheckpointForceable checkpointForceable, ResultPartition[] partitions) {
		this.jobCausalLog = jobCausalLog;
		this.readyToReplayFuture = readyToReplayFuture;
		this.vertexGraphInformation = vertexGraphInformation;

		this.unansweredDeterminantRequests = new ConcurrentHashMap<>();

		this.incompleteStateRestorations = new ConcurrentHashMap<>();

		this.currentState = readyToReplayFuture == null ? new RunningState(this) : new StandbyState(this);

		LOG.info("Starting recovery manager in state {}", currentState);

		this.epochProvider = epochProvider;
		this.recordCountProvider = recordCountProvider;
		this.numberOfRecoveringSubpartitions = new AtomicInteger(0);
		this.checkpointForceable = checkpointForceable;
		setPartitions(partitions);
	}

	private void setPartitions(ResultPartition[] partitions) {
		int maxNumSubpart =
			Arrays.stream(partitions).mapToInt(ResultPartition::getNumberOfSubpartitions).max().orElse(0);

		this.subpartitionTable = HashBasedTable.create(partitions.length, maxNumSubpart);
		//todo: unansweredInFlightLogRequests may be unnecessary if we store the request in the subpartition
		this.unansweredInFlighLogRequests = HashBasedTable.create(partitions.length, maxNumSubpart);

		for (ResultPartition rp : partitions) {
			IntermediateResultPartitionID partitionID = rp.getPartitionId().getPartitionId();
			ResultSubpartition[] subpartitions = rp.getResultSubpartitions();
			for (int i = 0; i < subpartitions.length; i++)
				this.subpartitionTable.put(partitionID, i, (PipelinedSubpartition) subpartitions[i]);
		}
	}

	@Override
	public void setProcessingTimeService(ProcessingTimeForceable processingTimeForceable) {
		this.processingTimeForceable = processingTimeForceable;
	}

	@Override
	public ProcessingTimeForceable getProcessingTimeForceable() {
		return processingTimeForceable;
	}

	@Override
	public CheckpointForceable getCheckpointForceable() {
		return checkpointForceable;
	}

	public void setInputGate(InputGate inputGate) {
		this.inputGate = inputGate;
	}

//====================== State Machine Messages ========================================

	@Override
	public synchronized void notifyStartRecovery() {
		this.currentState.notifyStartRecovery();
	}


	@Override
	public synchronized void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		//TODO future work, allow for different handlers to handle DeterminantResponseEvents
		// whose determinants are not found
		// For now, these are just merged, providing at least once guarantees in the case of multiple failures with low
		// determinant sharing depth
		this.currentState.notifyDeterminantResponseEvent(e);
	}

	@Override
	public synchronized void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		this.currentState.notifyDeterminantRequestEvent(e, channelRequestArrivedFrom);
	}

	@Override
	public synchronized void notifyStateRestorationStart(long checkpointId) {
		this.currentState.notifyStateRestorationStart(checkpointId);
	}

	@Override
	public synchronized void notifyStateRestorationComplete(long checkpointId) {
		this.currentState.notifyStateRestorationComplete(checkpointId);
	}

	@Override
	public synchronized void notifyNewInputChannel(RemoteInputChannel inputChannel, int consumedSupartitionIndex,
												   int numberBuffersRemoved) {
		this.currentState.notifyNewInputChannel(inputChannel, consumedSupartitionIndex, numberBuffersRemoved);
	}

	@Override
	public synchronized void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID,
													int index) {
		this.currentState.notifyNewOutputChannel(intermediateResultPartitionID, index);
	}

	@Override
	public synchronized void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		this.currentState.notifyInFlightLogRequestEvent(e);
	}

	@Override
	public synchronized void checkAsyncEvent() {
		this.currentState.checkAsyncEvent();
	}


	@Override
	public synchronized void setState(State state) {
		this.currentState = state;
		this.currentState.executeEnter();
	}

	//============== Check state ==========================
	@Override
	public synchronized boolean isRunning() {
		return currentState instanceof RunningState;
	}

	@Override
	public synchronized boolean isReplaying() {
		return currentState instanceof ReplayingState;
	}

	@Override
	public synchronized boolean isRestoringState() {
		return !incompleteStateRestorations.isEmpty();
	}

	@Override
	public synchronized boolean isWaitingConnections() {
		return currentState instanceof WaitingConnectionsState;
	}

	@Override
	public synchronized boolean isRecovering() {
		if (!isRunning())
			return true;

		return numberOfRecoveringSubpartitions.get() != 0;
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

	public static class UnansweredDeterminantRequest {
		private int numResponsesReceived;
		private int requestingChannel;
		private DeterminantRequestEvent event;
		private final Object lock;

		/**
		 * The delta we are going to return. Starts empty, but is progressively merged with downstream deltas.
		 */
		VertexCausalLogDelta vertexCausalLogDelta;

		public UnansweredDeterminantRequest(DeterminantRequestEvent event, int requestingChannel) {
			this.numResponsesReceived = 0;
			this.requestingChannel = requestingChannel;
			this.event = event;
			lock = new Object();
			vertexCausalLogDelta = new VertexCausalLogDelta(event.getFailedVertex());
		}

		public int getNumResponsesReceived() {
			return numResponsesReceived;
		}


		public int getRequestingChannel() {
			return requestingChannel;
		}

		public void incResponsesReceived() {
			numResponsesReceived++;
		}

		public VertexCausalLogDelta getVertexCausalLogDelta() {
			return vertexCausalLogDelta;
		}

		public DeterminantRequestEvent getEvent() {
			return event;
		}

		public Object getLock() {
			return lock;
		}
	}


}
