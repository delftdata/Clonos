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

import org.apache.flink.runtime.causal.*;
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
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
import java.util.concurrent.atomic.AtomicLong;

public class RecoveryManager implements IRecoveryManager {

	private static final Logger LOG = LoggerFactory.getLogger(RecoveryManager.class);

	public final VertexGraphInformation vertexGraphInformation;
	final CompletableFuture<Void> readyToReplayFuture;
	final JobCausalLog jobCausalLog;

	final ConcurrentMap<VertexID, UnansweredDeterminantRequest> unansweredDeterminantRequests;

	ConcurrentMap<IntermediateResultPartitionID, ConcurrentMap<Integer, InFlightLogRequestEvent>> unansweredInFlighLogRequests;

	final ConcurrentMap<Long, Boolean> incompleteStateRestorations;

	State currentState;

	InputGate inputGate;

	//TODO check iff needed, or instead can just keep the pipelined subpartitions
	List<RecordWriter> recordWriters;

	//TODO check iff needed, or instead can just keep the pipelined subpartitions
	Map<IntermediateResultPartitionID, RecordWriter> intermediateResultPartitionIDRecordWriterMap;

	EpochProvider epochProvider;

	RecordCountProvider recordCountProvider;

	static final SinkRecoveryStrategy sinkRecoveryStrategy = SinkRecoveryStrategy.TRANSACTIONAL;

	ProcessingTimeForceable processingTimeForceable;
	CheckpointForceable checkpointForceable;

	//TODO move to separate and compute from sink jobvertex
	public static enum SinkRecoveryStrategy {
		TRANSACTIONAL,
		KAFKA
	}

	public AtomicInteger numberOfRecoveringSubpartitions;

	public RecoveryManager(EpochProvider epochProvider, JobCausalLog jobCausalLog, CompletableFuture<Void> readyToReplayFuture, VertexGraphInformation vertexGraphInformation, RecordCountProvider recordCountProvider, CheckpointForceable checkpointForceable) {
		this.jobCausalLog = jobCausalLog;
		this.readyToReplayFuture = readyToReplayFuture;
		this.vertexGraphInformation = vertexGraphInformation;

		this.unansweredDeterminantRequests = new ConcurrentHashMap<>();

		this.incompleteStateRestorations = new ConcurrentHashMap<>();

		this.currentState = readyToReplayFuture == null ? new RunningState(this) : new StandbyState(this);

		LOG.info("Starting recovery manager in state {}", currentState);

		this.intermediateResultPartitionIDRecordWriterMap = new HashMap<>();

		this.epochProvider = epochProvider;
		this.recordCountProvider = recordCountProvider;
		numberOfRecoveringSubpartitions = new AtomicInteger(0);
		this.checkpointForceable = checkpointForceable;
	}

	public void setRecordWriters(List<RecordWriter> recordWriters) {
		this.recordWriters = recordWriters;
		unansweredInFlighLogRequests = new ConcurrentHashMap<>(recordWriters.size());
		for (RecordWriter recordWriter : recordWriters) {
			IntermediateResultPartitionID intermediateResultPartitionID = recordWriter.getResultPartition().getPartitionId().getPartitionId();
			this.intermediateResultPartitionIDRecordWriterMap.put(intermediateResultPartitionID, recordWriter);

			unansweredInFlighLogRequests.put(intermediateResultPartitionID, new ConcurrentHashMap<>(recordWriter.getResultPartition().getNumberOfSubpartitions()));
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
	public synchronized void notifyNewInputChannel(RemoteInputChannel inputChannel, int consumedSupartitionIndex, int numberBuffersRemoved) {
		this.currentState.notifyNewInputChannel(inputChannel, consumedSupartitionIndex, numberBuffersRemoved);
	}

	@Override
	public synchronized void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID, int index) {
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
		int requestingChannel;
		DeterminantRequestEvent event;

		/**
		 * The delta we are going to return. Starts empty, but is progressively merged with downstream deltas.
		 */
		VertexCausalLogDelta vertexCausalLogDelta;

		public UnansweredDeterminantRequest(DeterminantRequestEvent event, int requestingChannel) {
			this.numResponsesReceived = 0;
			this.requestingChannel = requestingChannel;
			this.event = event;
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
	}


}
