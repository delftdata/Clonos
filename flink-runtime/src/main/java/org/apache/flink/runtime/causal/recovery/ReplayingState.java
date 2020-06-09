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

import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.causal.log.thread.SubpartitionThreadLogDelta;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.*;

/**
 * In this state we do the actual process of recovery. Once done transition to {@link RunningState}
 * <p>
 * A downstream failure in this state does not matter, requests simply get added to the queue of unanswered requests.
 * Only when we finish our recovery do we answer those requests.
 * <p>
 * Upstream failures in this state mean that perhaps the upstream will not have sent all buffers. This means we must resend
 * replay requests.
 */
public class ReplayingState extends AbstractState {

	DeterminantEncodingStrategy determinantEncodingStrategy;

	ByteBuf mainThreadRecoveryBuffer;

	List<Thread> recoveryThreads;


	public ReplayingState(RecoveryManager context, VertexCausalLogDelta recoveryDeterminants) {
		super(context);

		LOG.info("Entered replaying state with delta: {}", recoveryDeterminants);
		determinantEncodingStrategy = context.jobCausalLog.getDeterminantEncodingStrategy();

		recoveryThreads = new LinkedList<>();
		createSubpartitionRecoveryThreads(recoveryDeterminants);

		if (recoveryDeterminants.getMainThreadDelta() != null)
			this.mainThreadRecoveryBuffer = recoveryDeterminants.getMainThreadDelta().getRawDeterminants();

		checkFinished();
	}

	private void createSubpartitionRecoveryThreads(VertexCausalLogDelta recoveryDeterminants) {

		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> partitionEntry : recoveryDeterminants.getPartitionDeltas().entrySet()) {
			ResultSubpartition[] subpartitions = context.intermediateResultPartitionIDRecordWriterMap.get(partitionEntry.getKey()).getResultPartition().getResultSubpartitions();

			for (Map.Entry<Integer, SubpartitionThreadLogDelta> subpartitionEntry : partitionEntry.getValue().entrySet()) {
				LOG.info("Created recovery thread for Partition {} subpartition index {} with buffer {}", partitionEntry.getKey(), subpartitionEntry.getKey(), subpartitionEntry.getValue().getRawDeterminants());
				PipelinedSubpartition pipelinedSubpartition = (PipelinedSubpartition) subpartitions[subpartitionEntry.getKey()];
				Thread t = new SubpartitionRecoveryThread(subpartitionEntry.getValue().getRawDeterminants(), pipelinedSubpartition, context, partitionEntry.getKey(), subpartitionEntry.getKey());
				recoveryThreads.add(t);
				t.start();
			}
		}

	}

	@Override
	public void notifyNewInputChannel(RemoteInputChannel remoteInputChannel, int consumedSubpartitionIndex, int numberOfBuffersRemoved) {
		//we got notified of a new input channel while we were replaying
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, we have to resend the inflight log request, and ask to skip X buffers

		LOG.info("Got notified of new input channel event, while in state " + this.getClass() + " requesting upstream to replay and skip numberOfBuffersRemoved");
		IntermediateResultPartitionID id = remoteInputChannel.getPartitionId().getPartitionId();
		try {
			remoteInputChannel.sendTaskEvent(new InFlightLogRequestEvent(id, consumedSubpartitionIndex, context.finalRestoredCheckpointId, numberOfBuffersRemoved));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}


	//===================

	@Override
	public int replayRandomInt() {
		Determinant nextDeterminant = determinantEncodingStrategy.decodeNext(mainThreadRecoveryBuffer);
		if (!(nextDeterminant instanceof RNGDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected RNG, but got: " + nextDeterminant);
		checkFinished();
		return ((RNGDeterminant) nextDeterminant).getNumber();
	}

	@Override
	public byte replayNextChannel() {
		Determinant nextDeterminant = determinantEncodingStrategy.decodeNext(mainThreadRecoveryBuffer);
		if (!(nextDeterminant instanceof OrderDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Order, but got: " + nextDeterminant);

		checkFinished();
		return ((OrderDeterminant) nextDeterminant).getChannel();
	}

	@Override
	public long replayNextTimestamp() {
		Determinant nextDeterminant = determinantEncodingStrategy.decodeNext(mainThreadRecoveryBuffer);
		if (!(nextDeterminant instanceof TimestampDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Timestamp, but got: " + nextDeterminant);

		checkFinished();
		return ((TimestampDeterminant) nextDeterminant).getTimestamp();
	}

	private void checkFinished() {
		if (mainThreadRecoveryBuffer == null || !mainThreadRecoveryBuffer.isReadable()) {
			if (mainThreadRecoveryBuffer != null)
				mainThreadRecoveryBuffer.release();
			LOG.info("Finished recovering main thread! Transitioning to RunningState!");
			context.setState(new RunningState(context));
		}

	}

	@Override
	public String toString() {
		return "ReplayingState{}";
	}

	private static class SubpartitionRecoveryThread extends Thread {
		private final PipelinedSubpartition pipelinedSubpartition;
		private final ByteBuf recoveryBuffer;
		private final DeterminantEncodingStrategy determinantEncodingStrategy;
		private final RecoveryManager context;
		private final IntermediateResultPartitionID partitionID;
		private final int index;

		public SubpartitionRecoveryThread(ByteBuf recoveryBuffer, PipelinedSubpartition pipelinedSubpartition, RecoveryManager context, IntermediateResultPartitionID partitionID, int index) {
			this.recoveryBuffer = recoveryBuffer;
			this.pipelinedSubpartition = pipelinedSubpartition;
			this.determinantEncodingStrategy = context.jobCausalLog.getDeterminantEncodingStrategy();
			this.context = context;
			this.partitionID = partitionID;
			this.index = index;
		}

		@Override
		public void run() {
			//1. Netty has been told that there is no data.
			pipelinedSubpartition.setIsRecovering(true);
			//2. Rebuild in-fligh log and subpartition state
			while (recoveryBuffer.isReadable()) {

				Determinant determinant = determinantEncodingStrategy.decodeNext(recoveryBuffer);

				if (!(determinant instanceof BufferBuiltDeterminant))
					throw new RuntimeException("Subpartition has corrupt recovery buffer, expected buffer built, got: " + determinant);
				BufferBuiltDeterminant bufferBuiltDeterminant = (BufferBuiltDeterminant) determinant;

				LOG.info("Requesting to build and log buffer with {} bytes", bufferBuiltDeterminant.getNumberOfBytes());
				pipelinedSubpartition.buildAndLogBuffer(bufferBuiltDeterminant.getNumberOfBytes());
				LOG.info("Finished building and logging one buffer in supartition recovery thread");
			}
			// If there is a replay request, we have to prepare it, before setting isRecovering to true
			InFlightLogRequestEvent unansweredRequest = context.unansweredInFlighLogRequests.get(partitionID).remove(index);
			if(unansweredRequest != null)
				pipelinedSubpartition.requestReplay(unansweredRequest.getCheckpointId(), unansweredRequest.getNumberOfBuffersToSkip());

			//3. Tell netty to restart requesting buffers.
			pipelinedSubpartition.setIsRecovering(false);
			pipelinedSubpartition.notifyDataAvailable();
			recoveryBuffer.release();
			LOG.info("Done recovering pipelined subpartition");
		}
	}

}
