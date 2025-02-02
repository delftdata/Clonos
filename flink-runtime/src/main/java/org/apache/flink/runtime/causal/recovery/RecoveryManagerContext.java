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
import org.apache.flink.runtime.causal.determinant.AsyncDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Table;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RecoveryManagerContext implements IRecoveryManagerContext {
	RecoveryManager owner;

	public final VertexGraphInformation vertexGraphInformation;
	public final short vertexID;
	final JobCausalLog causalLog;

	final Set<Long> incompleteStateRestorations;

	InputGate inputGate;
	Table<IntermediateResultPartitionID, Integer, PipelinedSubpartition> subpartitionTable;

	final EpochTracker epochTracker;

	ProcessingTimeForceable processingTimeForceable;
	CheckpointForceable checkpointForceable;

	final Table<VertexID, Long, UnansweredDeterminantRequest> unansweredDeterminantRequests;
	final Table<IntermediateResultPartitionID, Integer, InFlightLogRequestEvent> unansweredInFlightLogRequests;
	final List<AsyncDeterminant> unansweredRPCRequests;

	final AbstractInvokable invokable;
	final CompletableFuture<Void> readyToReplayFuture;


	public RecoveryManagerContext(AbstractInvokable invokable, JobCausalLog causalLog,
								  CompletableFuture<Void> readyToReplayFuture, VertexGraphInformation vertexGraphInformation,
								  EpochTracker epochTracker, CheckpointForceable checkpointForceable,
								  ResultPartition[] partitions) {
		this.invokable = invokable;
		this.causalLog = causalLog;
		this.readyToReplayFuture = readyToReplayFuture;
		this.vertexGraphInformation = vertexGraphInformation;
		this.vertexID = vertexGraphInformation.getThisTasksVertexID().getVertexID();

		this.unansweredDeterminantRequests = HashBasedTable.create();

		this.incompleteStateRestorations = new HashSet<>();

		this.epochTracker = epochTracker;
		this.checkpointForceable = checkpointForceable;

		this.unansweredRPCRequests = new LinkedList<>();
		int maxNumSubpart =
			Arrays.stream(partitions).mapToInt(ResultPartition::getNumberOfSubpartitions).max().orElse(0);
		this.unansweredInFlightLogRequests = HashBasedTable.create(partitions.length, maxNumSubpart);
		this.subpartitionTable = HashBasedTable.create(partitions.length, maxNumSubpart);
		setPartitions(partitions);

	}

	private void setPartitions(ResultPartition[] partitions) {

		for (ResultPartition rp : partitions) {
			IntermediateResultPartitionID partitionID = rp.getPartitionId().getPartitionId();
			ResultSubpartition[] subpartitions = rp.getResultSubpartitions();
			for (int i = 0; i < subpartitions.length; i++)
				this.subpartitionTable.put(partitionID, i, (PipelinedSubpartition) subpartitions[i]);
		}
	}

	@Override
	public void setOwner(RecoveryManager owner){
		this.owner = owner;
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

	@Override
	public short getTaskVertexID() {
		return vertexID;
	}

	@Override
	public EpochTracker getEpochTracker(){
		return this.epochTracker;
	}

	@Override
	public void setInputGate(InputGate inputGate) {
		this.inputGate = inputGate;
	}

	@Override
	public void appendRPCRequestDuringRecovery(AsyncDeterminant determinant){
		this.unansweredRPCRequests.add(determinant);
	}


	@Override
	public int getNumberOfDirectDownstreamNeighbourVertexes(){
		return subpartitionTable.size();
	}
//=======================================================================

}
