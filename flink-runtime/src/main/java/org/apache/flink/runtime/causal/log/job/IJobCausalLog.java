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
package org.apache.flink.runtime.causal.log.job;

import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.List;

/**
 * A JobCausalLog contains the determinant logs of all upstream vertexes and itself.
 * It processes deltas from upstream and sends deltas downstream
 */
public interface IJobCausalLog extends CheckpointListener {


	void registerDownstreamConsumer(InputChannelID inputChannelID, IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartition);

	void unregisterDownstreamConsumer(InputChannelID toCancel);

	/*
	Encodes and appends to this tasks log
	 */
	void appendDeterminant(Determinant determinant, long epochID);

	void appendSubpartitionDeterminant(Determinant determinant, long epochID, IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex);

	void processUpstreamVertexCausalLogDelta(VertexCausalLogDelta d, long epochID);

	VertexCausalLogDelta getDeterminantsOfVertex(VertexID vertexId, long startEpochID);

	List<VertexCausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID, long checkpointID);

	/**
	 * The encoding strategy used to encode determinants.
	 */
	DeterminantEncoder getDeterminantEncoder();


	//================ Safety check metrics==================================================
	// These methods are used to ensure that after recovery, log length is the exact size it should be
    int mainThreadLogLength();
    int subpartitionLogLength(IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex);
}
