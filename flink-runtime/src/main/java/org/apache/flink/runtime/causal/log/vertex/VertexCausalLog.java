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

package org.apache.flink.runtime.causal.log.vertex;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

public interface VertexCausalLog {

	/**
	 * Registers a consumer of the determinant log.
	 * Offsets will start to be tracked from the current earliest epoch
	 * @param inputChannelID the id of the downstream consumer channel.
	 */
	void registerDownstreamConsumer(InputChannelID inputChannelID, IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionID);

	void unregisterDownstreamConsumer(InputChannelID toCancel);

	/**
	 * Get all determinants in this log from start to end. Does not advance any internal offsets.
	 * @return a byte[] containing all determinants in sequence
	 * @param startEpochID
	 */
	VertexCausalLogDelta getDeterminants(long startEpochID);

	/**
	 * Calculates the next update to send downstream. Advances internal counters as well.
	 * @param consumer the channel to get the next update for.
	 * @return a delta containing the update to send downstream
	 */
	VertexCausalLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long checkpointID);

	void notifyCheckpointComplete(long checkpointID);

    int mainThreadLogLength();

	int subpartitionLogLength(IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex);
}
