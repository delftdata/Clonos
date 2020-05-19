/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.state.CheckpointListener;

/**
 * Used to log a single upstream (or the current task's) task's determinants.
 * Is responsible for garbage collection of determinants which have been checkpointed or sent to all downstream tasks.
 * It is responsible for remembering what determinants it has sent to which downstream tasks.
 */
public interface CausalLog extends CheckpointListener {

	/**
	 * Registers a consumer of the determinant log.
	 * Offsets will start to be tracked from the current earliest epoch
	 * @param inputChannelID the id of the downstream consumer channel.
	 */
	void registerDownstreamConsumer(InputChannelID inputChannelID);

	/**
	 * Get all determinants in this log from start to end. Does not advance any internal offsets.
	 * @return a byte[] containing all determinants in sequence
	 */
	byte[] getDeterminants();


	/**
	 * Appends the provided determinants to the  log.
	 * @param determinants to append
	 */
	void appendDeterminants(byte[] determinants);

	/**
	 * Process a {@link CausalLogDelta}.
	 * This involves using the provided offset to determine if any new determinants are present and appending only those.
	 * @param causalLogDelta
	 */
	void processUpstreamVertexCausalLogDelta(CausalLogDelta causalLogDelta);


	/**
	 * Calculates the next update to send downstream. Advances internal counters as well.
	 * @param consumer the channel to get the next update for.
	 * @return a {@link CausalLogDelta} containing the update to send downstream
	 */
	CausalLogDelta getNextDeterminantsForDownstream(InputChannelID consumer);

	/**
	 * Notifies the log of a checkpoint. This starts a new epoch.
	 * @param checkpointId
	 */
	void notifyCheckpointBarrier(long checkpointId);

    void unregisterDownstreamConsumer(InputChannelID toCancel);

}
