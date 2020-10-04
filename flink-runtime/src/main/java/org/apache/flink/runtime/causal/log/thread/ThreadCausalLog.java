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

package org.apache.flink.runtime.causal.log.thread;

import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

public interface ThreadCausalLog {

	CausalLogID getCausalLogID();
	/**
	 * Get all determinants in this log from start to end. Does not advance any internal offsets.
	 * @return a byte[] containing all determinants in sequence
	 * @param startEpochID
	 */
	ByteBuf getDeterminants(long startEpochID);

	/**
	 * This is only used for testing and runtime assertions that recovery went correctly.
	 * Obtains the length in bytes of the log
	 */
	int logLength();


	/**
	 * Processes and appends a delta to the log, deduplicating determinants if needed
	 * @param delta the delta itself
	 * @param offsetFromEpoch how far from the start of the epoch epochID this delta starts
	 * @param epochID the epoch to which the delta belongs.
	 */
	void processUpstreamDelta(ByteBuf delta, int offsetFromEpoch, long epochID);

	/**
	 * Serializes and appends a determinant to the determinant log, making it available for downstream.
	 * @param determinant the determinant to be appended
	 * @param epochID the current epoch the producer is in.
	 */
	void appendDeterminant(Determinant determinant, long epochID);

	/**
	 * Checks whether this log has an update for the provided output channel in the provided epoch
	 */
	boolean hasDeltaForConsumer(InputChannelID outputChannelID, long epochID);

	/**
	 * Computes how far in bytes from the start of epoch epochID the consumer is.
	 */
	int getOffsetFromEpochForConsumer(InputChannelID outputChannelID, long epochID);

	/**
	 * Returns the ByteBuf delta for a consumer. Before requesting one, one should use {@link ThreadCausalLog#hasDeltaForConsumer},
	 * otherwise results are undefined.
	 * @param outputChannelID the consumer requesting a delta update
	 * @param epochID the epoch for which the update is requested. i.e. the epoch at which the consumer is.
	 * @return a ByteBuf containing the delta
	 */
	ByteBuf getDeltaForConsumer(InputChannelID outputChannelID, long epochID);

	/**
	 * Notifies the log of a checkpoint completion, allowing log truncation of old epochs
	 * @param checkpointID
	 */
	void notifyCheckpointComplete(long checkpointID);

	/**
	 * Close the log releasing all held resources
	 */
	void close();
}
