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

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Manages the {@link org.apache.flink.runtime.causal.log.thread.ThreadCausalLog}s of a specific job.
 * The most important functionalities offered are appending determinants to the local logs,
 * enriching a Buffer going downstream with determinant deltas, and deserializing said deltas.
 *
 * After a failure of an upstream vertex V, one can also request all determinants of vertex V.
 */
public interface JobCausalLog {

	void registerSubtask(VertexGraphInformation vertexGraphInformation, ResultPartitionWriter[] resultPartitionsOfLocalVertex);

	ThreadCausalLog getThreadCausalLog(CausalLogID causalLogID);

	void processCausalLogDelta(ByteBuf msg);

	ByteBuf enrichWithCausalLogDelta(ByteBuf serialized, InputChannelID inputChannelID, long epochID);

	DeterminantResponseEvent respondToDeterminantRequest(VertexID vertexId, long startEpochID);

	void registerDownstreamConsumer(InputChannelID inputChannelID, CausalLogID consumedSubpartition);

	void unregisterDownstreamConsumer(InputChannelID toCancel);


	DeterminantEncoder getDeterminantEncoder();

	void notifyCheckpointComplete(long checkpointID);

	void close();

	//================ Safety check metrics==================================================
	int threadLogLength(CausalLogID causalLogID);

}
