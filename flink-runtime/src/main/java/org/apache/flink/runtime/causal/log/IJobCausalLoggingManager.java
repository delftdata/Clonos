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
package org.apache.flink.runtime.causal.log;

import org.apache.flink.runtime.causal.VertexId;
import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * A CausalLog contains the determinant logs of all upstream operators and itself.
 */
public interface IJobCausalLoggingManager extends CheckpointListener {

	void registerDownstreamConsumer(InputChannelID inputChannelID);

	List<CausalLogDelta> getDeterminants();

	/*
	Encodes and appends to this tasks log
	 */
	void appendDeterminant(Determinant determinant, long checkpointID);

	/**
	 * Note: Should ONLY be used by Main thread determinants
	 * @param determinant
	 */
	void appendDeterminant(Determinant determinant);

	void updateCheckpointID(long checkpointID);

	void processCausalLogDelta(CausalLogDelta d, long checkpointID);

	ByteBuf getDeterminantsOfVertex(VertexId vertexId);

	List<CausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID, long checkpointID);

	DeterminantEncodingStrategy getDeterminantEncodingStrategy();

	VertexId getVertexId();

    void unregisterDownstreamConsumer(InputChannelID toCancel);

}
