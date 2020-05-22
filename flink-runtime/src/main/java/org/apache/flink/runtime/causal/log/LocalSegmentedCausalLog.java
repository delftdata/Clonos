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
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

public class LocalSegmentedCausalLog implements LocalCausalLog{

	public LocalSegmentedCausalLog(VertexId vertexId){

	}

	@Override
	public void appendDeterminants(byte[] determinants, long checkpointID) {

	}

	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID) {

	}

	@Override
	public ByteBuf getDeterminants() {
		return null;
	}

	@Override
	public CausalLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long checkpointID) {
		return null;
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}
}
