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

package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.causal.log.thread.SubpartitionThreadLogDelta;
import org.apache.flink.runtime.causal.log.thread.ThreadLogDelta;
import org.apache.flink.runtime.causal.log.vertex.BasicUpstreamVertexCausalLog;
import org.apache.flink.runtime.causal.log.vertex.UpstreamVertexCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class VertexCausalLogTest {

	/**
	 * Scenario: We are tracking the determinants of an upstream with one subpartitions, then it fails
	 */
	@Test
	public void noMemoryLeaksForUpstreamLog(){
		//VertexID vertexID = new VertexID((short) 0);
		//IntermediateResultPartitionID partitionID = new IntermediateResultPartitionID();
		//InputChannelID consumer = new InputChannelID();

		//UpstreamVertexCausalLog downstreamLog = new BasicUpstreamVertexCausalLog(vertexID);


		//ByteBuf mainThreadDelta1Buf = Unpooled.buffer();
		//mainThreadDelta1Buf.writeBytes("Hello world".getBytes());
		//ThreadLogDelta mainThreadDelta1 = new ThreadLogDelta(mainThreadDelta1Buf, 0);

		//ByteBuf subpartitionThreadDelta1Buf = Unpooled.buffer();
		//subpartitionThreadDelta1Buf.writeBytes("Lorem".getBytes());
		//SubpartitionThreadLogDelta subpartitionThreadDelta1 = new SubpartitionThreadLogDelta(subpartitionThreadDelta1Buf,0,0);
		//Map<IntermediateResultPartitionID, Map<Integer, SubpartitionThreadLogDelta>> partitionToIndexToDeltaMap =
		//	Collections.singletonMap(partitionID, Collections.singletonMap(0,subpartitionThreadDelta1));

		//VertexCausalLogDelta delta1 = new VertexCausalLogDelta(vertexID, mainThreadDelta1, partitionToIndexToDeltaMap);

		//downstreamLog.processUpstreamCausalLogDelta(delta1, 0);

		//downstreamLog.registerDownstreamConsumer(consumer, partitionID, 0);

		//VertexCausalLogDelta consumedDelta = downstreamLog.getNextDeterminantsForDownstream(consumer, 0);
		//consumedDelta.getMainThreadDelta().getRawDeterminants().release();
		//consumedDelta.getMainThreadDelta().getRawDeterminants().release();
		//System.out.println(consumedDelta);


		//downstreamLog.notifyCheckpointComplete(1);
		//assert (mainThreadDelta1Buf.refCnt() == 0 && subpartitionThreadDelta1Buf.refCnt() == 0);

	}


}
