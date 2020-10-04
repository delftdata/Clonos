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

import org.apache.flink.runtime.causal.log.job.JobCausalLogImpl;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.job.serde.DeltaEncodingStrategy;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JobCausalLogFactory {
	private final int numDeterminantBuffersPerTask;
	private final DeltaEncodingStrategy deltaEncodingStrategy;
	NetworkBufferPool determinantNetworkBufferPool;

	protected static final Logger LOG = LoggerFactory.getLogger(JobCausalLogFactory.class);

	public JobCausalLogFactory(NetworkBufferPool determinantNetworkBufferPool, int numDeterminantBuffersPerTask,
							   DeltaEncodingStrategy deltaEncodingStrategy){
		this.determinantNetworkBufferPool = determinantNetworkBufferPool;
		this.numDeterminantBuffersPerTask = numDeterminantBuffersPerTask;
		this.deltaEncodingStrategy = deltaEncodingStrategy;
	}

	public JobCausalLog buildJobCausalLog(VertexGraphInformation vertexGraphInformation, ResultPartitionWriter[] resultPartitionsOfLocalVertex, int determinantSharingDepth) {
		BufferPool taskDeterminantBufferPool;
		try {
			taskDeterminantBufferPool = determinantNetworkBufferPool.createBufferPool(numDeterminantBuffersPerTask,
				numDeterminantBuffersPerTask);
		} catch (IOException e) {
			throw new RuntimeException("Could not register determinant buffer pool!: \n" + e.getMessage());
		}

		LOG.info("Creating a JobCausalLog for VertexID={} using encoding strategy {} and determinantSharingDepth {}", vertexGraphInformation.getThisTasksVertexID().getVertexId(), deltaEncodingStrategy.name(), determinantSharingDepth);

		return new JobCausalLogImpl(vertexGraphInformation, determinantSharingDepth,
			resultPartitionsOfLocalVertex, taskDeterminantBufferPool, deltaEncodingStrategy);
	}
}
