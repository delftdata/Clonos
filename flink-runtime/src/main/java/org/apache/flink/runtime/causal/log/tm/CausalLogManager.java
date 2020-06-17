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

package org.apache.flink.runtime.causal.log.tm;


import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.log.job.CausalLogDelta;
import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CausalLogManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLogManager.class);

	private ConcurrentMap<JobID, JobCausalLog> jobIDToManagerMap;
	private ConcurrentMap<InputChannelID, JobCausalLog> inputChannelIDToManagerMap;
	private NetworkBufferPool determinantBufferPool;
	private int numDeterminantBuffersPerTask;

	public CausalLogManager(NetworkBufferPool determinantBufferPool, int numDeterminantBuffersPerTask) {
		this.determinantBufferPool = determinantBufferPool;
		this.numDeterminantBuffersPerTask = numDeterminantBuffersPerTask;
		this.jobIDToManagerMap = new ConcurrentHashMap<>();
		this.inputChannelIDToManagerMap = new ConcurrentHashMap<>();
	}

	public JobCausalLog registerNewJob(JobID jobID, VertexGraphInformation vertexGraphInformation, ResultPartitionWriter[] resultPartitionsOfLocalVertex, Object lock) {
		LOG.debug("Registering a new Job {}.", jobID);
		BufferPool taskDeterminantBufferPool = null;
		try {
			taskDeterminantBufferPool = determinantBufferPool.createBufferPool(numDeterminantBuffersPerTask, numDeterminantBuffersPerTask);
		} catch (IOException e) {
			throw new RuntimeException("Could not register determinant buffer pool!");
		}
		JobCausalLog jobCausalLog = new JobCausalLog(vertexGraphInformation, resultPartitionsOfLocalVertex, taskDeterminantBufferPool, lock);
		jobIDToManagerMap.put(jobID, jobCausalLog);
		return jobCausalLog;
	}

	public void registerNewDownstreamConsumer(JobID jobID, InputChannelID inputChannelID, IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartition) {
		LOG.debug("Registering a new downstream consumer channel {} for job {}.", inputChannelID, jobID);
		JobCausalLog c = jobIDToManagerMap.get(jobID);
		if (c != null)
			c.registerDownstreamConsumer(inputChannelID, intermediateResultPartitionID, consumedSubpartition);
		inputChannelIDToManagerMap.put(inputChannelID, c);
	}

	public CausalLogDelta getNextDeterminantsForDownstream(InputChannelID inputChannelID, long epochID) {
		return new CausalLogDelta(epochID, inputChannelIDToManagerMap.get(inputChannelID).getNextDeterminantsForDownstream(inputChannelID, epochID).toArray(new VertexCausalLogDelta[]{}));
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		inputChannelIDToManagerMap.remove(toCancel).unregisterDownstreamConsumer(toCancel);
	}

	public void processCausalLogDelta(JobID jobID, CausalLogDelta causalLogDelta) {
		long epoch = causalLogDelta.getEpochID();
		IJobCausalLog jobCausalLog = jobIDToManagerMap.get(jobID);
		if(jobCausalLog == null)
			throw new RuntimeException("Unknown Job");
		for(VertexCausalLogDelta vertexCausalLogDelta : causalLogDelta.getVertexCausalLogDeltas())
			jobCausalLog.processUpstreamVertexCausalLogDelta(vertexCausalLogDelta, causalLogDelta.getEpochID());
	}
}
