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
import org.apache.flink.shaded.netty4.io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link CausalLogManager} manages the CausalLogs of different jobs as {@link JobCausalLog}s.
 * A {@link JobCausalLog} gets its own BufferPool for storing determinants
 */
public class CausalLogManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLogManager.class);

	//Stores the causal logs of each job
	private ConcurrentMap<JobID, JobCausalLog> jobIDToManagerMap;
	private ConcurrentMap<JobID, ConcurrentSet<ConsumerRegistrationRequest>> jobPreregistrationMap;

	// Maps the IDs of the <b>output</b> (i.e. downstream consumer) channels to the causal log they are consuming from
	private ConcurrentMap<InputChannelID, JobCausalLog> inputChannelIDToManagerMap;

	// The {@link NetworkBufferPool} from which new {@link BufferPool} instances are made
	private NetworkBufferPool determinantBufferPool;

	// Configuration parameter on how many buffers each job should get for determinant storage
	private int numDeterminantBuffersPerTask;

	private static final Object registrationLock = new Object();

	public CausalLogManager(NetworkBufferPool determinantBufferPool, int numDeterminantBuffersPerTask) {
		this.determinantBufferPool = determinantBufferPool;
		this.numDeterminantBuffersPerTask = numDeterminantBuffersPerTask;
		this.jobIDToManagerMap = new ConcurrentHashMap<>();
		this.inputChannelIDToManagerMap = new ConcurrentHashMap<>();
		this.jobPreregistrationMap = new ConcurrentHashMap<>();
	}

	public JobCausalLog registerNewJob(JobID jobID, VertexGraphInformation vertexGraphInformation,
									   ResultPartitionWriter[] resultPartitionsOfLocalVertex, Object lock) {
		LOG.debug("Registering a new Job {}.", jobID);
		synchronized (registrationLock) {
			BufferPool taskDeterminantBufferPool = null;
			try {
				taskDeterminantBufferPool = determinantBufferPool.createBufferPool(numDeterminantBuffersPerTask,
					numDeterminantBuffersPerTask);
			} catch (IOException e) {
				throw new RuntimeException("Could not register determinant buffer pool!: \n" + e.getMessage());
			}
			JobCausalLog jobCausalLog = new JobCausalLog(vertexGraphInformation, resultPartitionsOfLocalVertex,
				taskDeterminantBufferPool, lock);
			jobIDToManagerMap.put(jobID, jobCausalLog);
			if(jobPreregistrationMap.containsKey(jobID)) {
				for (ConsumerRegistrationRequest req : jobPreregistrationMap.get(jobID))
					jobCausalLog.registerDownstreamConsumer(req.inputChannelID, req.intermediateResultPartitionID,
						req.consumedPartition);

				jobPreregistrationMap.get(jobID).clear();
			}

		return jobCausalLog;
		}
	}

	public void registerNewDownstreamConsumer(JobID jobID, InputChannelID inputChannelID,
											  IntermediateResultPartitionID intermediateResultPartitionID,
											  int consumedSubpartition) {
		LOG.debug("Registering a new downstream consumer channel {} for job {}.", inputChannelID, jobID);
		synchronized (registrationLock) {
			JobCausalLog c = jobIDToManagerMap.get(jobID);
			if (c == null)
				jobPreregistrationMap.computeIfAbsent(jobID, k -> new ConcurrentSet<>()).add(new ConsumerRegistrationRequest(inputChannelID, intermediateResultPartitionID, consumedSubpartition));
			else {
				c.registerDownstreamConsumer(inputChannelID, intermediateResultPartitionID, consumedSubpartition);
				inputChannelIDToManagerMap.put(inputChannelID, c);
			}
		}
	}

	public CausalLogDelta getNextDeterminantsForDownstream(InputChannelID inputChannelID, long epochID) {
		return new CausalLogDelta(epochID,
			inputChannelIDToManagerMap.get(inputChannelID).getNextDeterminantsForDownstream(inputChannelID, epochID).toArray(new VertexCausalLogDelta[]{}));
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		synchronized (registrationLock) {
			inputChannelIDToManagerMap.remove(toCancel).unregisterDownstreamConsumer(toCancel);
		}
	}

	public void processCausalLogDelta(JobID jobID, CausalLogDelta causalLogDelta) {
		IJobCausalLog jobCausalLog = jobIDToManagerMap.get(jobID);
		if (jobCausalLog == null)
			throw new RuntimeException("Unknown Job");
		for (VertexCausalLogDelta vertexCausalLogDelta : causalLogDelta.getVertexCausalLogDeltas())
			jobCausalLog.processUpstreamVertexCausalLogDelta(vertexCausalLogDelta, causalLogDelta.getEpochID());
	}

	private static class ConsumerRegistrationRequest {
		private final InputChannelID inputChannelID;
		private final IntermediateResultPartitionID intermediateResultPartitionID;
		private final int consumedPartition;

		public ConsumerRegistrationRequest(InputChannelID inputChannelID,
										   IntermediateResultPartitionID intermediateResultPartitionID,
										   int consumedPartition) {
			this.inputChannelID = inputChannelID;
			this.intermediateResultPartitionID = intermediateResultPartitionID;
			this.consumedPartition = consumedPartition;
		}

		public InputChannelID getInputChannelID() {
			return inputChannelID;
		}

		public IntermediateResultPartitionID getIntermediateResultPartitionID() {
			return intermediateResultPartitionID;
		}

		public int getConsumedPartition() {
			return consumedPartition;
		}
	}
}
