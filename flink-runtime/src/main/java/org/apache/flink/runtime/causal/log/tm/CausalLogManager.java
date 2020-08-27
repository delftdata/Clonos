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
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link CausalLogManager} manages the CausalLogs of different jobs as {@link JobCausalLog}s.
 * A {@link JobCausalLog} gets its own BufferPool for storing determinants
 */
public class CausalLogManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLogManager.class);

	//Stores the causal logs of each job
	private final Map<JobID, JobCausalLog> jobIDToManagerMap;

	// Maps the IDs of the <b>output</b> (i.e. downstream consumer) channels to the causal log they are consuming from
	private final Map<InputChannelID, JobCausalLog> inputChannelIDToManagerMap;

	private final ConcurrentHashMap<InputChannelID, Boolean> removedConsumers;

	// The {@link NetworkBufferPool} from which new {@link BufferPool} instances are made
	private NetworkBufferPool determinantBufferPool;

	// Configuration parameter on how many buffers each job should get for determinant storage
	private int numDeterminantBuffersPerTask;


	public CausalLogManager(NetworkBufferPool determinantBufferPool, int numDeterminantBuffersPerTask) {
		this.determinantBufferPool = determinantBufferPool;
		this.numDeterminantBuffersPerTask = numDeterminantBuffersPerTask;
		this.jobIDToManagerMap = new HashMap<>();
		this.inputChannelIDToManagerMap = new HashMap<>();
		this.removedConsumers = new ConcurrentHashMap<>(50);
	}

	public JobCausalLog registerNewJob(JobID jobID, VertexGraphInformation vertexGraphInformation,
									   int determinantSharingDepth,
									   ResultPartitionWriter[] resultPartitionsOfLocalVertex, Object lock) {
		LOG.info("Registering a new Job {}.", jobID);

		BufferPool taskDeterminantBufferPool = null;
		try {
			taskDeterminantBufferPool = determinantBufferPool.createBufferPool(numDeterminantBuffersPerTask,
				numDeterminantBuffersPerTask);
		} catch (IOException e) {
			throw new RuntimeException("Could not register determinant buffer pool!: \n" + e.getMessage());
		}

		JobCausalLog jobCausalLog = new JobCausalLog(vertexGraphInformation, determinantSharingDepth,
			resultPartitionsOfLocalVertex,
			taskDeterminantBufferPool, lock);

		synchronized (jobIDToManagerMap) {
			jobIDToManagerMap.put(jobID, jobCausalLog);
			jobIDToManagerMap.notifyAll();
		}

		return jobCausalLog;
	}

	public void registerNewDownstreamConsumer(JobID jobID, InputChannelID inputChannelID,
											  IntermediateResultPartitionID intermediateResultPartitionID,
											  int consumedSubpartition) {
		LOG.info("Registering a new downstream consumer channel {} for job {}.", inputChannelID, jobID);

		JobCausalLog c;
		synchronized (jobIDToManagerMap) {
			c = jobIDToManagerMap.get(jobID);
			while (c == null) {
				waitUninterruptedly(jobIDToManagerMap);
				c = jobIDToManagerMap.get(jobID);
			}
		}

		c.registerDownstreamConsumer(inputChannelID, intermediateResultPartitionID, consumedSubpartition);
		synchronized (inputChannelIDToManagerMap) {
			inputChannelIDToManagerMap.put(inputChannelID, c);
			inputChannelIDToManagerMap.notifyAll();
		}
	}

	public CausalLogDelta getNextDeterminantsForDownstream(InputChannelID inputChannelID, long epochID) {
		JobCausalLog log;
		LOG.debug("Get next determinants for channel {} epoch {}", inputChannelID, epochID);
		synchronized (inputChannelIDToManagerMap) {
			log = inputChannelIDToManagerMap.get(inputChannelID);
			while (log == null) {
				if (removedConsumers.containsKey(inputChannelID))
					return new CausalLogDelta(epochID, new VertexCausalLogDelta[0]);
				waitUninterruptedly(inputChannelIDToManagerMap);
				log = inputChannelIDToManagerMap.get(inputChannelID);
			}
		}

		return new CausalLogDelta(epochID,
			log.getNextDeterminantsForDownstream(inputChannelID, epochID).toArray(new VertexCausalLogDelta[]{}));
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		JobCausalLog log;
		synchronized (inputChannelIDToManagerMap) {
			removedConsumers.put(toCancel, Boolean.TRUE);
			log = inputChannelIDToManagerMap.remove(toCancel);
			while (log == null) {
				waitUninterruptedly(inputChannelIDToManagerMap);
				log = inputChannelIDToManagerMap.remove(toCancel);
			}
		}
		log.unregisterDownstreamConsumer(toCancel);
	}

	public void processCausalLogDelta(JobID jobID, CausalLogDelta causalLogDelta) {
		JobCausalLog jobCausalLog;
		if(LOG.isDebugEnabled())
			LOG.debug("Process delta for jobid {} from id {}", jobID, causalLogDelta);
		synchronized (jobIDToManagerMap) {
			jobCausalLog = jobIDToManagerMap.get(jobID);
			while (jobCausalLog == null) {
				waitUninterruptedly(jobIDToManagerMap);
				jobCausalLog = jobIDToManagerMap.get(jobID);
			}
		}

		for (VertexCausalLogDelta vertexCausalLogDelta : causalLogDelta.getVertexCausalLogDeltas())
			jobCausalLog.processUpstreamVertexCausalLogDelta(vertexCausalLogDelta, causalLogDelta.getEpochID());
	}

	private static void waitUninterruptedly(Object o) {
		try {
			o.wait(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void unregisterJob(JobID jobID) {
		JobCausalLog jobCausalLog;
		synchronized (jobIDToManagerMap) {
			jobCausalLog = jobIDToManagerMap.remove(jobID);
			while (jobCausalLog == null) {
				waitUninterruptedly(jobIDToManagerMap);
				jobCausalLog = jobIDToManagerMap.get(jobID);
			}
		}

		jobCausalLog.close();


	}
}
