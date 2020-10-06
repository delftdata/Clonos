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


import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.causal.JobCausalLogFactory;
import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.job.serde.DeltaEncodingStrategy;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link CausalLogManager} manages the CausalLogs of different jobs as {@link JobCausalLog}s.
 * A {@link JobCausalLog} gets its own BufferPool for storing determinants
 * <p>
 * It also tracks the input and output channel IDs and connecting them the the Job causal logs to which they belong.
 */
public class CausalLogManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLogManager.class);

	//Stores the causal logs of each job
	private final ConcurrentMap<JobID, JobCausalLog> jobIDToManagerMap;

	// Maps the IDs of the <b>output</b> (i.e. downstream consumer) channels to the causal log they are consuming from
	private final ConcurrentMap<InputChannelID, JobCausalLog> outputChannelIDToCausalLog;

	// Maps the IDs of the <b>input</b> (i.e. upstream producer) channels to the causal log they are producing to
	private final ConcurrentMap<InputChannelID, JobCausalLog> inputChannelIDToCausalLog;

	private final JobCausalLogFactory jobCausalLogFactory;

	public CausalLogManager(NetworkBufferPool determinantBufferPool, int numDeterminantBuffersPerTask,
							DeltaEncodingStrategy deltaEncodingStrategy) {
		this.jobCausalLogFactory = new JobCausalLogFactory(determinantBufferPool, numDeterminantBuffersPerTask,
			deltaEncodingStrategy);

		this.jobIDToManagerMap = new ConcurrentHashMap<>();
		this.outputChannelIDToCausalLog = new ConcurrentHashMap<>();
		this.inputChannelIDToCausalLog = new ConcurrentHashMap<>();
	}

	public JobCausalLog registerNewTask(JobID jobID, VertexGraphInformation vertexGraphInformation,
										int determinantSharingDepth,
										ResultPartitionWriter[] resultPartitionsOfLocalVertex) {
		JobCausalLog causalLog;
		LOG.info("Registering task {} for JobID {}.", vertexGraphInformation.getThisTasksVertexID(), jobID);
		synchronized (jobIDToManagerMap) {
			if(!jobIDToManagerMap.containsKey(jobID)) {
				jobIDToManagerMap.put(jobID, jobCausalLogFactory.buildJobCausalLog(determinantSharingDepth));
			}
			causalLog = jobIDToManagerMap.get(jobID);
			jobIDToManagerMap.notifyAll();
		}
		LOG.info("Leave sync");

		synchronized (causalLog) {
			causalLog.registerSubtask(vertexGraphInformation, resultPartitionsOfLocalVertex);
		}

		return causalLog;
	}


	public void registerNewUpstreamConnection(InputChannelID inputChannelID, JobID jobID) {

		LOG.info("Registering a new upstream producer channel {} for job {}.", inputChannelID, jobID);
		JobCausalLog c = waitForCausalLogRegistration(jobIDToManagerMap, jobID);

		synchronized (inputChannelIDToCausalLog) {
			inputChannelIDToCausalLog.put(inputChannelID, c);
			inputChannelIDToCausalLog.notifyAll();
		}

	}

	public void registerNewDownstreamConsumer(InputChannelID outputChannelID, JobID jobID,
											  CausalLogID consumedSubpartition) {
		LOG.info("Registering a new downstream consumer channel {} for job {}.", outputChannelID, jobID);

		JobCausalLog c = waitForCausalLogRegistration(jobIDToManagerMap, jobID);

		c.registerDownstreamConsumer(outputChannelID, consumedSubpartition);
		synchronized (outputChannelIDToCausalLog) {
			outputChannelIDToCausalLog.put(outputChannelID, c);
			outputChannelIDToCausalLog.notifyAll();
		}
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		JobCausalLog log = waitForCausalLogRegistration(outputChannelIDToCausalLog, toCancel);
		log.unregisterDownstreamConsumer(toCancel);
	}


	public void unregisterJob(JobID jobID) {
		JobCausalLog jobCausalLog = waitForCausalLogRegistration(jobIDToManagerMap, jobID);

		jobCausalLog.close();
	}

	public ByteBuf enrichWithCausalLogDeltas(ByteBuf serialized, InputChannelID outputChannelID, long epochID) {
		if (LOG.isDebugEnabled())
			LOG.debug("Get next determinants for channel {}", outputChannelID);
		JobCausalLog log = outputChannelIDToCausalLog.get(outputChannelID);
		if (log == null)
			log = waitForCausalLogRegistration(outputChannelIDToCausalLog, outputChannelID);

		serialized = log.enrichWithCausalLogDelta(serialized, outputChannelID, epochID);
		return serialized;
	}

	public void deserializeCausalLogDelta(ByteBuf msg, InputChannelID inputChannelID) {
		JobCausalLog log = inputChannelIDToCausalLog.get(inputChannelID);
		if (log == null)
			log = waitForCausalLogRegistration(inputChannelIDToCausalLog, inputChannelID);
		log.processCausalLogDelta(msg);
	}

	private <K> JobCausalLog waitForCausalLogRegistration(final Map<K, JobCausalLog> map, final K key) {
		JobCausalLog c;
		synchronized (map) {
			c = map.get(key);
			while (c == null) {
				try {
					map.wait(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				c = map.get(key);
			}
		}
		return c;
	}


}
