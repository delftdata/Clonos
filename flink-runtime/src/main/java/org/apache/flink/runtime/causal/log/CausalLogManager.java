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
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.job.serde.DeltaEncodingStrategy;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link CausalLogManager} manages the CausalLogs of different jobs as {@link JobCausalLog}s.
 * A {@link JobCausalLog} gets its own BufferPool for storing determinants
 */
public class CausalLogManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLogManager.class);

	//Stores the causal logs of each job
	private final Map<JobID, JobCausalLog> jobIDToManagerMap;

	// Maps the IDs of the <b>output</b> (i.e. downstream consumer) channels to the causal log they are consuming from
	private final Map<InputChannelID, JobCausalLog> outputChannelIDToCausalLog;

	// Maps the IDs of the <b>input</b> (i.e. upstream producer) channels to the causal log they are producing to
	private final Map<InputChannelID, JobCausalLog> inputChannelIDToCausalLog;

	private final ConcurrentHashMap<InputChannelID, Boolean> removedOutputChannels;
	private final JobCausalLogFactory jobCausalLogFactory;


	//TODO would be awesome to remove all this synchronization
	public CausalLogManager(NetworkBufferPool determinantBufferPool, int numDeterminantBuffersPerTask,
							DeltaEncodingStrategy deltaEncodingStrategy) {
		this.jobCausalLogFactory = new JobCausalLogFactory(determinantBufferPool, numDeterminantBuffersPerTask, deltaEncodingStrategy);

		this.jobIDToManagerMap = new HashMap<>();
		this.outputChannelIDToCausalLog = new HashMap<>();
		this.inputChannelIDToCausalLog = new HashMap<>();
		this.removedOutputChannels = new ConcurrentHashMap<>(50);
	}

	public JobCausalLog registerNewJob(JobID jobID, VertexGraphInformation vertexGraphInformation,
									   int determinantSharingDepth,
									   ResultPartitionWriter[] resultPartitionsOfLocalVertex) {
		LOG.info("Registering a new Job {}.", jobID);

		JobCausalLog causalLog = jobCausalLogFactory.buildJobCausalLog(vertexGraphInformation, resultPartitionsOfLocalVertex, determinantSharingDepth);

		synchronized (jobIDToManagerMap) {
			jobIDToManagerMap.put(jobID, causalLog);
			jobIDToManagerMap.notifyAll();
		}

		return causalLog;
	}


	public void registerNewUpstreamConnection(InputChannelID inputChannelID, JobID jobID) {

		LOG.info("Registering a new upstream producer channel {} for job {}.", inputChannelID, jobID);
		JobCausalLog c = getJobCausalLog(jobID);

		synchronized (inputChannelIDToCausalLog) {
			inputChannelIDToCausalLog.put(inputChannelID, c);
			inputChannelIDToCausalLog.notifyAll();
		}

	}

	public void registerNewDownstreamConsumer(InputChannelID outputChannelID, JobID jobID,
											  IntermediateResultPartitionID intermediateResultPartitionID,
											  int consumedSubpartition) {
		LOG.info("Registering a new downstream consumer channel {} for job {}.", outputChannelID, jobID);

		JobCausalLog c = getJobCausalLog(jobID);

		c.registerDownstreamConsumer(outputChannelID, intermediateResultPartitionID, consumedSubpartition);
		synchronized (outputChannelIDToCausalLog) {
			outputChannelIDToCausalLog.put(outputChannelID, c);
			outputChannelIDToCausalLog.notifyAll();
		}
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		JobCausalLog log;
		synchronized (outputChannelIDToCausalLog) {
			removedOutputChannels.put(toCancel, Boolean.TRUE);
			log = outputChannelIDToCausalLog.remove(toCancel);
			while (log == null) {
				waitUninterruptedly(outputChannelIDToCausalLog);
				log = outputChannelIDToCausalLog.remove(toCancel);
			}
		}
		log.unregisterDownstreamConsumer(toCancel);
	}


	public void unregisterJob(JobID jobID) {
		JobCausalLog jobCausalLog = getJobCausalLog(jobID);

		jobCausalLog.close();


	}

	public ByteBuf enrichWithCausalLogDeltas(ByteBuf serialized, InputChannelID outputChannelID, long epochID) {
		JobCausalLog log;
		if(LOG.isDebugEnabled())
			LOG.debug("Get next determinants for channel {}", outputChannelID);
		synchronized (outputChannelIDToCausalLog) {
			log = outputChannelIDToCausalLog.get(outputChannelID);
			while (log == null) {
				if (removedOutputChannels.containsKey(outputChannelID))
					return serialized;
				waitUninterruptedly(outputChannelIDToCausalLog);
				log = outputChannelIDToCausalLog.get(outputChannelID);
			}
		}

		serialized = log.enrichWithCausalLogDelta(serialized, outputChannelID, epochID);
		return serialized;
	}

	public void deserializeCausalLogDelta(ByteBuf msg, InputChannelID inputChannelID) {
		JobCausalLog jobCausalLog;
		synchronized (inputChannelIDToCausalLog) {
			jobCausalLog = inputChannelIDToCausalLog.get(inputChannelID);
			while (jobCausalLog == null) {
				waitUninterruptedly(inputChannelIDToCausalLog);
				jobCausalLog = inputChannelIDToCausalLog.get(inputChannelID);
			}
		}

		jobCausalLog.processCausalLogDelta(msg);
	}

	private JobCausalLog getJobCausalLog(JobID jobID) {
		JobCausalLog c;
		synchronized (jobIDToManagerMap) {
			c = jobIDToManagerMap.get(jobID);
			while (c == null) {
				waitUninterruptedly(jobIDToManagerMap);
				c = jobIDToManagerMap.get(jobID);
			}
		}
		return c;
	}

	private static void waitUninterruptedly(Object o) {
		try {
			o.wait(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
