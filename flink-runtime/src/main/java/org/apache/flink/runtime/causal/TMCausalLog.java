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


import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TMCausalLog {

	private static final Logger LOG = LoggerFactory.getLogger(TMCausalLog.class);

	private ConcurrentMap<JobID, JobCausalLoggingManager> jobIDToManagerMap;
	private ConcurrentMap<InputChannelID, JobCausalLoggingManager> inputChannelIDToManagerMap;

	public TMCausalLog() {
		this.jobIDToManagerMap = new ConcurrentHashMap<>();
		this.inputChannelIDToManagerMap = new ConcurrentHashMap<>();
	}

	public void registerNewJob(JobID jobID, VertexGraphInformation vertexGraphInformation) {
		LOG.info("Registering a new Job {}.", jobID);
		JobCausalLoggingManager newManager = new JobCausalLoggingManager(vertexGraphInformation);
		jobIDToManagerMap.put(jobID, newManager);
	}

	public JobCausalLoggingManager getCausalLoggingManagerOfJob(JobID jobID) {
		return jobIDToManagerMap.get(jobID);
	}

	public void registerNewDownstreamConsumer(JobID jobID, InputChannelID inputChannelID) {
		LOG.info("Registering a new downstream consumer channel {} for job {}.", inputChannelID, jobID);
		JobCausalLoggingManager c = jobIDToManagerMap.get(jobID);
		if (c != null)
			c.registerDownstreamConsumer(inputChannelID);
		inputChannelIDToManagerMap.put(inputChannelID, c);
	}

	public List<CausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID) {
		return inputChannelIDToManagerMap.get(inputChannelID).getNextDeterminantsForDownstream(inputChannelID);
	}

	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		inputChannelIDToManagerMap.remove(toCancel).unregisterDownstreamConsumer(toCancel);
	}

	public void processVertexCausalLogDelta(JobID jobID, List<CausalLogDelta> deltaList) {
		for (CausalLogDelta d : deltaList)
			this.jobIDToManagerMap.get(jobID).processCausalLogDelta(d);
	}
}
