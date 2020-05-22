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

import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.VertexId;
import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class JobCausalLoggingManager implements IJobCausalLoggingManager {

	private static final Logger LOG = LoggerFactory.getLogger(JobCausalLoggingManager.class);

	VertexId myVertexId;
	long currentCheckpoint;
	Map<VertexId, UpstreamCausalLog> determinantLogs;
	LocalCausalLog myCausalLog;
	DeterminantEncodingStrategy determinantEncodingStrategy;

	public JobCausalLoggingManager(VertexGraphInformation vertexGraphInformation) {
		this(vertexGraphInformation.getThisTasksVertexId(), vertexGraphInformation.getUpstreamVertexes());
	}

	public JobCausalLoggingManager(VertexId myVertexId, Collection<VertexId> upstreamVertexIds) {
		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} ", myVertexId, String.join(", ", upstreamVertexIds.stream().map(Object::toString).collect(Collectors.toList())));
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;
		this.currentCheckpoint = 0L;
		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new ReplicatedCausalLog(u));

		myCausalLog = new LocalSegmentedCausalLog(myVertexId);
		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();
	}


	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID) {
		for(CausalLog circularVertexCausalLog : determinantLogs.values())
			circularVertexCausalLog.registerDownstreamConsumer(inputChannelID);
	}

	@Override
	public List<CausalLogDelta> getDeterminants() {
		List<CausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new CausalLogDelta(key, determinantLogs.get(key).getDeterminants(), 0));
		}
		results.add(new CausalLogDelta(this.myVertexId, determinantLogs.get(this.myVertexId).getDeterminants(), 0));
		return results;
	}


	@Override
	public void appendDeterminant(Determinant determinant, long checkpointID) {
		LOG.info("Appending determinant {}", determinant);
		myCausalLog.appendDeterminants(
			this.determinantEncodingStrategy.encode(determinant),
			checkpointID
		);
	}

	@Override
	public void appendDeterminant(Determinant determinant) {
		appendDeterminant(determinant, currentCheckpoint);
	}

	@Override
	public void updateCheckpointID(long checkpointID) {
		this.currentCheckpoint = checkpointID;
	}

	@Override
	public void processCausalLogDelta(CausalLogDelta d, long checkpointID) {
		LOG.info("Processing UpstreamCausalLogDelta {}", d);
		LOG.info("Determinant log pre processing: {}", this.determinantLogs.get(d.vertexId));
		this.determinantLogs.get(d.vertexId).processUpstreamVertexCausalLogDelta(d, checkpointID);
		LOG.info("Determinant log post processing: {}", this.determinantLogs.get(d.vertexId));
	}

	@Override
	public DeterminantEncodingStrategy getDeterminantEncodingStrategy() {
		return determinantEncodingStrategy;
	}

	@Override
	public VertexId getVertexId() {
		return myVertexId;
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		for(CausalLog vcl : determinantLogs.values())
			vcl.unregisterDownstreamConsumer(toCancel);
	}

	@Override
	public ByteBuf getDeterminantsOfVertex(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}

	@Override
	public List<CausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID, long checkpointID){
		LOG.info("Getting deltas to send to downstream channel {}", inputChannelID);
		List<CausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			LOG.info("Determinant log pre processing: {}", determinantLogs.get(key));
			CausalLogDelta causalLogDelta = determinantLogs.get(key).getNextDeterminantsForDownstream(inputChannelID, checkpointID);
			if (causalLogDelta.rawDeterminants.readableBytes() != 0)
				results.add(causalLogDelta);
			LOG.info("Determinant log post processing: {}", determinantLogs.get(key));
		}
		return results;
	}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("Processing checkpoint complete notification for id {}", checkpointId);
		for (CausalLog log : determinantLogs.values()) {
			LOG.info("Determinant log pre processing: {}", log);
			log.notifyCheckpointComplete(checkpointId);
			LOG.info("Determinant log post processing: {}", log);
		}
	}

}
