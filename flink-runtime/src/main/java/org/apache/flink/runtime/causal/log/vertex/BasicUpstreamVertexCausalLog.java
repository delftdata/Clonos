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

package org.apache.flink.runtime.causal.log.vertex;

import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.log.thread.*;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class BasicUpstreamVertexCausalLog implements UpstreamVertexCausalLog {

	private static final Logger LOG = LoggerFactory.getLogger(BasicUpstreamVertexCausalLog.class);

	VertexID vertexId;

	//MPMC
	UpstreamThreadCausalLog mainThreadLog;

	//MPMC
	ConcurrentMap<IntermediateResultPartitionID, ConcurrentMap<Integer, UpstreamThreadCausalLog>> subpartitionLogs;

	BufferPool bufferPool;

	public BasicUpstreamVertexCausalLog(VertexID vertexId, BufferPool bufferPool){
		LOG.info("Creating new UpstreamVertexCausalLog for vertexID {}", vertexId);
		this.vertexId = vertexId;
		this.bufferPool = bufferPool;
		mainThreadLog = new NetworkBufferBasedContiguousUpstreamThreadCausalLog(bufferPool);
		subpartitionLogs = new ConcurrentHashMap<>(5);
	}


	@Override
	public void processUpstreamCausalLogDelta(VertexCausalLogDelta causalLogDelta, long checkpointID) {
		if(causalLogDelta.mainThreadDelta != null)
			mainThreadLog.processUpstreamVertexCausalLogDelta(causalLogDelta.getMainThreadDelta(), checkpointID);

		for(Map.Entry<IntermediateResultPartitionID, SortedMap<Integer,SubpartitionThreadLogDelta>> entry : causalLogDelta.partitionDeltas.entrySet()){

			ConcurrentMap<Integer, UpstreamThreadCausalLog> idsLogs =
				subpartitionLogs.computeIfAbsent(entry.getKey(), k ->
					new ConcurrentHashMap<>(10));

			for(SubpartitionThreadLogDelta logDelta : entry.getValue().values()) {
				UpstreamThreadCausalLog threadLog = idsLogs.computeIfAbsent(logDelta.getSubpartitionIndex(),  k -> new NetworkBufferBasedContiguousUpstreamThreadCausalLog(bufferPool));
				threadLog.processUpstreamVertexCausalLogDelta(logDelta, checkpointID);
			}
		}
	}

	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID, IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionID) {
		LOG.info("Registering downstream consumer at upstream vertex level");
		//ignore the consumed intermediateDataSet and subpartition. This is an upstream log, so consumer depends on all logs
		//mainThreadLog.registerDownstreamConsumer(inputChannelID);
		//for(UpstreamThreadCausalLog log : subpartitionLogs.values().stream().flatMap( map -> map.values().stream()).collect(Collectors
		//	.toList()))
		//	log.registerDownstreamConsumer(inputChannelID);

	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {

		//mainThreadLog.unregisterDownstreamConsumer(toCancel);
		//for(UpstreamThreadCausalLog log : subpartitionLogs.values().stream().flatMap( map -> map.values().stream()).collect(Collectors
		//	.toList()))
		//	log.registerDownstreamConsumer(toCancel);
	}

	@Override
	public VertexCausalLogDelta getDeterminants() {
		LOG.info("Building vertexCausalLogDelta for vertexID {}", vertexId);
		ByteBuf mainThreadBuf = mainThreadLog.getDeterminants();
		LOG.info("mainThreadBuf {}", mainThreadBuf);

		Map<IntermediateResultPartitionID, Map<Integer,SubpartitionThreadLogDelta>> subpartitionDeltas = new HashMap<>(subpartitionLogs.size());

		for(Map.Entry<IntermediateResultPartitionID, ConcurrentMap<Integer,UpstreamThreadCausalLog>> datasetEntry : subpartitionLogs.entrySet()){
			List<SubpartitionThreadLogDelta> deltasWithData = new ArrayList<>(datasetEntry.getValue().size());
			for(Map.Entry<Integer, UpstreamThreadCausalLog> subpartitionEntry : datasetEntry.getValue().entrySet()){
				ByteBuf buf = subpartitionEntry.getValue().getDeterminants();
				LOG.info("DATASETID {}, INDEX {}, Buf {}", datasetEntry.getKey(), subpartitionEntry.getKey(), buf);
				if(buf.capacity() > 0)
					deltasWithData.add(new SubpartitionThreadLogDelta(buf, 0, subpartitionEntry.getKey()));
			}
			if(!deltasWithData.isEmpty()){
				Map<Integer, SubpartitionThreadLogDelta> internalMap = new HashMap<>(deltasWithData.size());
				deltasWithData.forEach(d->internalMap.put(d.getSubpartitionIndex(),d));
				subpartitionDeltas.put(datasetEntry.getKey(),internalMap);
			}

		}

		return new VertexCausalLogDelta(vertexId, (mainThreadBuf.capacity() > 0 ? new ThreadLogDelta(mainThreadBuf,0) : null), subpartitionDeltas);
	}

	@Override
	public VertexCausalLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long checkpointID) {
		ThreadLogDelta mainThreadDelta = mainThreadLog.getNextDeterminantsForDownstream(consumer, checkpointID);

		Map<IntermediateResultPartitionID, Map<Integer,SubpartitionThreadLogDelta>> subpartitionDeltas = new HashMap<>(subpartitionLogs.size());

		for(Map.Entry<IntermediateResultPartitionID, ConcurrentMap<Integer,UpstreamThreadCausalLog>> datasetEntry : subpartitionLogs.entrySet()){
			List<SubpartitionThreadLogDelta> deltasWithData = new ArrayList<>(datasetEntry.getValue().size());
			for(Map.Entry<Integer, UpstreamThreadCausalLog> subpartitionEntry : datasetEntry.getValue().entrySet()){
				SubpartitionThreadLogDelta delta =  new SubpartitionThreadLogDelta(subpartitionEntry.getValue().getNextDeterminantsForDownstream(consumer, checkpointID), subpartitionEntry.getKey());
				if(delta.getRawDeterminants().capacity() > 0)
					deltasWithData.add(delta);
			}
			if(!deltasWithData.isEmpty()){
				Map<Integer, SubpartitionThreadLogDelta> internalMap = new HashMap<>(deltasWithData.size());
				deltasWithData.forEach(d -> internalMap.put(d.getSubpartitionIndex(), d));
				subpartitionDeltas.put(datasetEntry.getKey(),internalMap);
			}

		}

		return new VertexCausalLogDelta(vertexId, (mainThreadDelta.getBufferSize() > 0 ? mainThreadDelta : null), subpartitionDeltas);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointID) {
		try {
			mainThreadLog.notifyCheckpointComplete(checkpointID);
			for(UpstreamThreadCausalLog log : subpartitionLogs.values().stream().flatMap(map -> map.values().stream()).collect(Collectors.toList()))
				log.notifyCheckpointComplete(checkpointID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
