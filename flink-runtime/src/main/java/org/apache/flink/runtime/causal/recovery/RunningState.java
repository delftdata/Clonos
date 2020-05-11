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

package org.apache.flink.runtime.causal.recovery;

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.VertexId;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * We either start in this state, or transition to it after the full recovery.
 */
public class RunningState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(RunningState.class);

	public RunningState(RecoveryManager context) {
		super(context);
		while (!context.unansweredInFlighLogRequests.isEmpty()) {
			InFlightLogRequestEvent request = context.unansweredInFlighLogRequests.poll();
			((PipelinedSubpartition) context.intermediateDataSetIDResultPartitionMap.get(request.getIntermediateDataSetID()).getResultSubpartitions()[request.getSubpartitionIndex()]).requestReplay(request.getCheckpointId(), request.getNumberOfBuffersToSkip());
		}
	}


	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		LOG.info("Received an InflightLogRequest {}", e);
		LOG.info("Registered result partition datasetIDs: {}", context.intermediateDataSetIDResultPartitionMap.keySet().stream().map(Objects::toString).collect(Collectors.joining(", ")));
		ResultPartitionWriter resultPartition = context.intermediateDataSetIDResultPartitionMap.get(e.getIntermediateDataSetID());
		LOG.info("Result partition to request replay from: {}", resultPartition);
		((PipelinedSubpartition) resultPartition.getResultSubpartitions()[e.getSubpartitionIndex()]).requestReplay(e.getCheckpointId(), e.getNumberOfBuffersToSkip());
	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		LOG.info("Received a determinant request {} on channel {}", e, channelRequestArrivedFrom);
		//Since we are in running state, we can simply reply
		VertexId vertex = e.getFailedVertex();
		try {
			DeterminantResponseEvent responseEvent = new DeterminantResponseEvent(vertex, context.causalLoggingManager.getDeterminantsOfVertex(vertex));
			LOG.info("Responding with: {}", responseEvent);
			context.inputGate.sendTaskEvent(responseEvent);
		} catch (IOException | InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "RunningState{}";
	}
}
