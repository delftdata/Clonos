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
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * We either start in this state, or transition to it after the full recovery.
 */
public class RunningState extends AbstractState {

	private static final Logger LOG = LoggerFactory.getLogger(RunningState.class);

	public RunningState(RecoveryManager context) {
		super(context);
		while (!context.unansweredInFlighLogRequests.isEmpty()) {
			InFlightLogRequestEvent req = context.unansweredInFlighLogRequests.poll();
			RecordWriter rw = context.intermediateDataSetIDToRecordWriter.get(req.getIntermediateDataSetID());
			PipelinedSubpartition subpartitionRequested = ((PipelinedSubpartition)rw.getResultPartition().getResultSubpartitions()[req.getSubpartitionIndex()]);
			subpartitionRequested.requestReplay(req.getCheckpointId(), req.getNumberOfBuffersToSkip());
		}
	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		LOG.info("Received an InflightLogRequest {}", e);
		RecordWriter rw = context.intermediateDataSetIDToRecordWriter.get(e.getIntermediateDataSetID());
		PipelinedSubpartition subpartitionRequested = ((PipelinedSubpartition)rw.getResultPartition().getResultSubpartitions()[e.getSubpartitionIndex()]);
		LOG.info("Result partition intermedieateDataset to request replay from: {}", e.getIntermediateDataSetID());
		subpartitionRequested.requestReplay(e.getCheckpointId(), e.getNumberOfBuffersToSkip());
	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e, int channelRequestArrivedFrom) {
		LOG.info("Received a determinant request {} on channel {}", e, channelRequestArrivedFrom);
		//Since we are in running state, we can simply reply
		VertexId vertex = e.getFailedVertex();
		try {
			//todo we cant just .array(). Fixing for tests to work
			DeterminantResponseEvent responseEvent = new DeterminantResponseEvent(vertex, context.jobCausalLoggingManager.getDeterminantsOfVertex(vertex).array());
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
