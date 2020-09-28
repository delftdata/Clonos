/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.causal.services.BufferOrderService;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A wrapper around a {@link CheckpointBarrierHandler} which uses a {@link CausalBufferOrderService} to ensure
 * correct ordering of buffers.
 * <p>
 * This wrapper is implemented at this component and not the
 * {@link org.apache.flink.runtime.io.network.partition.consumer.InputGate} since the barriers serve as
 * synchronization points.
 * Thus, there is no need to record them in the CausalLog.
 */
public class CausalBufferHandler implements CheckpointBarrierHandler {

	private final Object lock;

	private CheckpointBarrierHandler wrapped;
	private BufferOrderService bufferOrderService;

	public CausalBufferHandler(EpochProvider epochProvider,
							   IJobCausalLog causalLoggingManager,
							   IRecoveryManager recoveryManager,
							   CheckpointBarrierHandler wrapped,
							   int numInputChannels,
							   Object checkpointLock) {
		this.wrapped = wrapped;
		this.lock = checkpointLock;
		this.bufferOrderService = new CausalBufferOrderService(recoveryManager, causalLoggingManager, epochProvider, wrapped, numInputChannels);
	}


	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		//We lock to guarantee that async events don't try to write to the causal log at the same time as the
		// order service.
		synchronized (lock){
			return bufferOrderService.getNextBuffer();
		}
	}



	@Override
	public void registerCheckpointEventHandler(AbstractInvokable task) {
		this.wrapped.registerCheckpointEventHandler(task);
	}

	@Override
	public void cleanup() throws IOException {
		this.wrapped.cleanup();
	}

	@Override
	public boolean isEmpty() {
		return wrapped.isEmpty();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return wrapped.getAlignmentDurationNanos();
	}

	@Override
	public void ignoreCheckpoint(long checkpointID) throws IOException {
		wrapped.ignoreCheckpoint(checkpointID);
	}

	@Override
	public void unblockChannelIfBlocked(int absoluteChannelIndex) {
		this.wrapped.unblockChannelIfBlocked(absoluteChannelIndex);
	}
}
