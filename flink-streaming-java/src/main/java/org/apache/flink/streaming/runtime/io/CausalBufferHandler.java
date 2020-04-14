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

import org.apache.flink.runtime.causal.CausalLoggingManager;
import org.apache.flink.runtime.causal.RecoveryManager;
import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

public class CausalBufferHandler implements CheckpointBarrierHandler {

	private RecoveryManager recoveryManager;
	private CausalLoggingManager causalLoggingManager;

	private Deque<BufferOrEvent>[] bufferedBuffersPerChannel;

	private CheckpointBarrierHandler wrapped;

	//This is just to reduce the complexity of going over bufferedBuffersPerChannel
	private int numUnprocessedBuffers;

	public CausalBufferHandler(CausalLoggingManager causalLoggingManager, RecoveryManager recoveryManager, CheckpointBarrierHandler wrapped, int numInputChannels) {
		this.causalLoggingManager = causalLoggingManager;
		this.recoveryManager = recoveryManager;
		this.wrapped = wrapped;
		this.bufferedBuffersPerChannel = new Deque[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new ArrayDeque<>();
		numUnprocessedBuffers = 0;
	}


	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		BufferOrEvent toReturn;
		if (recoveryManager.isRecovering()) {
			OrderDeterminant determinant = recoveryManager.getNextOrderDeterminant();
			if (bufferedBuffersPerChannel[determinant.getChannel()].isEmpty())
				toReturn = processUntilFindBufferForChannel(determinant.getChannel());
			else
				toReturn = bufferedBuffersPerChannel[determinant.getChannel()].pop();
		} else {
			if (numUnprocessedBuffers != 0)
				toReturn = pickUnprocessedBuffer();
			else
				toReturn = wrapped.getNextNonBlocked();
		}
		causalLoggingManager.appendDeterminant(new OrderDeterminant((byte) toReturn.getChannelIndex()));
		return toReturn;
	}

	private BufferOrEvent pickUnprocessedBuffer() {
		for(Deque<BufferOrEvent> deque : bufferedBuffersPerChannel) {
			if(!deque.isEmpty()) {
				numUnprocessedBuffers--;
				return deque.pop();
			}
		}
		return null;//unrecheable
	}

	private BufferOrEvent processUntilFindBufferForChannel(byte channel) throws Exception {
		while(true){
			BufferOrEvent newBufferOrEvent = wrapped.getNextNonBlocked();

			//If this was a BoE for the channel we were looking for, return with it
			if(newBufferOrEvent.getChannelIndex() == channel)
				return newBufferOrEvent;

			//Otherwise, append it to the correct queue and try again
			bufferedBuffersPerChannel[newBufferOrEvent.getChannelIndex()].push(newBufferOrEvent);
			numUnprocessedBuffers++;
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
		for (Deque deque : bufferedBuffersPerChannel)
			if (!deque.isEmpty())
				return false;

		return wrapped.isEmpty();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return wrapped.getAlignmentDurationNanos();
	}
}
