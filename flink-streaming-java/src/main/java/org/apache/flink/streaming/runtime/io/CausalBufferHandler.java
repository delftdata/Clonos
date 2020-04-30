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

import org.apache.flink.runtime.causal.ICausalLoggingManager;
import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.stream.Collectors;

public class CausalBufferHandler implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CausalBufferHandler.class);

	private ICausalLoggingManager causalLoggingManager;

	private Deque<BufferOrEvent>[] bufferedBuffersPerChannel;

	private CheckpointBarrierHandler wrapped;

	//This is just to reduce the complexity of going over bufferedBuffersPerChannel
	private int numUnprocessedBuffers;

	public CausalBufferHandler(ICausalLoggingManager causalLoggingManager, CheckpointBarrierHandler wrapped, int numInputChannels) {
		this.causalLoggingManager = causalLoggingManager;
		this.wrapped = wrapped;
		this.bufferedBuffersPerChannel = new Deque[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new ArrayDeque<>();
		numUnprocessedBuffers = 0;
	}


	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		//todo should process DeterminantRequestEvents and respond to them
		LOG.info("Call to getNextNonBlocked");
		BufferOrEvent toReturn;
		if (causalLoggingManager.hasDeterminantsToRecoverFrom()) {
			if(!causalLoggingManager.isRecovering())
				causalLoggingManager.startRecovery();
			LOG.info("We are recovering! Fetching a determinant from the CausalLogManager");
			OrderDeterminant determinant = causalLoggingManager.getRecoveryOrderDeterminant();
			LOG.info("Determinant says {}!", determinant);
			if (bufferedBuffersPerChannel[determinant.getChannel()].isEmpty())
				toReturn = processUntilFindBufferForChannel(determinant.getChannel());
			else
				toReturn = bufferedBuffersPerChannel[determinant.getChannel()].pop();
		} else {
			if(causalLoggingManager.isRecovering()){
				causalLoggingManager.stopRecovery();
			}
			LOG.info("We are not recovering!");
			if (numUnprocessedBuffers != 0) {
				LOG.info("Getting an unprocessed buffer from recovery! Unprocessed buffers: {}, bufferedBuffers: {}", numUnprocessedBuffers,
					"{" + Arrays.asList(bufferedBuffersPerChannel).stream().map(d -> "[" + d.size() + "]").collect(Collectors.joining(", ")) + "}");
				toReturn = pickUnprocessedBuffer();
			}else {
				LOG.info("Getting a buffer from CheckpointBarrierHandler!");
				toReturn = wrapped.getNextNonBlocked();
			}
		}
		causalLoggingManager.appendDeterminant(new OrderDeterminant((byte) toReturn.getChannelIndex()));
		return toReturn;
	}

	private BufferOrEvent pickUnprocessedBuffer() {
		//todo improve runtime complexity
		for(Deque<BufferOrEvent> deque : bufferedBuffersPerChannel) {
			if(!deque.isEmpty()) {
				numUnprocessedBuffers--;
				return deque.pop();
			}
		}
		return null;//unrecheable
	}

	private BufferOrEvent processUntilFindBufferForChannel(byte channel) throws Exception {
		LOG.info("Found no buffered buffers for channel {}. Processing buffers until I find one", channel);
		while(true){
			BufferOrEvent newBufferOrEvent = wrapped.getNextNonBlocked();
			LOG.info("Got a new buffer from channel {}", newBufferOrEvent.getChannelIndex());
			//If this was a BoE for the channel we were looking for, return with it
			if(newBufferOrEvent.getChannelIndex() == channel) {
				LOG.info("It is from the expected channel, returning");
				return newBufferOrEvent;
			}

			LOG.info("It is not from the expected channel, continuing");
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
