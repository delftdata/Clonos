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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CausalBufferHandler implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CausalBufferHandler.class);
	private final IRecoveryManager recoveryManager;
	private final EpochProvider epochProvider;
	private final Object lock;

	private IJobCausalLog causalLoggingManager;

	private Queue<BufferOrEvent>[] bufferedBuffersPerChannel;

	private CheckpointBarrierHandler wrapped;

	//This is just to reduce the complexity of going over bufferedBuffersPerChannel
	private int numUnprocessedBuffers;

	public CausalBufferHandler(EpochProvider epochProvider, IJobCausalLog causalLoggingManager, IRecoveryManager recoveryManager, CheckpointBarrierHandler wrapped, int numInputChannels, Object checkpointLock) {
		this.epochProvider = epochProvider;
		this.causalLoggingManager = causalLoggingManager;
		this.recoveryManager = recoveryManager;
		this.wrapped = wrapped;
		this.bufferedBuffersPerChannel = new LinkedList[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new LinkedList<>();
		numUnprocessedBuffers = 0;
		this.lock = checkpointLock;
	}


	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		LOG.info("Call to getNextNonBlocked");
		BufferOrEvent toReturn = null;
		if (recoveryManager.isReplaying()) {
			while(toReturn == null) {
				LOG.info("We are replaying! Fetching a determinant from the CausalLogManager");
				byte nextChannel = recoveryManager.replayNextChannel();
				LOG.info("Determinant says next channel is {}!", nextChannel);
				if (bufferedBuffersPerChannel[nextChannel].isEmpty())
					toReturn = processUntilFindBufferForChannel(nextChannel);
				else {
					toReturn = bufferedBuffersPerChannel[nextChannel].poll();
					numUnprocessedBuffers--;
				}
			if(toReturn.isEvent() && toReturn.getEvent().getClass() == DeterminantRequestEvent.class)
				toReturn = null; //skip, as it has already been processed.

			}
		} else {
			LOG.info("We are not recovering!");
			if (numUnprocessedBuffers != 0) {
				LOG.info("Getting an unprocessed buffer from recovery! Unprocessed buffers: {}, bufferedBuffers: {}", numUnprocessedBuffers,
					"{" + Arrays.asList(bufferedBuffersPerChannel).stream().map(d -> "[" + d.size() + "]").collect(Collectors.joining(", ")) + "}");
				toReturn = pickUnprocessedBuffer();
			}else {
				LOG.info("Getting a buffer from CheckpointBarrierHandler!");
				while(true) {
					toReturn = wrapped.getNextNonBlocked();
					if (toReturn.isEvent()) {
						AbstractEvent event = toReturn.getEvent();
						if (event.getClass() == DeterminantRequestEvent.class) {
							recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) event, toReturn.getChannelIndex());
						}else
							break;
					}else
						break;
				}
			}
		}
		LOG.info("Returning buffer from channel {} : {}", toReturn.getChannelIndex(), toReturn);
		synchronized (lock) {
			causalLoggingManager.appendDeterminant(new OrderDeterminant((byte) toReturn.getChannelIndex()), epochProvider.getCurrentEpochID());
		}
		return toReturn;
	}

	private BufferOrEvent pickUnprocessedBuffer() {
		//todo improve runtime complexity
		for(Queue<BufferOrEvent> queue : bufferedBuffersPerChannel) {
			if(!queue.isEmpty()) {
				numUnprocessedBuffers--;
				return queue.poll();
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
			bufferedBuffersPerChannel[newBufferOrEvent.getChannelIndex()].add(newBufferOrEvent);
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
		for (Queue qeque : bufferedBuffersPerChannel)
			if (!qeque.isEmpty())
				return false;

		return wrapped.isEmpty();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return wrapped.getAlignmentDurationNanos();
	}
}
