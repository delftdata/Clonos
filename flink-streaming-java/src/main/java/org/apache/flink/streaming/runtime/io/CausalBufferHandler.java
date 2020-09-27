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
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A wrapper around a {@link CheckpointBarrierHandler} which interacts with the
 * {@link org.apache.flink.runtime.causal.recovery.RecoveryManager} and the
 * {@link org.apache.flink.runtime.causal.log.job.JobCausalLog}
 * to ensure correct replay.
 * <p>
 * This wrapper is implemented at this component and not the
 * {@link org.apache.flink.runtime.io.network.partition.consumer.InputGate} since the barriers serve as
 * synchronization points.
 * Thus, there is no need to record them in the CausalLog.
 */
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

	private OrderDeterminant reuseOrderDeterminant;

	private boolean isRecovering;

	private final int numInputChannels;

	public CausalBufferHandler(EpochProvider epochProvider,
							   IJobCausalLog causalLoggingManager,
							   IRecoveryManager recoveryManager,
							   CheckpointBarrierHandler wrapped,
							   int numInputChannels,
							   Object checkpointLock) {

		this.numInputChannels = numInputChannels;
		this.epochProvider = epochProvider;
		this.causalLoggingManager = causalLoggingManager;
		this.recoveryManager = recoveryManager;
		this.wrapped = wrapped;
		this.bufferedBuffersPerChannel = new LinkedList[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new LinkedList<>();
		this.numUnprocessedBuffers = 0;
		this.lock = checkpointLock;
		this.reuseOrderDeterminant = new OrderDeterminant();
		this.isRecovering = recoveryManager.isRecovering();
	}


	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		BufferOrEvent toReturn = null;

		if (numInputChannels == 1) {
			//Simple case, when there is only one channel we do not need to store order determinants, nor
			// do any special replay logic
			toReturn = getNextNonBlockedNew();
		} else {
			if (isRecovering && (isRecovering = recoveryManager.isReplaying()))
				toReturn = getNextNonBlockedReplayed();
			else
				toReturn = getNextNonBlockedNew();

			//When there is more than one channel we do need to append determinants
			synchronized (lock) {
				causalLoggingManager.appendDeterminant(reuseOrderDeterminant.replace((byte) toReturn.getChannelIndex()),
					epochProvider.getCurrentEpochID());
			}
		}

		LOG.debug("Returning buffer from channel {}", toReturn.getChannelIndex());
		return toReturn;
	}

	private BufferOrEvent getNextNonBlockedNew() throws Exception {
		BufferOrEvent toReturn;
		while (true) {
			if (numUnprocessedBuffers != 0)
				toReturn = pickBufferedUnprocessedBuffer();
			else
				toReturn = wrapped.getNextNonBlocked();

			if (toReturn.isEvent() && toReturn.getEvent().getClass() == DeterminantRequestEvent.class) {
				LOG.debug("Buffer is DeterminantRequest, sending notification");
				recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) toReturn.getEvent(),
					toReturn.getChannelIndex());
				continue;
			}
			LOG.debug("Buffer is valid, forwarding");
			break;
		}
		return toReturn;
	}

	private BufferOrEvent getNextNonBlockedReplayed() throws Exception {
		BufferOrEvent toReturn;
		byte nextChannel = recoveryManager.replayNextChannel();
		LOG.debug("Determinant says next channel is {}!", nextChannel);
		while (true) {
			if (!bufferedBuffersPerChannel[nextChannel].isEmpty()) {
				toReturn = bufferedBuffersPerChannel[nextChannel].poll();
				numUnprocessedBuffers--;
			} else {
				toReturn = processUntilFindBufferForChannel(nextChannel);
			}

			if (toReturn.isEvent() && toReturn.getEvent().getClass() == DeterminantRequestEvent.class) {
				LOG.debug("Buffer is DeterminantRequest, sending notification");
				recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) toReturn.getEvent(),
					toReturn.getChannelIndex());
				continue;
			}
			LOG.debug("Buffer is valid, forwarding");
			break;
		}
		return toReturn;
	}

	private BufferOrEvent pickBufferedUnprocessedBuffer() {
		//todo improve runtime complexity
		for (Queue<BufferOrEvent> queue : bufferedBuffersPerChannel) {
			if (!queue.isEmpty()) {
				numUnprocessedBuffers--;
				return queue.poll();
			}
		}
		return null;//unrecheable
	}

	private BufferOrEvent processUntilFindBufferForChannel(byte channel) throws Exception {
		LOG.debug("Found no buffered buffers for channel {}. Processing buffers until I find one", channel);
		while (true) {
			BufferOrEvent newBufferOrEvent = wrapped.getNextNonBlocked();
			LOG.debug("Got a new buffer from channel {}", newBufferOrEvent.getChannelIndex());
			//If this was a BoE for the channel we were looking for, return with it
			if (newBufferOrEvent.getChannelIndex() == channel) {
				LOG.debug("It is from the expected channel, returning");
				return newBufferOrEvent;
			}

			LOG.debug("It is not from the expected channel, continuing");
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
		for (Queue queue : bufferedBuffersPerChannel)
			if (!queue.isEmpty())
				return false;

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
