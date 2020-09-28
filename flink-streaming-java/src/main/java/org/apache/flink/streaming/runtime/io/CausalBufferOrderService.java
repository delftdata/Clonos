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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.causal.services.AbstractCausalService;
import org.apache.flink.runtime.causal.services.BufferOrderService;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

/**
 * A service that delivers deterministically ordered buffers from the {@link CheckpointBarrierHandler}.
 *
 * This service is slightly more complex than other services as it returns the actual buffers and not just the
 * channel order determinant value.
 */
public class CausalBufferOrderService extends AbstractCausalService implements BufferOrderService {

	private static final Logger LOG = LoggerFactory.getLogger(CausalBufferOrderService.class);

	// The place from where we pull buffers in order to order them.
	private final CheckpointBarrierHandler bufferSource;

	// We use this to buffer buffers from the incorrect channels in order to deliver them in correct order
	private Queue<BufferOrEvent>[] bufferedBuffersPerChannel;

	// The determinant object we reuse to avoid object creation and thus large GC
	private OrderDeterminant reuseOrderDeterminant;

	// The number of input channels this task has. Important for buffering and exception cases such as = 1
	private final int numInputChannels;

	// If there are any buffered buffers, we need to check the queues
	private int numBufferedBuffers;

	public CausalBufferOrderService(IRecoveryManager recoveryManager,IJobCausalLog causalLoggingManager,
									EpochProvider epochProvider, CheckpointBarrierHandler bufferSource, int numInputChannels) {
		super(recoveryManager, causalLoggingManager, epochProvider);
		this.bufferSource = bufferSource;
		this.bufferedBuffersPerChannel = new LinkedList[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new LinkedList<>();
		this.numInputChannels = numInputChannels;
		this.numBufferedBuffers = 0;
		this.reuseOrderDeterminant = new OrderDeterminant();
	}

	@Override
	public BufferOrEvent getNextBuffer() throws Exception {

		//Simple case, when there is only one channel we do not need to store order determinants, nor
		// do any special replay logic, because everything is deterministic.
		if (numInputChannels == 1)
			return getNextNonBlockedNew();

		BufferOrEvent toReturn;
		if (isRecovering())
			toReturn = getNextNonBlockedReplayed();
		else
			toReturn = getNextNonBlockedNew();

		causalLoggingManager.appendDeterminant(reuseOrderDeterminant.replace((byte) toReturn.getChannelIndex()),
			epochProvider.getCurrentEpochID());

		return toReturn;
	}

	private BufferOrEvent getNextNonBlockedNew() throws Exception {
		BufferOrEvent toReturn;
		while (true) {
			if (numBufferedBuffers != 0)
				toReturn = pickBufferedUnprocessedBuffer();
			else
				toReturn = bufferSource.getNextNonBlocked();

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
				numBufferedBuffers--;
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
				numBufferedBuffers--;
				return queue.poll();
			}
		}
		return null;//unrecheable
	}

	private BufferOrEvent processUntilFindBufferForChannel(byte channel) throws Exception {
		LOG.debug("Found no buffered buffers for channel {}. Processing buffers until I find one", channel);
		while (true) {
			BufferOrEvent newBufferOrEvent = bufferSource.getNextNonBlocked();
			LOG.debug("Got a new buffer from channel {}", newBufferOrEvent.getChannelIndex());
			//If this was a BoE for the channel we were looking for, return with it
			if (newBufferOrEvent.getChannelIndex() == channel) {
				LOG.debug("It is from the expected channel, returning");
				return newBufferOrEvent;
			}

			LOG.debug("It is not from the expected channel, continuing");
			//Otherwise, append it to the correct queue and try again
			bufferedBuffersPerChannel[newBufferOrEvent.getChannelIndex()].add(newBufferOrEvent);
			numBufferedBuffers++;
		}
	}
}
