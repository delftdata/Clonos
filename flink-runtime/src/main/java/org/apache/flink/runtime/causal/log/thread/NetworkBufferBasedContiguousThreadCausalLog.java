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

package org.apache.flink.runtime.causal.log.thread;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Thread level causal log.
 * Local versions, main thread and subpartition thread are SPMC and SPSC respectively.
 * Upstream version is MPMC.
 * We have to be conservative and treat all as MPMC.
 * Furthermore, there are asynchronous checkpoint notifications, which may change epoch start offsets.
 */
public class NetworkBufferBasedContiguousThreadCausalLog implements ThreadCausalLog {

	protected static final Logger LOG = LoggerFactory.getLogger(NetworkBufferBasedContiguousThreadCausalLog.class);

	protected final BufferPool bufferPool;

	protected CompositeByteBuf buf;

	@GuardedBy("buf")
	protected ConcurrentMap<Long, EpochStartOffset> epochStartOffsets;

	protected ConcurrentMap<InputChannelID, ConsumerOffset> channelOffsetMap;

	protected ReadWriteLock epochLock;
	protected Lock readLock;
	protected Lock writeLock;

	//Writers compare and set to reserve space in the buffer.
	protected AtomicInteger writerIndex;

	protected long earliestEpoch;

	public NetworkBufferBasedContiguousThreadCausalLog(BufferPool bufferPool) {
		buf = Unpooled.compositeBuffer();
		this.bufferPool = bufferPool;

		addComponent();
		epochStartOffsets = new ConcurrentHashMap<>();
		channelOffsetMap = new ConcurrentHashMap<>();
		writerIndex = new AtomicInteger(0);

		epochLock = new ReentrantReadWriteLock();
		readLock = epochLock.readLock();
		writeLock = epochLock.writeLock();

		earliestEpoch = 0l;
	}

	@Override
	public ByteBuf getDeterminants() {
		ByteBuf result;
		int startIndex = 0;

		readLock.lock();
		try {
			EpochStartOffset offset = epochStartOffsets.get(earliestEpoch);
			if (offset != null)
				startIndex = offset.getOffset();

			synchronized (buf) {
				result = buf.asReadOnly().slice(startIndex, writerIndex.get()-startIndex).retain();
			}
		} finally {
			readLock.unlock();
		}
		return result;
	}

	@Override
	public ThreadLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long epochID) {
		//If a request is coming for the next determinants, then the epoch MUST already exist.
		readLock.lock();
		try {
			EpochStartOffset epochStartOffset = epochStartOffsets.get(epochID);
			if (epochStartOffset == null)
				return new ThreadLogDelta(Unpooled.EMPTY_BUFFER, 0);
			ConsumerOffset consumerOffset = channelOffsetMap.computeIfAbsent(consumer, k -> new ConsumerOffset(epochStartOffset));

			long currentConsumerEpochID = consumerOffset.getEpochStart().getId();
			if (currentConsumerEpochID != epochID) {
				if (currentConsumerEpochID > epochID)
					throw new RuntimeException("Consumer went backwards!");
				//if (currentConsumerEpochID + 1 != epochID)
				//	throw new RuntimeException("Consumer skipped an epoch!");
				consumerOffset.epochStart = epochStartOffset;
				consumerOffset.offset = 0;
			}

			int physicalConsumerOffset = consumerOffset.epochStart.offset + consumerOffset.offset;

			int numBytesToSend = computeNumberOfBytesToSend(epochID, physicalConsumerOffset);

			ByteBuf update = buf.asReadOnly().slice(physicalConsumerOffset, numBytesToSend).retain();

			ThreadLogDelta toReturn = new ThreadLogDelta(update, consumerOffset.getOffset());
			consumerOffset.setOffset(consumerOffset.getOffset() + numBytesToSend);
			return toReturn;
		} finally {
			readLock.unlock();
		}
	}

	private int computeNumberOfBytesToSend(long epochID, int physicalConsumerOffset) {
		int currentWriteIndex = writerIndex.get();
		EpochStartOffset nextEpochStartOffset = epochStartOffsets.get(epochID + 1);
		int numBytesToSend;

		if (nextEpochStartOffset != null)
			numBytesToSend = nextEpochStartOffset.getOffset() - physicalConsumerOffset;
		else
			numBytesToSend = currentWriteIndex - physicalConsumerOffset;
		return numBytesToSend;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("Notify checkpoint complete for id {}", checkpointId);
		writeLock.lock();
		try {
			EpochStartOffset followingEpoch = epochStartOffsets.computeIfAbsent(checkpointId, k -> new EpochStartOffset(k, writerIndex.get()));
			for (Long epochID : epochStartOffsets.keySet())
				if (epochID < checkpointId)
					epochStartOffsets.remove(epochID);

			int followingEpochOffset = followingEpoch.getOffset();
			buf.readerIndex(followingEpochOffset);
			buf.discardReadComponents();
			int move = followingEpochOffset - buf.readerIndex();

			for (EpochStartOffset epochStartOffset : epochStartOffsets.values())
				epochStartOffset.setOffset(epochStartOffset.getOffset() - move);
			LOG.info("Offsets moved by {} bytes", move);
			writerIndex.set(writerIndex.get() - move);
			earliestEpoch = checkpointId;
		} finally {
			writeLock.unlock();
		}
	}


	@Override
	public String toString() {
		return "NetworkBufferBasedContiguousLocalThreadCausalLog{" +
			"buf=" + buf +
			", earliestEpoch=" + earliestEpoch +
			", writerIndex=" + writerIndex.get() +
			", epochStartOffsets=[" + epochStartOffsets.entrySet().stream().map(Objects::toString).collect(Collectors.joining(", ")) + "]" +
			", channelOffsetMap={" + channelOffsetMap.entrySet().stream().map(x -> x.getKey() + " -> " + x.getValue()).collect(Collectors.joining(", ")) + "}" +
			'}';
	}

	protected boolean notEnoughSpaceFor(int length) {
		return buf.writableBytes() < length;
	}


	protected void addComponent() {
		Buffer buffer = null;

		try {
			buffer = bufferPool.requestBufferBlocking();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		ByteBuf byteBuf = buffer.asByteBuf();
		//The writer index movement tricks netty into adding to the composite capacity.
		byteBuf.writerIndex(byteBuf.capacity());
		buf.addComponent(byteBuf);
	}

	protected static class EpochStartOffset {
		//The checkpoint id that initiates this epoch
		private long id;
		//The physical offset in the circular log of the first element after the checkpoint
		private int offset;


		public EpochStartOffset(long id, int offset) {
			this.id = id;
			this.offset = offset;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}


		@Override
		public String toString() {
			return "CheckpointOffset{" +
				"id=" + id +
				", offset=" + offset +
				'}';
		}
	}

	/**
	 * Marks the next element to be read by the downstream consumer
	 */
	protected class ConsumerOffset {
		// Refers to the epoch that the downstream is currently in
		private EpochStartOffset epochStart;

		// The logical offset from that epoch
		private int offset;

		public ConsumerOffset(EpochStartOffset epochStart) {
			this.epochStart = epochStart;
			this.offset = 0;
		}

		public EpochStartOffset getEpochStart() {
			return epochStart;
		}

		public void setEpochStart(EpochStartOffset epochStart) {
			this.epochStart = epochStart;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}

		@Override
		public String toString() {
			return "DownstreamChannelOffset{" +
				"epochStart=" + epochStart +
				", offset=" + offset +
				'}';
		}
	}
}
