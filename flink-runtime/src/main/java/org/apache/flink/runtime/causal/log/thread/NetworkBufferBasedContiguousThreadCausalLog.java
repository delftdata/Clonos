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
import org.apache.flink.shaded.netty4.io.netty.buffer.*;
import org.apache.flink.shaded.netty4.io.netty.util.ByteProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
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

	protected final CompositeByteBuf buf;

	@GuardedBy("buf")
	protected ConcurrentMap<Long, EpochStartOffset> epochStartOffsets;

	protected ConcurrentMap<InputChannelID, ConsumerOffset> channelOffsetMap;

	protected ReadWriteLock epochLock;
	protected Lock readLock;
	protected Lock writeLock;

	protected AtomicInteger writerIndex;

	protected long earliestEpoch;

	public NetworkBufferBasedContiguousThreadCausalLog(BufferPool bufferPool) {
		buf = ByteBufAllocator.DEFAULT.compositeDirectBuffer(Integer.MAX_VALUE);
		this.bufferPool = bufferPool;

		addComponent();
		epochStartOffsets = new ConcurrentHashMap<>();
		channelOffsetMap = new ConcurrentHashMap<>();
		writerIndex = new AtomicInteger(0);

		epochLock = new ReentrantReadWriteLock();
		readLock = epochLock.readLock();
		writeLock = epochLock.writeLock();

		earliestEpoch = 0L;
	}

	@Override
	public ByteBuf getDeterminants(long epoch) {
		ByteBuf result;
		int startIndex = 0;

		readLock.lock();
		try {
			EpochStartOffset offset = epochStartOffsets.get(epoch);
			if (offset != null)
				startIndex = offset.getOffset();
			int writerPos = writerIndex.get();
			int numBytesToSend = writerPos-startIndex;
			//result = buf.alloc().directBuffer(numBytesToSend);
			//buf.readBytes(result, startIndex, numBytesToSend);

			//result = getPooledByteBuf();
			//result.writeBytes(buf,writerPos, numBytesToSend);

			result = makeAllocatedDelta(buf, startIndex, numBytesToSend);


			//result= Unpooled.directBuffer(numBytesToSend, numBytesToSend);

			//result.writeBytes(buf, startIndex, numBytesToSend);
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
			ByteBuf update;

			if(numBytesToSend == 0)
				update = Unpooled.EMPTY_BUFFER;
			else {
				update = makeAllocatedDelta(buf, physicalConsumerOffset, numBytesToSend);
				//update = Unpooled.directBuffer(numBytesToSend, numBytesToSend);
				//update.writeBytes(buf, physicalConsumerOffset, numBytesToSend);
			}

			ThreadLogDelta toReturn = new ThreadLogDelta(update, consumerOffset.getOffset());
			consumerOffset.setOffset(consumerOffset.getOffset() + numBytesToSend);
			return toReturn;
		} finally {
			readLock.unlock();
		}
	}

	private ByteBuf getPooledByteBuf(){
		Buffer ret = null;
		try{
			while(ret == null)
				ret = bufferPool.requestBuffer();
		}catch(IOException e) {
			e.printStackTrace();
		}
		return ret.asByteBuf();
	}

	private ByteBuf makeAllocatedDelta(CompositeByteBuf buf, int srcOffset, int numBytesToSend){
		CompositeByteBuf result = ByteBufAllocator.DEFAULT.compositeDirectBuffer(Integer.MAX_VALUE);

		ByteBuf comp = getPooledByteBuf();
		comp.writerIndex(comp.capacity());
		int compSize = comp.capacity();
		int numComponentsNeeded = (int) Math.ceil(numBytesToSend/(float)compSize);

		result.addComponent(comp);
		for(int i = 1; i < numComponentsNeeded; i++){

			ByteBuf c = getPooledByteBuf();
			c.writerIndex(c.capacity());
			result.addComponent(c);
		}

		result.writeBytes(buf, srcOffset, numBytesToSend);
		return result;
	}

	/**
	 *
	 * Uses must be wrapped by reader lock
	 */
	private ByteBuf makeDeltaUnsafe(CompositeByteBuf buf, int srcOffset, int numBytesToSend) {
		CompositeByteBuf result = ByteBufAllocator.DEFAULT.compositeDirectBuffer(Integer.MAX_VALUE);

		int currOffset = srcOffset;
		int numBytesLeft = numBytesToSend;
		while(numBytesLeft != 0 ){

			ByteBuf component = buf.componentAtOffset(currOffset);
			int numBytesToAddFromThisComp = Math.min(numBytesLeft, component.capacity());
			//We do not retain due to the invariant that if a consumer has not received this epochs determinants,
			// then the epoch may not be completed. Thus, only when all references to this epochs components are
			// released, will the epoch be able to complete
			ByteBuf slice = component.retainedSlice(0, numBytesToAddFromThisComp);

			result.addComponent(true, slice);

			numBytesLeft -= numBytesToAddFromThisComp;
			currOffset += numBytesToAddFromThisComp;
		}
		return result;
	}

	@Override
	public int logLength() {
		int result;
		readLock.lock();
		try {
			EpochStartOffset offset = epochStartOffsets.get(earliestEpoch);
			if (offset != null)
				result = writerIndex.get() - offset.getOffset();
			else
				result = writerIndex.get();

		} finally {
			readLock.unlock();
		}
		return result;
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
		LOG.debug("Notify checkpoint complete for id {}", checkpointId);
		writeLock.lock();
		try {
			EpochStartOffset followingEpoch = epochStartOffsets.computeIfAbsent(checkpointId, epochID -> new EpochStartOffset(epochID, writerIndex.get()));
			for (Long epochID : epochStartOffsets.keySet())
				if (epochID < checkpointId)
					epochStartOffsets.remove(epochID);

			int followingEpochOffset = followingEpoch.getOffset();
			buf.readerIndex(followingEpochOffset);
			buf.discardReadComponents();
			int move = followingEpochOffset - buf.readerIndex();

			for (EpochStartOffset epochStartOffset : epochStartOffsets.values())
				epochStartOffset.setOffset(epochStartOffset.getOffset() - move);
			LOG.debug("Offsets moved by {} bytes", move);
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
		LOG.debug("Adding component, composite size: {}", buf.capacity());
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
	protected static class ConsumerOffset {
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
