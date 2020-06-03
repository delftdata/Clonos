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

import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Causal Log local threads.
 * For main thread it must be a SPMC log.
 * For subpartition logs it is SPSC.
 * Since there is a single producer, we can always block and request more segments.
 * There still needs to be a lock which guards access to the buffer, due to the fact that checkpointCompletes are
 * assynchronous notifications.
 * Eack consumer can only update its own index, meaning this is thread safe.
 */
public class NetworkBufferBasedContiguousLocalThreadCausalLog implements LocalThreadCausalLog {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferBasedContiguousLocalThreadCausalLog.class);

	private final BufferPool bufferPool;

	CompositeByteBuf buf;

	@GuardedBy("buf")
	private SortedMap<Long, EpochStartOffset> epochStartOffsets;

	private ConcurrentMap<InputChannelID, ConsumerOffset> channelOffsetMap;


	public NetworkBufferBasedContiguousLocalThreadCausalLog(BufferPool bufferPool) {
		buf = Unpooled.compositeBuffer();
		this.bufferPool = bufferPool;

		addComponent();
		epochStartOffsets = new TreeMap<>();

		EpochStartOffset startEpochOffset = new EpochStartOffset(0l, 0);
		epochStartOffsets.put(0l, startEpochOffset);

		//initialized to 0s
		channelOffsetMap = new ConcurrentHashMap<>();
	}




	@Override
	public void appendDeterminants(byte[] determinants, long checkpointID) {
		synchronized (buf) {
			if (!epochStartOffsets.containsKey(checkpointID))
				epochStartOffsets.put(checkpointID, new EpochStartOffset(checkpointID, buf.writerIndex()));
			LOG.info("Grabbed buf lock to write determinants");
			while (!hasSpaceFor(determinants.length))
				addComponent();

			buf.writeBytes(determinants);
		}
		LOG.info("Done");
	}


	@Override
	public ByteBuf getDeterminants() {
		synchronized (buf) {
			return buf.asReadOnly().retain();
		}
	}

	@Override
	public ThreadLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long epochID) {

		EpochStartOffset epochStartOffset = epochStartOffsets.get(epochID);
		if (epochStartOffset == null)
			return new ThreadLogDelta(Unpooled.EMPTY_BUFFER, 0);

		ConsumerOffset consumerOffset = channelOffsetMap.computeIfAbsent(consumer, k -> new ConsumerOffset(epochStartOffset));
		if (consumerOffset.getEpochStart().getId() != epochID) {
			if (consumerOffset.getEpochStart().getId() > epochID)
				throw new RuntimeException("Consumer went backwards!");
			consumerOffset.epochStart = epochStartOffset;
			consumerOffset.offset = 0;
		}

		int physicalConsumerOffset = consumerOffset.epochStart.offset + consumerOffset.offset;

		EpochStartOffset nextEpochStartOffset = epochStartOffsets.get(epochID + 1);
		int numBytesToSend;
		if(nextEpochStartOffset != null)
			numBytesToSend = nextEpochStartOffset.getOffset() - physicalConsumerOffset;
		else
			numBytesToSend = buf.writerIndex() - physicalConsumerOffset;

		ByteBuf update;
		synchronized (buf) {
			update = buf.asReadOnly().slice(physicalConsumerOffset, numBytesToSend).retain();
		}
		ThreadLogDelta toReturn = new ThreadLogDelta(update, consumerOffset.getOffset());
		consumerOffset.setOffset(consumerOffset.getOffset() + numBytesToSend);
		return toReturn;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (buf) {
			List<Long> epochsToRemove = new LinkedList<>();
			for (Long epoch : epochStartOffsets.keySet())
				if (epoch < checkpointId)
					epochsToRemove.add(epoch);

			for (Long epoch : epochsToRemove)
				epochStartOffsets.remove(epoch);


			if (epochStartOffsets.size() > 0) {
				EpochStartOffset newEarliestOffset = epochStartOffsets.get(epochStartOffsets.firstKey());
				buf.readerIndex(newEarliestOffset.offset);
				buf.discardReadComponents();
				int move = newEarliestOffset.offset - buf.readerIndex();

				for (EpochStartOffset epochStartOffset : epochStartOffsets.values()) {
					epochStartOffset.setOffset(epochStartOffset.getOffset() - move);
				}
			}
		}
	}


	@Override
	public String toString() {
		return "NetworkBufferBasedContiguousLocalThreadCausalLog{" +
			"buf=" + buf +
			", epochStartOffsets=[" + epochStartOffsets.entrySet().stream().map(Objects::toString).collect(Collectors.joining(", ")) + "]" +
			", channelOffsetMap={" + channelOffsetMap.entrySet().stream().map(x -> x.getKey() + " -> " + x.getValue()).collect(Collectors.joining(", ")) + "}" +
			'}';
	}

	@GuardedBy("buf")
	private boolean hasSpaceFor(int length) {
		LOG.info("Has Space? Determinent length {}, writable bytes {}, capacity {}", length, buf.writableBytes(), buf.capacity());
		return buf.writableBytes() >= length;
	}


	private void addComponent() {
		Buffer buffer = null;

		try {
			buffer = bufferPool.requestBufferBlocking();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Requested buffer {}", buffer);
		ByteBuf byteBuf = buffer.asByteBuf();
		//The writer index movement tricks netty into adding to the composite capacity.
		LOG.info("CompBuf before: {}", buf);
		byteBuf.writerIndex(byteBuf.capacity());
		buf.addComponent(byteBuf);
		LOG.info("CompBuf after: {}", buf);
	}

	private static class EpochStartOffset {
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
	private class ConsumerOffset {
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
