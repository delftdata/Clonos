/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional debugrmation
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

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * {@link SpilledReplayIterator} is to be used in combination with {@link SpillableSubpartitionInFlightLogger}.
 * The {@link SpillableSubpartitionInFlightLogger} spills the in-flight log to disk asynchronously, while this
 * {@link InFlightLogIterator} implementation is able to then read those files and regenerate those buffers.
 * This is done deterministically and buffers have the exact same size.
 * <p>
 * To achieve this behaviour we split the Iterator into a producer and a consumer. The producer will first lock
 * the <code>subpartitionLock</code>, preventing any in-memory buffers to be spilled. Then it uses all buffers available in
 * the partition's {@link BufferPool} to create asynchronous read requests to the spill files. It produces these
 * segments through callbacks into separate {@link LinkedBlockingDeque}'s, since each epoch is in a different file,
 * and each file may be served by a separate async IO thread. Not doing so could cause interleavings of messages.
 * <p>
 * The consumer is simple in comparison. It simply checks if the buffer is available in memory, and if it is,
 * returns it. Otherwise, it will check the appropriate deque for the buffer, blocking if necessary.
 */
public class SpilledReplayIterator extends InFlightLogIterator<Buffer> {
	private static final Logger LOG = LoggerFactory.getLogger(SpilledReplayIterator.class);



	//The queues to contain buffers	which are asynchronously read
	private ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyBuffersPerEpoch;

	//The cursor indicating the consumers position in the log
	private EpochCursor cursor;

	public SpilledReplayIterator(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
								 BufferPool partitionBufferPool,
								 IOManager ioManager, Object subpartitionLock) {
		LOG.debug("SpilledReplayIterator created");
		this.cursor = new EpochCursor(logToReplay);

		readyBuffersPerEpoch = new ConcurrentHashMap<>(logToReplay.keySet().size());
		//Initialize the queues
		for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
			LinkedBlockingDeque<Buffer> queue = new LinkedBlockingDeque<>();
			readyBuffersPerEpoch.put(entry.getKey(), queue);
		}

		//Start the producer
		Thread producer = new Thread(new ProducerRunnable(ioManager, logToReplay, partitionBufferPool,
			readyBuffersPerEpoch, subpartitionLock));
		producer.start();
	}

	@Override
	public int numberRemaining() {
		return cursor.getRemaining();
	}

	@Override
	public long getEpoch() {
		return cursor.getCurrentEpoch();
	}

	@Override
	public Buffer next() {
		LOG.debug("Fetching buffer, cursor: {}", cursor);
		try {

			SpillableSubpartitionInFlightLogger.BufferHandle bufferHandle = cursor.getCurrentBufferHandle();
			Buffer buffer = bufferHandle.getBuffer();
			if (!bufferHandle.isAvailableInMemory())//Producer will increase refCnt if flush not complete when processed
				buffer = readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).take();

			cursor.next();
			return buffer;
		} catch (InterruptedException e) {
			throw new RuntimeException("Error while getting next: " + e.getMessage());
		}
	}

	@Override
	public Buffer peekNext() {
		try {
			LOG.debug("Call to peek. Cursor {}", cursor);
			SpillableSubpartitionInFlightLogger.BufferHandle bufferHandle = cursor.getCurrentBufferHandle();
			Buffer buffer = bufferHandle.getBuffer();
			if (!bufferHandle.isAvailableInMemory()) {//Producer will increase refCnt if flush not complete when processed
				buffer = readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).take();
				//After peeking push it back
				readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).putFirst(buffer);
			}
			return buffer;
		} catch (InterruptedException e) {
			throw new RuntimeException("Error while peeking: " + e.getMessage());
		}
	}

	@Override
	public boolean hasNext() {
		return !cursor.reachedEnd();
	}


	private static class ProducerRunnable implements Runnable {
		//The position in the log of the producer
		private final EpochCursor cursor;
		private final Object subpartitionLock;

		private BufferPool bufferPool;
		private Map<Long, BufferFileReader> epochReaders;

		public ProducerRunnable(
			IOManager ioManager,
			SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
			BufferPool bufferPool,
			ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyDataBuffersPerEpoch,
			Object subpartitionLock) {

			this.subpartitionLock = subpartitionLock;
			this.bufferPool = bufferPool;
			this.cursor = new EpochCursor(logToReplay);
			this.epochReaders = new HashMap<>(logToReplay.keySet().size());
			for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
				try {
					epochReaders.put(entry.getKey(), ioManager.createBufferFileReader(entry.getValue().getFileHandle()
						, new ReadCompletedCallback(readyDataBuffersPerEpoch.get(entry.getKey()))));
				} catch (Exception e) {
					throw new RuntimeException("Error during recovery, could not create epoch readers: " + e.getMessage());
				}
			}
		}

		@Override
		public void run() {
			LOG.debug("Replay Producer thread starting");
			if (cursor.getRemaining() == 0)
				return;
			try {
				synchronized (subpartitionLock){
				BufferFileReader reader;
				while (!cursor.reachedEnd()) {
					reader = epochReaders.get(cursor.getCurrentEpoch());
					SpillableSubpartitionInFlightLogger.BufferHandle storedBuffer =
						cursor.getCurrentBufferHandle();
					if (storedBuffer.isAvailableInMemory()) {
						LOG.debug("Buffer for cursor {}, is in memory", cursor);
						storedBuffer.getBuffer().retainBuffer();
					}else {
						LOG.debug("Buffer for cursor {}, is on disk", cursor);
						reader.readInto(bufferPool.requestBufferBlocking());
					}

					cursor.next();
				}
			}
			} catch (Exception e) {
				throw new RuntimeException("An error occurred during recovery: " + e.getMessage());
			}
		}
	}

	private static class ReadCompletedCallback implements RequestDoneCallback<Buffer> {

		private final LinkedBlockingDeque<Buffer> readyDataBuffers;

		public ReadCompletedCallback(LinkedBlockingDeque<Buffer> readyDataBuffers) {
			this.readyDataBuffers = readyDataBuffers;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			try {
				readyDataBuffers.put(request);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			String msg = "Read of buffer failed during replay with error: " + e.getMessage();
			LOG.debug("Error: " + msg);
			throw new RuntimeException(msg);
		}
	}

	private static class EpochCursor {

		private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log;
		private long currentEpoch;
		private int epochOffset; //The next buffer the reader will request

		private long lastEpoch;
		private int remaining;

		public EpochCursor(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log) {
			this.currentEpoch = log.firstKey();
			this.epochOffset = 0;
			this.lastEpoch = log.lastKey();
			this.remaining =
				log.values().stream().mapToInt(SpillableSubpartitionInFlightLogger.Epoch::getEpochSize).sum();
			this.log = log;
		}

		public void next() {
			epochOffset++;
			remaining--;
			if (reachedEndOfEpoch(epochOffset, currentEpoch))
				if (currentEpoch != lastEpoch) {
					currentEpoch++;
					epochOffset = 0;
				}
		}

		public boolean reachedEnd() {
			return remaining == 0;
		}

		private boolean reachedEndOfEpoch(int offset, long epochID) {
			return offset >= log.get(epochID).getEpochSize();
		}

		public long getCurrentEpoch() {
			return currentEpoch;
		}

		public int getEpochOffset() {
			return epochOffset;
		}

		public int getRemaining() {
			return remaining;
		}

		public SpillableSubpartitionInFlightLogger.BufferHandle getCurrentBufferHandle() {
			return log.get(currentEpoch).getEpochBuffers().get(epochOffset);
		}

		@Override
		public String toString() {
			return "EpochCursor{" +
				"currentEpoch=" + currentEpoch +
				", epochOffset=" + epochOffset +
				", remaining=" + remaining +
				'}';
		}
	}


}
