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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

@NotThreadSafe
public class SpilledReplayIterator extends InFlightLogIterator<Buffer> {
	private static final Logger LOG = LoggerFactory.getLogger(SpilledReplayIterator.class);
	private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;

	private ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyBuffersPerEpoch;
	private EpochCursor cursor;

	public SpilledReplayIterator(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
								 BufferPool partitionBufferPool,
								 IOManager ioManager,
								 Object flushLock) {

		LOG.debug("SpilledReplayIterator created");
		this.logToReplay = logToReplay;
		this.cursor = new EpochCursor(logToReplay);

		readyBuffersPerEpoch = new ConcurrentHashMap<>(logToReplay.keySet().size());

		for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
			LinkedBlockingDeque<Buffer> queue = new LinkedBlockingDeque<>();
			readyBuffersPerEpoch.put(entry.getKey(), queue);
		}

		Thread producer = new Thread(new ProducerRunnable(ioManager, logToReplay, partitionBufferPool,
			readyBuffersPerEpoch, flushLock));
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
			LOG.debug("Attempt to fetch buffer from readyDataBuffers");
			Buffer buffer =
				logToReplay.get(cursor.getCurrentEpoch()).getEpochBuffers().get(cursor.getEpochOffset());
			if (buffer == null)
				buffer = readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).take();
			cursor.next();
			LOG.debug("Buffer fetched from readyDataBuffers successfully");
			return buffer;
		} catch (InterruptedException e) {
			throw new RuntimeException("Error while getting next: " + e.getMessage());
		}
	}

	@Override
	public Buffer peekNext() {
		try {
			LOG.debug("Call to peek. Cursor {}", cursor);
			Buffer storedBuffer =
				logToReplay.get(cursor.getCurrentEpoch()).getEpochBuffers().get(cursor.getEpochOffset());
			if (storedBuffer == null) {
				LOG.debug("Buffer is on disk, waiting until ready");
				storedBuffer = readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).take();
				//After peeking push it back
				LOG.debug("Push it back");
				readyBuffersPerEpoch.get(cursor.getCurrentEpoch()).putFirst(storedBuffer);
				LOG.debug("Done pushing it back");
			}
			return storedBuffer;
		} catch (InterruptedException e) {
			throw new RuntimeException("Error while peeking: " + e.getMessage());
		}
	}

	@Override
	public boolean hasNext() {
		return !cursor.reachedEnd();
	}


	private static class ProducerRunnable implements Runnable {

		private final IOManager ioManager;
		private final EpochCursor cursor;
		private BufferPool bufferPool;
		private SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;
		private ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyDataBuffersPerEpoch;
		private Map<Long, BufferFileReader> epochReaders;
		private final Object flushLock;

		public ProducerRunnable(
			IOManager ioManager,
			SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
			BufferPool bufferPool,
			ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyDataBuffersPerEpoch,
			Object flushLock) {

			this.ioManager = ioManager;
			this.logToReplay = logToReplay;
			this.bufferPool = bufferPool;
			this.flushLock = flushLock;
			this.readyDataBuffersPerEpoch = readyDataBuffersPerEpoch;
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
				synchronized (flushLock) {
					BufferFileReader reader;
					while (!cursor.reachedEnd()) {
						SpillableSubpartitionInFlightLogger.Epoch epoch = logToReplay.get(cursor.getCurrentEpoch());
						reader = epochReaders.get(cursor.getCurrentEpoch());

						Buffer storedBuffer = epoch.getEpochBuffers().get(cursor.getEpochOffset());
						if (storedBuffer == null)
							reader.readInto(bufferPool.requestBufferBlocking());
						 else
							storedBuffer.retainBuffer();

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
			LOG.debug("Disk read completed");
			try {
				readyDataBuffers.put(request);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG.debug("Was able to push buffer to readyDataBuffers");
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			throw new RuntimeException("Read of buffer failed during replay with error: " + e.getMessage());
		}
	}

	private static class EpochCursor {

		private long currentEpoch;
		private int epochOffset; //The next buffer the reader will request

		Map<Long, Integer> epochSizes;
		private long lastEpoch;
		private int remaining;

		public EpochCursor(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log) {
			this.currentEpoch = log.firstKey();
			this.epochOffset = 0;
			this.lastEpoch = log.lastKey();
			this.remaining =
				log.values().stream().mapToInt(SpillableSubpartitionInFlightLogger.Epoch::getEpochSize).sum();
			epochSizes = log.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(),
				e.getValue().getEpochSize())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
			return offset >= epochSizes.get(epochID);
		}

		public long getCurrentEpoch() {
			return currentEpoch;
		}

		public int getEpochOffset() {
			return epochOffset;
		}

		public int getEpochSize() {
			return epochSizes.get(currentEpoch);
		}

		public int getRemaining() {
			return remaining;
		}

		@Override
		public String toString() {
			return "EpochCursor{" +
				"currentEpoch=" + currentEpoch +
				", epochOffset=" + epochOffset +
				", epochSizes={" + epochSizes.entrySet().stream().map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(", ")) +
				"}, remaining=" + remaining +
				'}';
		}
	}


}
