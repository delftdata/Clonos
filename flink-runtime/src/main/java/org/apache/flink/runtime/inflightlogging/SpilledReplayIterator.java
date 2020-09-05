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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * {@link SpilledReplayIterator} is to be used in combination with {@link SpillableSubpartitionInFlightLogger}.
 * The {@link SpillableSubpartitionInFlightLogger} spills the in-flight log to disk asynchronously, while this
 * {@link InFlightLogIterator} implementation is able to then read those files and regenerate those buffers.
 * This is done deterministically and buffers have the exact same size.
 * <p>
 * To achieve this behaviour we split the Iterator into a producer and a consumer. The producer will first lock
 * the <code>subpartitionLock</code>, preventing any in-memory buffers to be spilled. Then it uses all buffers
 * available in
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
	private EpochCursor consumerCursor;

	private EpochCursor producerCursor;

	//Used to signal back to flusher thread that we are done replaying and that it may resume flushing
	private final AtomicBoolean isReplaying;

	public SpilledReplayIterator(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
								 BufferPool recoveryBufferPool,
								 IOManager ioManager, Object subpartitionLock, int ignoreBuffers,
								 AtomicBoolean isReplaying) {
		LOG.debug("SpilledReplayIterator created");
		LOG.debug("State of in-flight log: { {} }",
			logToReplay.entrySet().stream().map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(",")));
		this.consumerCursor = new EpochCursor(logToReplay);
		this.producerCursor = new EpochCursor(logToReplay);
		this.isReplaying = isReplaying;

		//skip ignoreBuffers buffers
		for (int i = 0; i < ignoreBuffers; i++) {
			consumerCursor.next();
			producerCursor.next();
		}


		readyBuffersPerEpoch = new ConcurrentHashMap<>(logToReplay.keySet().size());
		//Initialize the queues
		for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
			LinkedBlockingDeque<Buffer> queue = new LinkedBlockingDeque<>();
			readyBuffersPerEpoch.put(entry.getKey(), queue);
		}

		//Start the producer
		Thread producer = new Thread(new ProducerRunnable(ioManager, logToReplay, recoveryBufferPool,
			readyBuffersPerEpoch, subpartitionLock, producerCursor));
		producer.start();
	}

	@Override
	public int numberRemaining() {
		return consumerCursor.getRemaining();
	}

	@Override
	public long getEpoch() {
		return consumerCursor.getCurrentEpoch();
	}

	@Override
	public Buffer next() {
		Buffer buffer = null;
		while (!consumerCursor.behind(producerCursor)) ;
		try {

			buffer = consumerCursor.getCurrentBuffer();

			if (buffer.asByteBuf().refCnt() == 0)
				buffer = readyBuffersPerEpoch.get(consumerCursor.getCurrentEpoch()).take();

			consumerCursor.next();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close();
		}

		if (!consumerCursor.hasNext())
			isReplaying.set(false);

		LOG.debug("Fetching buffer for cursor: {}, buffer: {}", consumerCursor, buffer);
		return buffer;
	}

	@Override
	public Buffer peekNext() {
		Buffer buffer = null;
		while (!consumerCursor.behind(producerCursor)) ;
		try {
			LOG.debug("Call to peek. Cursor {}", consumerCursor);
			buffer = consumerCursor.getCurrentBuffer();
			if (buffer.asByteBuf().refCnt() == 0) {//Producer will increase refCnt if flush not complete when
				// processed
				buffer = readyBuffersPerEpoch.get(consumerCursor.getCurrentEpoch()).take();
				//After peeking push it back
				readyBuffersPerEpoch.get(consumerCursor.getCurrentEpoch()).putFirst(buffer);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close(); //cleanup
		}
		return buffer;
	}

	@Override
	public void close() {
		//Note, there may be a better way to do this if a new iterator is going to be built. We could avoid recycling
		//buffers we will need
		try {
			while (consumerCursor.hasNext()) {
				while (!consumerCursor.behind(producerCursor)) ;
				Buffer buffer = consumerCursor.getCurrentBuffer();
				if (buffer.asByteBuf().refCnt() == 0)
					buffer = readyBuffersPerEpoch.get(consumerCursor.getCurrentEpoch()).take();
				buffer.recycleBuffer();
				consumerCursor.next();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close();
		}
	}

	@Override
	public boolean hasNext() {
		return consumerCursor.hasNext();
	}


	private static class ProducerRunnable implements Runnable {
		//The position in the log of the producer
		private final EpochCursor producerCursor;
		private final Object subpartitionLock;
		private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;

		private BufferPool recoveryBufferPool;
		private Map<Long, BufferFileReader> epochReaders;

		public ProducerRunnable(
			IOManager ioManager,
			SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
			BufferPool recoveryBufferPool,
			ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyDataBuffersPerEpoch,
			Object subpartitionLock, EpochCursor producerCursor) {
			this.logToReplay = logToReplay;
			this.subpartitionLock = subpartitionLock;
			this.recoveryBufferPool = recoveryBufferPool;
			this.producerCursor = producerCursor;

			this.epochReaders = new HashMap<>(logToReplay.keySet().size());
			for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
				try {
					epochReaders.put(entry.getKey(), ioManager.createBufferFileReader(entry.getValue().getFileHandle()
						, new ReadCompletedCallback(readyDataBuffersPerEpoch.get(entry.getKey()))));
				} catch (Exception e) {
					logAndThrowAsRuntimeException(e);
				}
			}
		}

		@Override
		public void run() {
			LOG.debug("Replay Producer thread starting");
			try {
				while (producerCursor.hasNext()) {
					synchronized (subpartitionLock) {
						BufferFileReader reader;
						while (producerCursor.hasNext()) {
							reader = epochReaders.get(producerCursor.getCurrentEpoch());
							Buffer buffer = producerCursor.getCurrentBuffer();
							if (buffer.asByteBuf().refCnt() != 0) {
								LOG.debug("Buffer for cursor {}, is in memory", producerCursor);
								buffer.retainBuffer();
							} else {
								LOG.debug("Buffer for cursor {}, is on disk. Buffer pool state: {}", producerCursor,
									recoveryBufferPool);
								Buffer bufferToReadInto = recoveryBufferPool.requestBuffer();
								if (bufferToReadInto == null) //no buffer available, release lock and try again
									break;
								reader.readInto(bufferToReadInto);
							}

							producerCursor.next();
						}
					}
					Thread.sleep(5);
				}

				//close will wait for all requests to complete before closing
				for (BufferFileReader r : epochReaders.values())
					r.close();
			} catch (Exception e) {
				logAndThrowAsRuntimeException(e);
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
			logAndThrowAsRuntimeException(e);
		}
	}

	private static class EpochCursor {

		private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log;
		private AtomicLong currentEpoch;
		private AtomicInteger epochOffset; //The next buffer the reader will request

		private long lastEpoch;
		private int remaining;

		public EpochCursor(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log) {
			this.currentEpoch = new AtomicLong(log.firstKey());
			this.epochOffset = new AtomicInteger(0);
			this.lastEpoch = log.lastKey();
			this.remaining =
				log.values().stream().mapToInt(SpillableSubpartitionInFlightLogger.Epoch::getEpochSize).sum();
			this.log = log;
		}

		public void next() {
			epochOffset.incrementAndGet();
			remaining--;
			if (reachedEndOfEpoch(epochOffset.get(), currentEpoch.get()))
				if (currentEpoch.get() != lastEpoch) {
					currentEpoch.incrementAndGet();
					epochOffset.set(0);
				}
		}

		public boolean hasNext() {
			return remaining != 0;
		}

		private boolean reachedEndOfEpoch(int offset, long epochID) {
			return offset >= log.get(epochID).getEpochSize();
		}

		public long getCurrentEpoch() {
			return currentEpoch.get();
		}

		public int getEpochOffset() {
			return epochOffset.get();
		}

		public int getRemaining() {
			return remaining;
		}

		public Buffer getCurrentBuffer() {
			SpillableSubpartitionInFlightLogger.Epoch epoch = log.get(currentEpoch.get());
			List<Buffer> buffers = epoch.getEpochBuffers();
			return buffers.get(epochOffset.get());
		}

		public boolean behind(EpochCursor other) {
			return this.getCurrentEpoch() < other.getCurrentEpoch() ||
				(this.getCurrentEpoch() == other.getCurrentEpoch() && this.getEpochOffset() < other.getEpochOffset());
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

	private static void logAndThrowAsRuntimeException(Exception e) {
		LOG.error("Error in SpilledReplayIterator", e);
		throw new RuntimeException(e);
	}

}
