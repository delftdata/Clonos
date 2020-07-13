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

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SpilledReplayIterator extends InFlightLogIterator<Buffer> {
	private static final Logger LOG = LoggerFactory.getLogger(SpilledReplayIterator.class);

	private static final int NUM_BUFFERS_TO_USE_IN_REPLAY = 20;

	private long currentKey;
	private int numberOfBuffersLeft;

	private ArrayBlockingQueue<Buffer> readyDataBuffers;

	private SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;

	private int consumerEpochOffset; //The next buffer the reader will request


	public SpilledReplayIterator(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> slicedLog,
								 long epochToStartFrom, BufferPool partitionBufferPool,
								 IOManager ioManager, Object flushLock) {

		LOG.info("SpilledReplayIterator created");
		this.currentKey = epochToStartFrom;
		this.logToReplay = slicedLog.tailMap(epochToStartFrom);

		this.consumerEpochOffset = 0;
		this.readyDataBuffers = new ArrayBlockingQueue<>(NUM_BUFFERS_TO_USE_IN_REPLAY);
		this.numberOfBuffersLeft = logToReplay.values().stream()
			.map(SpillableSubpartitionInFlightLogger.Epoch::getEpochBuffers).mapToInt(List::size).sum();

		Thread producer = new Thread(new ProducerRunnable(ioManager, slicedLog, partitionBufferPool, readyDataBuffers,
			epochToStartFrom, flushLock));
		producer.start();
	}

	@Override
	public int numberRemaining() {
		return numberOfBuffersLeft;
	}

	@Override
	public long getEpoch() {
		return currentKey;
	}

	@Override
	public Buffer next() {
		LOG.info("Fetching buffer {} of epoch {}", consumerEpochOffset ,currentKey);
		numberOfBuffersLeft--;
		if(consumerEpochOffset > logToReplay.get(currentKey).getEpochBuffers().size()){
			consumerEpochOffset = 0;
			currentKey++;
			LOG.info("Moving on to epoch {}", currentKey);
		}
		consumerEpochOffset++;
		try {
			LOG.info("Attempt to fetch buffer from readyDataBuffers");
			Buffer toRet = readyDataBuffers.take();
			LOG.info("Buffer fetched from readyDataBuffers successfully");
			return toRet;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Buffer peekNext() {
		LOG.info("Call to peek");
		while(readyDataBuffers.peek() == null);
		LOG.info("Peek available");
		return readyDataBuffers.peek();
	}

	@Override
	public boolean hasNext() {
		return numberOfBuffersLeft > 0;
	}


	private static class ProducerRunnable implements Runnable {
		private ReadCompletedCallback CALLBACK;//todo

		private final IOManager ioManager;
		private ArrayBlockingQueue<Buffer> readyDataBuffers;
		private BufferPool bufferPool;
		private SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;

		private BufferFileReader currentBufferFileReader;
		private long producerKey;
		private int producerEpochOffset; //The next buffer the producer will produce

		private final Object flushLock;
		private AtomicInteger openReadRequests;

		public ProducerRunnable(
			IOManager ioManager,
			SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
			BufferPool bufferPool,
			ArrayBlockingQueue<Buffer> readyDataBuffers,
			long epochToStartFrom,
			Object flushLock) {

			this.openReadRequests = new AtomicInteger(0);
			this.CALLBACK = new ReadCompletedCallback(readyDataBuffers,openReadRequests);
			this.ioManager = ioManager;
			this.logToReplay = logToReplay;
			this.bufferPool = bufferPool;
			this.readyDataBuffers = readyDataBuffers;
			this.flushLock = flushLock;

			this.producerKey = epochToStartFrom;
			this.producerEpochOffset = 0;


			try {
				this.currentBufferFileReader = ioManager.createBufferFileReader(logToReplay.get(producerKey).getFileHandle(), CALLBACK);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			LOG.info("Replay Producer thread starting");
			int numberOfBuffersToReplay = logToReplay.values().stream().map(SpillableSubpartitionInFlightLogger.Epoch::getEpochBuffers).mapToInt(List::size).sum();
			if(numberOfBuffersToReplay == 0)
				return;
			try {
				synchronized (flushLock) {
					LOG.info("Replay Producer thread acquired flush lock");
					while (producerKey <= logToReplay.lastKey()) {
						LOG.info("Producer will now create async requests for epoch {}", producerKey);
						SpillableSubpartitionInFlightLogger.Epoch epoch = logToReplay.get(producerKey);

						while (producerEpochOffset < epoch.getEpochBuffers().size()) {
							Buffer storedBuffer = epoch.getEpochBuffers().get(producerEpochOffset);
							if (storedBuffer == null) {
								LOG.info("Create read request for buffer {} of epoch {} ", producerEpochOffset, producerKey);
								openReadRequests.incrementAndGet();
								currentBufferFileReader.readInto(bufferPool.requestBufferBlocking());
							}else {
								LOG.info("Directly get buffer {} of epoch {} ", producerEpochOffset, producerKey);
								//tODO we really shouldnt do this, slows down recovery
								while(openReadRequests.get() != 0){Thread.sleep(1);}; //We need to wait for all async requests to finish first
								readyDataBuffers.put(storedBuffer);
							}
							producerEpochOffset++;
						}

						//tODO we really shouldnt do this, slows down recovery
						while(openReadRequests.get() != 0){Thread.sleep(1);}; //We need to wait for all async requests to finish first
						producerKey++;
						producerEpochOffset = 0;
						currentBufferFileReader = ioManager.createBufferFileReader(epoch.getFileHandle(), CALLBACK);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static class ReadCompletedCallback implements RequestDoneCallback<Buffer> {

		private final ArrayBlockingQueue<Buffer> readyDataBuffers;
		private final AtomicInteger openReadRequests;

		public ReadCompletedCallback(ArrayBlockingQueue<Buffer> readyDataBuffers, AtomicInteger openReadRequests){
			this.readyDataBuffers = readyDataBuffers;
			this.openReadRequests = openReadRequests;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			LOG.info("Disk read completed");
			try {
				readyDataBuffers.put(request);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG.info("Was able to push buffer to readyDataBuffers");
			openReadRequests.decrementAndGet();
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			throw new RuntimeException("Read of buffer failed during replay with error: " + e.getMessage());
		}
	}

}
