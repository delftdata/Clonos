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

import org.apache.flink.runtime.io.disk.iomanager.*;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * An inflight logger that periodically flushed available buffers according to a policy.
 * Due to limitations of the asynchronous file api, we must flush each epoch sequentially. Otherwise we may accidentally
 * free a buffer that has not been written yet. This is because each file is output to by a different thread.
 * <p>
 * We start a different file for each epoch so that it may be deleted upon completion.
 */
public class SpillableSubpartitionInFlightLogger implements InFlightLog {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableSubpartitionInFlightLogger.class);

	private final SortedMap<Long, Epoch> slicedLog;
	private final IOManager ioManager;

	private final Object flushLock = new Object();
	private final Consumer<SpillableSubpartitionInFlightLogger> flushPolicy;
	private final BufferPool recoveryBufferPool;

	private final AtomicBoolean isReplaying;

	public SpillableSubpartitionInFlightLogger(IOManager ioManager, BufferPool recoveryBufferPool){
		this(ioManager, recoveryBufferPool, null);
	}

	public SpillableSubpartitionInFlightLogger(IOManager ioManager, BufferPool recoveryBufferPool,
											   Consumer<SpillableSubpartitionInFlightLogger> flushPolicy) {
		this.ioManager = ioManager;
		this.recoveryBufferPool = recoveryBufferPool;

		this.slicedLog = new TreeMap<>();
		this.isReplaying = new AtomicBoolean(false);
		this.flushPolicy = flushPolicy;
	}


	@Override
	public void log(Buffer buffer, long epochID) {
		synchronized (flushLock) {
			Epoch epoch = slicedLog.computeIfAbsent(epochID, k -> new Epoch(createNewWriter(k), k));
			epoch.append(buffer);
			if (flushPolicy != null && !isReplaying.get())
				flushPolicy.accept(this);
		}
		LOG.debug("Logged a new buffer for epoch {} with refcnt {} and size {}", epochID, buffer.asByteBuf().refCnt(),
			buffer.getSize());
	}

	@Override
	public void notifyCheckpointComplete(long checkpointID) throws Exception {
		LOG.debug("Got notified of checkpoint {} completion", checkpointID);
		List<Long> toRemove = new LinkedList<>();
		List<Epoch> epochsRemoved = new LinkedList<>();

		synchronized (flushLock) {
			//keys are in ascending order
			for (long epochID : slicedLog.keySet())
				if (epochID < checkpointID)
					toRemove.add(epochID);

			for (long epochID : toRemove)
				epochsRemoved.add(slicedLog.remove(epochID));
		}

		for (Epoch epoch : epochsRemoved)
			epoch.removeEpochFile();
	}

	@Override
	public InFlightLogIterator<Buffer> getInFlightIterator(long epochID, int ignoreBuffers) {
		SortedMap<Long, Epoch> logToReplay;
		this.isReplaying.set(true);
		synchronized (flushLock) {
			logToReplay = slicedLog.tailMap(epochID);
			if (logToReplay.size() == 0)
				return null;
		}

		return new SpilledReplayIterator(logToReplay, recoveryBufferPool, ioManager, flushLock, ignoreBuffers,
			isReplaying);
	}

	@Override
	public void destroyBufferPools() {
		recoveryBufferPool.lazyDestroy();
	}

	@Override
	public void close() {
		synchronized (flushLock) {
			for (Epoch e : slicedLog.values())
				e.removeEpochFile();
		}
	}

	public void flushAllUnflushed(){
		if(isReplaying.get())
			return;
		synchronized (flushLock){
			for (Epoch e : slicedLog.values())
				e.flushAllUnflushed();
		}
	}

	public SortedMap<Long, Epoch> getSlicedLog() {
		return slicedLog;
	}

	private void notifyFlushCompleted(long epochID) {
		synchronized (flushLock) {
			Epoch epoch = slicedLog.get(epochID);
			if (epoch != null && !epoch.stable())
				epoch.notifyFlushCompleted();
		}
	}

	private void notifyFlushFailed(long epochID) {
		synchronized (flushLock) {
			Epoch epoch = slicedLog.get(epochID);
			if (epoch != null && !epoch.stable())
				epoch.notifyFlushFailed();
		}
	}



	private BufferFileWriter createNewWriter(long epochID) {
		BufferFileWriter writer = null;
		try {
			writer = ioManager.createBufferFileWriter(ioManager.createChannel(), new FlushCompletedCallback(this,
				epochID));
		} catch (IOException e) {
			throw new RuntimeException("Failed to create BufferFileWriter. Reason: " + e.getMessage());
		}
		return writer;
	}

	static class Epoch {
		private final List<Buffer> epochBuffers;
		private final BufferFileWriter writer;
		private int nextBufferToFlush;
		private int nextBufferToCompleteFlushing;
		private long epochID;


		public Epoch(BufferFileWriter writer, long epochID) {
			this.epochBuffers = new ArrayList<>(500);
			this.writer = writer;
			this.nextBufferToFlush = 0;
			this.nextBufferToCompleteFlushing = 0;
			this.epochID = epochID;
		}

		public void append(Buffer buffer) {
			this.epochBuffers.add(buffer.retainBuffer());
		}

		public List<Buffer> getEpochBuffers() {
			return epochBuffers;
		}

		public FileIOChannel.ID getFileHandle() {
			return writer.getChannelID();
		}

		public long getEpochID() {
			return epochID;
		}

		public void flushAllUnflushed() {
			if (writer.isClosed())
				return;

			try {
				for (; nextBufferToFlush < epochBuffers.size(); nextBufferToFlush++)
					writer.writeBlock(epochBuffers.get(nextBufferToFlush));

			} catch (IOException e) {
				LOG.debug("Attempt to write returned exception due to writer being closed. If writer is closed," +
					"that means epoch is stable, no need to write.");
			}
		}

		public void notifyFlushCompleted() {
			LOG.debug("Notify flush completed");
			Buffer buffer = epochBuffers.get(nextBufferToCompleteFlushing);
			buffer.recycleBuffer();
			nextBufferToCompleteFlushing++;
		}

		public void notifyFlushFailed() {
			//Do nothing and keep in memory
			LOG.debug("Flush failed for buffer {} of epoch {}, keeping in memory", nextBufferToCompleteFlushing,
				epochID);
			nextBufferToCompleteFlushing++;

			//synchronized (writer) {
			//	if (!writer.isClosed()) {
			//		//Must clear request queue, otherwise buffers are stored in wrong order
			//		writer.clearRequestQueue();
			//		//Resubmit requests in order
			//		for (int i = nextBufferToCompleteFlushing; i < nextBufferToFlush; i++) {
			//			try {
			//				writer.writeBlock(epochBuffers.get(i).getBuffer());
			//			} catch (IOException e) {
			//				throw new RuntimeException("Writer could not write buffer. Cause:" + e.getMessage());
			//			}
			//		}
			//	}
			//}
		}

		public boolean stable() {
			return nextBufferToCompleteFlushing == epochBuffers.size() || writer.isClosed();
		}

		public void removeEpochFile() {
			LOG.debug("Removing epoch file of epoch {}", epochID);
			try {
				writer.clearRequestQueue();
				writer.closeAndDelete();
				for (Buffer buffer : epochBuffers) {
					if (buffer.asByteBuf().refCnt() != 0)
						buffer.recycleBuffer(); // release the buffers left over

				}
			} catch (IOException e) {
				throw new RuntimeException("Could not close and delete epoch. Cause: " + e.getMessage());
			}
		}


		public boolean hasNeverBeenFlushed() {
			return nextBufferToFlush == 0;
		}

		public int getEpochSize() {
			return epochBuffers.size();
		}

		@Override
		public String toString() {
			return "Epoch{" +
				"size=" + epochBuffers.size() +
				",nextBufferToFlush=" + nextBufferToFlush +
				", nextBufferToCompleteFlushing=" + nextBufferToCompleteFlushing +
				'}';
		}

	}

	private static class FlushCompletedCallback implements RequestDoneCallback<Buffer> {

		private final SpillableSubpartitionInFlightLogger toNotify;
		private final long epochID;

		public FlushCompletedCallback(SpillableSubpartitionInFlightLogger toNotify, long epochID) {
			this.epochID = epochID;
			this.toNotify = toNotify;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			toNotify.notifyFlushCompleted(epochID);
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			LOG.debug("Flush failed. Retrying. Cause: {}", e.getMessage());
			toNotify.notifyFlushFailed(epochID);
		}
	}

}
