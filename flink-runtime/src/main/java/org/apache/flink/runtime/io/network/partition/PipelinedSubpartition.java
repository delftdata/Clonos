/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.determinant.BufferBuiltDeterminant;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.inflightlogging.*;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 */
public class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/**
	 * The read view to consume this subpartition.
	 */
	private PipelinedSubpartitionView readView;

	/**
	 * Flag indicating whether the subpartition has been finished.
	 */
	private boolean isFinished;

	@GuardedBy("buffers")
	private boolean flushRequested;

	/**
	 * Flag indicating whether the subpartition has been released.
	 */
	private volatile boolean isReleased;
	// ------------------------------------------------------------------------

	private InFlightLog inFlightLog;
	private IJobCausalLog causalLoggingManager;
	private IRecoveryManager recoveryManager;

	private long nextCheckpointId;

	/**
	 * Access is also guarded by buffers lock
	 * -1 represents not a checkpoint buffer
	 */
	@GuardedBy("buffers")
	private Deque<Long> checkpointIds;

	private AtomicBoolean downstreamFailed;

	@GuardedBy("buffers")
	private InFlightLogIterator<Buffer> inflightReplayIterator;

	@GuardedBy("buffers")
	private Deque<BufferConsumer> determinantRequests;

	private long currentEpochID;

	private AtomicBoolean isRecoveringSubpartitionInFlightState;

	private BufferBuiltDeterminant reuseBufferBuiltDeterminant;

	PipelinedSubpartition(int index, ResultPartition parent) {
		this(index, parent, null);
	}

	PipelinedSubpartition(int index, ResultPartition parent, IOManager ioManager) {
		super(index, parent);

		if (ioManager == null)
			this.inFlightLog = new InMemorySubpartitionInFlightLogger();
		else
			this.inFlightLog = new SpillableSubpartitionInFlightLogger(ioManager, this, InFlightLogConfig.eagerPolicy);

		this.checkpointIds = new LinkedList<>();
		this.nextCheckpointId = -1L;
		this.downstreamFailed = new AtomicBoolean(false);
		this.currentEpochID = 0L;
		this.isRecoveringSubpartitionInFlightState = new AtomicBoolean(false);
		this.determinantRequests = new LinkedList<>();
		this.reuseBufferBuiltDeterminant = new BufferBuiltDeterminant();
	}

	public void setIsRecoveringSubpartitionInFlightState(boolean isRecoveringSubpartitionInFlightState) {
		LOG.info("Set isRecoveringSubpartitionInFlightState to {}", isRecoveringSubpartitionInFlightState);
		this.isRecoveringSubpartitionInFlightState.set(isRecoveringSubpartitionInFlightState);
	}

	public void setStartingEpoch(long currentEpochID) {
		this.currentEpochID = currentEpochID;
	}

	public void setRecoveryManager(IRecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
	}

	public void setCausalLoggingManager(IJobCausalLog causalLoggingManager) {
		this.causalLoggingManager = causalLoggingManager;
	}

	public void notifyCheckpointBarrier(long checkpointId) {
		LOG.info("PipelinedSubpartition notified of checkpoint {} barrier", checkpointId);
		this.nextCheckpointId = checkpointId;
	}

	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("PipelinedSubpartition notified of checkpoint {} completion", checkpointId);
		this.inFlightLog.notifyCheckpointComplete(checkpointId);
	}


	@Override
	public boolean add(BufferConsumer bufferConsumer) {
		return add(bufferConsumer, false);
	}

	@Override
	public void flush() {
		synchronized (buffers) {
			if (buffers.isEmpty()) {
				return;
			}
			flushRequested = !buffers.isEmpty();
			if (recoveryManager.isRunning() && !isRecoveringSubpartitionInFlightState.get())
				notifyDataAvailable();
		}
	}

	public void bypassDeterminantRequest(BufferConsumer bufferConsumer) {
		LOG.info("Trying to acquire lock to add determinantRequest");
		synchronized (buffers) {
			LOG.info("Acquired lock to Add determinantRequest buffer consumer");
			determinantRequests.add(bufferConsumer);
			flushRequested = true;
			notifyDataAvailable();
		}
	}

	@Override
	public void finish() throws IOException {
		add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true);
		LOG.info("Finished {}.", this);
	}

	private boolean add(BufferConsumer bufferConsumer, boolean finish) {
		checkNotNull(bufferConsumer);

		synchronized (buffers) {
			if (isFinished || isReleased) {
				bufferConsumer.close();
				return false;
			}

			// Add the bufferConsumer and update the stats
			buffers.add(bufferConsumer);
			updateStatistics(bufferConsumer);
			increaseBuffersInBacklog(bufferConsumer);

			checkpointIds.add(nextCheckpointId);
			nextCheckpointId = -1;


			if (finish) {
				isFinished = true;
				flush();
			} else {
				if (recoveryManager.isRunning() && !isRecoveringSubpartitionInFlightState.get())
					maybeNotifyDataAvailable();
			}
		}

		return true;
	}

	@Override
	public void release() {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			for (BufferConsumer buffer : buffers) {
				buffer.close();
			}
			buffers.clear();
			checkpointIds.clear();


			view = readView;
			readView = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;
		}

		LOG.info("Released {}.", this);

		if (view != null) {
			view.releaseAllResources();
		}
	}

	public void sendFailConsumerTrigger(Throwable cause) {
		LOG.info("Sending fail consumer trigger. Setting downstream failed to true");
		downstreamFailed.set(true);
		parent.sendFailConsumerTrigger(index, cause);
	}

	@Nullable
	BufferAndBacklog pollBuffer() {

		LOG.debug("Call to pollBuffer");

		if (downstreamFailed.get()) {
			LOG.info("Polling for next buffer, but downstream is still failed.");
			return null;
		}

		LOG.debug("Attempt to obtain buffers lock to check for determinant requests");
		synchronized (buffers) {
			LOG.debug("Obtained buffers lock to check for determinant requests");
			if (!determinantRequests.isEmpty()) {
				LOG.info("We have a determinant request to send");
				BufferConsumer consumer = determinantRequests.poll();
				Buffer buffer = consumer.build();
				consumer.close();
				return new BufferAndBacklog(buffer, true, 0, false);
			}
		}

		if (isRecoveringSubpartitionInFlightState.get()) {
			LOG.info("We are still recovering this subpartition, cannot return a buffer yet.");
			return null;
		}

		if (!recoveryManager.isRunning()) {
			LOG.info("Recovery hasnt finished for this subpartition");
			return null;

		}

		synchronized (buffers) {
			if (inflightReplayIterator != null) {
				LOG.info("We are replaying, get inflight logs next buffer");
				return getReplayedBufferUnsafe();
			} else {
				return getBufferFromQueuedBufferConsumersUnsafe();
			}
		}

	}

	private BufferAndBacklog getReplayedBufferUnsafe() {
			Buffer buffer = inflightReplayIterator.next();

			long epoch = inflightReplayIterator.getEpoch();

			if (!inflightReplayIterator.hasNext()) {
				inflightReplayIterator = null;
				LOG.info("Finished replaying inflight log!");
			}

			return new BufferAndBacklog(buffer,
				inflightReplayIterator != null || isAvailableUnsafe(),
				getBuffersInBacklog() + (inflightReplayIterator != null ? inflightReplayIterator.numberRemaining() :
					0),
				_recoveryNextBufferIsEvent(), epoch);
	}

	private boolean _recoveryNextBufferIsEvent() {
		boolean isNextAnEvent;
		if (inflightReplayIterator != null && inflightReplayIterator.hasNext())
			isNextAnEvent = !inflightReplayIterator.peekNext().isBuffer();
		else
			isNextAnEvent = _nextBufferIsEvent();
		return isNextAnEvent;
	}

	private BufferAndBacklog getBufferFromQueuedBufferConsumersUnsafe() {
			Buffer buffer = null;
			long checkpointId = 0;

			if (buffers.isEmpty()) {
				flushRequested = false;
			}

			if (buffers.isEmpty())
				LOG.info("Call to getBufferFromQueued, but no buffer consumers to close");
			while (!buffers.isEmpty()) {
				BufferConsumer bufferConsumer = buffers.peek();
				checkpointId = checkpointIds.peek();


				buffer = bufferConsumer.build();


				checkState(bufferConsumer.isFinished() || buffers.size() == 1,
					"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the " +
						"buffers queue.");

				if (buffers.size() == 1) {
					// turn off flushRequested flag if we drained all of the available data
					flushRequested = false;
				}

				if (bufferConsumer.isFinished()) {
					buffers.pop().close();
					checkpointIds.pop();
					decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
				}

				if (buffer.readableBytes() > 0) {
					break;
				}

				buffer.recycleBuffer();
				buffer = null;
				if (!bufferConsumer.isFinished()) {
					break;
				}
			}

			if (buffer == null) {
				return null;
			}

			causalLoggingManager.appendSubpartitionDeterminant(reuseBufferBuiltDeterminant.replace(buffer.readableBytes()), currentEpochID, this.parent.getPartitionId().getPartitionId(), this.index);

			updateStatistics(buffer);
			inFlightLog.log(buffer, currentEpochID);
			LOG.debug("Creating BufferAndBacklog with epochID {}", currentEpochID);
			BufferAndBacklog result = new BufferAndBacklog(buffer, isAvailableUnsafe(), getBuffersInBacklog(),
				_nextBufferIsEvent(), currentEpochID);
			//We do this after the determinant and sending the BufferAndBacklog because a checkpoint x belongs to
			// epoch x-1
			if (checkpointId != -1L)
				currentEpochID = checkpointId;
			// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
			// It will be reported for reading either on flush or when the number of buffers in the queue
			// will be 2 or more.
			LOG.info("{}:{}: Polled buffer {} (hash: {}, memorySegment hash: {}). Buffers available for dispatch: {}."
				, parent, this, buffer, System.identityHashCode(buffer),
				System.identityHashCode(buffer.getMemorySegment()), getBuffersInBacklog());
			return result;
	}


	boolean nextBufferIsEvent() {
		synchronized (buffers) {
			return _nextBufferIsEvent();
		}
	}

	private boolean _nextBufferIsEvent() {
		assert Thread.holdsLock(buffers);

		return !buffers.isEmpty() && !buffers.peekFirst().isBuffer();
	}

	@Override
	public int releaseMemory() {
		// The pipelined subpartition does not react to memory release requests.
		// The buffers will be recycled by the consuming task.
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (buffers) {
			checkState(!isReleased);

			if (readView == null) {
				LOG.info("Creating read view for {} (index: {}) of partition {}.", this, index,
					parent.getPartitionId());

				readView = new PipelinedSubpartitionView(this, availabilityListener);
			} else {
				readView.setAvailabilityListener(availabilityListener);
				LOG.info("(Re)using read view {} for {} (index: {}) of partition {}.", readView, this, index,
					parent.getPartitionId());
			}


		}
		//If we are recovering, when we conclude, we must notify of data availability.
		if (recoveryManager == null || recoveryManager.isRunning()) {
			notifyDataAvailable();
		} else {
			recoveryManager.notifyNewOutputChannel(parent.getPartitionId().getPartitionId(), index);

		}

		return readView;
	}

	public boolean isAvailable() {
		synchronized (buffers) {
			return isAvailableUnsafe();
		}
	}

	private boolean isAvailableUnsafe() {
		return flushRequested || getNumberOfFinishedBuffers() > 0;
	}

	// ------------------------------------------------------------------------

	int getCurrentNumberOfBuffers() {
		return buffers.size();
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedSubpartition %d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished?" +
				" " +
				"%s, read view? %s]",
			index, numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	public void requestReplay(long checkpointId, int ignoreMessages) {
		synchronized (buffers) {
			if (inflightReplayIterator != null)
				inflightReplayIterator.close();
			inflightReplayIterator = inFlightLog.getInFlightIterator(checkpointId, ignoreMessages);
			if (inflightReplayIterator != null) {
				LOG.info("Replay has been requested for pipelined subpartition of id {}, index {}, skipping {} buffers, " +
						"buffers to replay {}. Setting downstreamFailed to false", this.parent.getPartitionId(), this.index,
					ignoreMessages, inflightReplayIterator.numberRemaining());
				if (!inflightReplayIterator.hasNext())
					inflightReplayIterator = null;
			}
			downstreamFailed.set(false);
		}
	}

	private void maybeNotifyDataAvailable() {
		// Notify only when we added first finished buffer.
		if (getNumberOfFinishedBuffers() == 1) {
			notifyDataAvailable();
		}
	}

	public void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}

	private int getNumberOfFinishedBuffers() {
		assert Thread.holdsLock(buffers);

		if (buffers.size() == 1 && buffers.peekLast().isFinished()) {
			return 1;
		}

		// We assume that only last buffer is not finished.
		return Math.max(0, buffers.size() - 1);
	}

	public void buildAndLogBuffer(int bufferSize) {
		LOG.info("building buffer and discarding result");
		while (true) {
			synchronized (buffers) {
				LOG.info("Acquired buffers lock to build and discard");
				if (!buffers.isEmpty()) {

					BufferConsumer consumer = buffers.peek();

					if (consumer.isFinished()) {
						if (consumer.getUnreadBytes() > 0 && consumer.getUnreadBytes() < bufferSize) {
							String msg = "Size of finished buffer (" + consumer.getUnreadBytes() + ") does not match" +
								" " +
								"size of recovery request to build buffer ( " + bufferSize + " ).";
							LOG.info("Exception:" + msg);
							throw new RuntimeException(msg);
						} else {
							buffers.pop().close();
							checkpointIds.pop();
							continue;
						}
					}
					//If there is enough data in consumer for building the correct buffer
					if (consumer.getUnreadBytes() >= bufferSize) {
						LOG.info("There are enough bytes to build the requested buffer!");
						long checkpointId = checkpointIds.peek();

						//This assumes that the input buffers which are before this close in the determinant log have
						// been
						// fully processed, thus the bufferconsumer will have this amount of data.
						causalLoggingManager.appendSubpartitionDeterminant(
							reuseBufferBuiltDeterminant.replace(bufferSize), currentEpochID,
							this.parent.getPartitionId().getPartitionId(), this.index);
						Buffer buffer = consumer.build(bufferSize);


						checkState(consumer.isFinished() || buffers.size() == 1,
							"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of" +
								" " +
								"the buffers queue.");

						if (buffers.size() == 1) {
							// turn off flushRequested flag if we drained all of the available data
							flushRequested = false;
						}

						if (consumer.isFinished()) {
							buffers.pop().close();
							checkpointIds.pop();
						}

						if (buffer.readableBytes() == 0)
							throw new RuntimeException("Requested to rebuild buffer with 0 bytes.");

						updateStatistics(buffer);
						inFlightLog.log(buffer, currentEpochID);
						buffer.recycleBuffer(); //It is not sent downstream, so we must recycle it here.
						//We do this after the determinant because a checkpoint x belongs to epoch x-1
						if (checkpointId != -1)
							currentEpochID = checkpointId;
						break;
					}
				}
			}
			try {
				Thread.sleep(1l);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOG.info("Done building and discarding");

	}
}
