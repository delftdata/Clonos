/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a local subpartition.
 */
public class LocalInputChannel extends InputChannel implements BufferAvailabilityListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	// ------------------------------------------------------------------------

	private final Object requestLock = new Object();

	/** The local partition manager. */
	private final ResultPartitionManager partitionManager;

	/** Task event dispatcher for backwards events. */
	private final TaskEventDispatcher taskEventDispatcher;

	/** The consumed subpartition. */
	private volatile ResultSubpartitionView subpartitionView;

	private volatile boolean isReleased;

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventDispatcher taskEventDispatcher,
		TaskIOMetricGroup metrics) {

		this(inputGate, channelIndex, partitionId, partitionManager, taskEventDispatcher,
			0, 0, metrics);
	}

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventDispatcher taskEventDispatcher,
		int initialBackoff,
		int maxBackoff,
		TaskIOMetricGroup metrics) {

		super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, metrics.getNumBytesInLocalCounter(), metrics.getNumBuffersInLocalCounter());

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {

		boolean retriggerRequest = false;

		// The lock is required to request only once in the presence of retriggered requests.
		synchronized (requestLock) {
			checkState(!isReleased, "LocalInputChannel has been released already");

			if (subpartitionView == null) {
				LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
					this, subpartitionIndex, partitionId);

				try {
					ResultSubpartitionView subpartitionView = partitionManager.createSubpartitionView(
						partitionId, subpartitionIndex, this);

					if (subpartitionView == null) {
						throw new IOException("Error requesting subpartition.");
					}

					// make the subpartition view visible
					this.subpartitionView = subpartitionView;

					// check if the channel was released in the meantime
					if (isReleased) {
						subpartitionView.releaseAllResources();
						this.subpartitionView = null;
					}
				} catch (PartitionNotFoundException notFound) {
					if (increaseBackoff()) {
						retriggerRequest = true;
					} else {
						throw notFound;
					}
				}
			}
		}

		// Do this outside of the lock scope as this might lead to a
		// deadlock with a concurrent release of the channel via the
		// input gate.
		if (retriggerRequest) {
			inputGate.retriggerPartitionRequest(partitionId.getPartitionId());
		}
	}

	/**
	 * Retriggers a subpartition request.
	 */
	void retriggerSubpartitionRequest(Timer timer, final int subpartitionIndex) {
		synchronized (requestLock) {
			checkState(subpartitionView == null, "already requested partition");

			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						requestSubpartition(subpartitionIndex);
					} catch (Throwable t) {
						setError(t);
					}
				}
			}, getCurrentBackoff());
		}
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
		checkError();

		ResultSubpartitionView subpartitionView = this.subpartitionView;
		if (subpartitionView == null) {
			// There is a possible race condition between writing a EndOfPartitionEvent (1) and flushing (3) the Local
			// channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush notification (4). When
			// they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue LocalInputChannel after (or
			// during) it was released during reading the EndOfPartitionEvent (2).
			if (isReleased) {
				LOG.debug("{} is released. getNextBuffer() returns empty.", this);
				return Optional.empty();
			}

			// this can happen if the request for the partition was triggered asynchronously
			// by the time trigger
			// would be good to avoid that, by guaranteeing that the requestPartition() and
			// getNextBuffer() always come from the same thread
			// we could do that by letting the timer insert a special "requesting channel" into the input gate's queue
			subpartitionView = checkAndWaitForSubpartitionView();
		}

		BufferAndBacklog next = subpartitionView.getNextBuffer();

		if (next == null) {
			if (subpartitionView.isReleased()) {
				throw new CancelTaskException("Consumed partition " + subpartitionView + " has been released.");
			} else {
				LOG.debug("{} getNextBuffer() returns empty.", this);
				return Optional.empty();
			}
		}

		numBytesIn.inc(next.buffer().getSizeUnsafe());
		numBuffersIn.inc();
		numBuffersRemoved++;
		numBuffersDeduplicate++;
		return Optional.of(new BufferAndAvailability(next.buffer(), next.isMoreAvailable(), next.buffersInBacklog()));
	}

	@Override
	public void notifyDataAvailable() {
		notifyChannelNonEmpty();
	}

	private ResultSubpartitionView checkAndWaitForSubpartitionView() {
		// synchronizing on the request lock means this blocks until the asynchronous request
		// for the partition view has been completed
		// by then the subpartition view is visible or the channel is released
		synchronized (requestLock) {
			checkState(!isReleased, "released");
			checkState(subpartitionView != null, "Queried for a buffer before requesting the subpartition.");
			return subpartitionView;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		LOG.debug("Send task event {} from channel {}.", event, this);
		checkError();
		checkState(subpartitionView != null || event instanceof InFlightLogRequestEvent, "Tried to send task event to producer before requesting the subpartition.");

		if (!taskEventDispatcher.publish(partitionId, event)) {
			throw new IOException("Error while publishing event " + event + " to producer. The producer could not be found.");
		}
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased;
	}

	@Override
	void notifySubpartitionConsumed() throws IOException {
		if (subpartitionView != null) {
			subpartitionView.notifySubpartitionConsumed();
		}
	}

	/**
	 * Releases the partition reader.
	 */
	@Override
	void releaseAllResources() throws CancelTaskException, IOException {
		ResultSubpartitionView view = subpartitionView;
		try {
			if (!isReleased) {
				isReleased = true;

				if (view != null) {
					subpartitionView = null;
					checkError();
					// If releasing because of error don't release subpartition view.
					view.releaseAllResources();
				}
			}
		} catch (IOException e) {
			// RunStandbyTaskStrategy
			LOG.debug("Release (because of error) all resources of channel {} (but not {}).", this, view);
		}
	}

	@Override
	public int getResetNumberBuffersRemoved() {
		LOG.info("Get and reset {} buffers in {} to truncate upstream inflight log.", numBuffersRemoved, this);
		int nbr = numBuffersRemoved;
		numBuffersRemoved = 0;
		return nbr;
	}

	@Override
	public void resetNumberBuffersDeduplicate() {
		LOG.info("NoOP: Reset {} buffers for deduplication in {}.", numBuffersDeduplicate, this);
		numBuffersDeduplicate = 0;
	}

	@Override
	public int getNumberBuffersDeduplicate() {
		LOG.info("NoOP: Get {} buffers for deduplication in {}.", numBuffersDeduplicate, this);
		return numBuffersDeduplicate;
	}

	// NoOP because LocalInputChannel will have lost all state after a failure
	@Override
	public void setNumberBuffersDeduplicate(int nbd) {}

	// Ditto
	@Override
	public void setDeduplicating() {}

	@Override
	public String toString() {
		return "LocalInputChannel " + channelIndex + " [" + partitionId + "]";
	}

	// ------------------------------------------------------------------------
	// Reincarnation to a local input channel at runtime
	// ------------------------------------------------------------------------

	public LocalInputChannel toNewLocalInputChannel(ResultPartitionID newPartitionId,
			ResultPartitionManager partitionManager, TaskEventDispatcher taskEventDispatcher,
			int initialBackoff, int maxBackoff, TaskIOMetricGroup metrics) throws IOException {
		return new LocalInputChannel(inputGate, channelIndex, newPartitionId,
				partitionManager, taskEventDispatcher, initialBackoff, maxBackoff, metrics);
	}

	public RemoteInputChannel toNewRemoteInputChannel(ResultPartitionID newPartitionId,
			ConnectionID newProducerAddress, ConnectionManager connectionManager,
			int initialBackoff, int maxBackoff, TaskIOMetricGroup metrics) throws IOException {
		releaseAllResources();
		RemoteInputChannel newRemoteInputChannel = new RemoteInputChannel(inputGate, channelIndex, newPartitionId,
				checkNotNull(newProducerAddress), connectionManager, initialBackoff,
				maxBackoff, metrics);
		if (inputGate.isCreditBased()) {
			inputGate.assignExclusiveSegments((InputChannel) newRemoteInputChannel);
		}
		return newRemoteInputChannel;
	}
}
