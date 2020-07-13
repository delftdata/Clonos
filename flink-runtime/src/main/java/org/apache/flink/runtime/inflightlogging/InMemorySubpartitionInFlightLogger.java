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
package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class InMemorySubpartitionInFlightLogger implements InFlightLog {

	private static final Logger LOG = LoggerFactory.getLogger(InMemorySubpartitionInFlightLogger.class);

	private final SortedMap<Long, List<Buffer>> slicedLog;

	public InMemorySubpartitionInFlightLogger() {
		slicedLog = new TreeMap<>();
	}

	public synchronized void log(Buffer buffer, long epochID) {
		List<Buffer> epochLog = slicedLog.computeIfAbsent(epochID, k -> new LinkedList<>());
		epochLog.add(buffer.retainBuffer());
		LOG.debug("Logged a new buffer for epoch {}", epochID);
	}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {

		LOG.debug("Got notified of checkpoint {} completion\nCurrent log: {}", checkpointId, representLogAsString(this.slicedLog));
		List<Long> toRemove = new LinkedList<>();

		//keys are in ascending order
		for (long epochId : slicedLog.keySet()) {
			if (epochId < checkpointId) {
				toRemove.add(epochId);
				LOG.debug("Removing epoch {}", epochId);
			}
		}

		for (long checkpointBarrierId : toRemove) {
			List<Buffer> slice = slicedLog.remove(checkpointBarrierId);
			for (Buffer b : slice) {
				b.recycleBuffer();
			}
		}
	}

	@Override
	public synchronized InFlightLogIterator<Buffer> getInFlightIterator(long startEpochID) {
		//The lower network stack recycles buffers, so for each replay, we must increase reference counts
		increaseReferenceCountsUnsafe(startEpochID);
		return new ReplayIterator(startEpochID, slicedLog);
	}

	private void increaseReferenceCountsUnsafe(Long epochID) {
		for (List<Buffer> epoch : slicedLog.tailMap(epochID).values())
			for (Buffer buffer : epoch)
				buffer.retainBuffer();
	}

	public static class ReplayIterator extends InFlightLogIterator<Buffer> {
		private long startKey;
		private long currentKey;
		private ListIterator<Buffer> currentIterator;
		private SortedMap<Long, List<Buffer>> logToReplay;
		private int numberOfBuffersLeft;

		public ReplayIterator(long epochToStartFrom, SortedMap<Long, List<Buffer>> fullLog) {

			//Failed at checkpoint x, so we replay starting at epoch x
			this.startKey = epochToStartFrom;
			this.currentKey = epochToStartFrom;
			this.logToReplay = fullLog.tailMap(epochToStartFrom);
			LOG.info(" Getting iterator starting  from epochID {} with log state {} and sublog state {}", currentKey, representLogAsString(fullLog), representLogAsString(this.logToReplay));
			if (this.logToReplay.get(currentKey) != null) {
				this.currentIterator = this.logToReplay.get(currentKey).listIterator();
				this.numberOfBuffersLeft = this.logToReplay.values().stream().mapToInt(List::size).sum(); //add up the sizes
			} else {
				this.currentIterator = null;
				this.numberOfBuffersLeft = 0;
			}
			LOG.info("State of log: {}\nlog tailmap {}\nIterator creation {}: ", representLogAsString(fullLog), representLogAsString(this.logToReplay), this.toString());
		}

		private void advanceToNextNonEmptyIteratorIfNeeded() {
			while (currentIterator != null &&!currentIterator.hasNext() && currentKey < logToReplay.lastKey()) {
				this.currentIterator = logToReplay.get(++currentKey).listIterator();
				while (currentIterator.hasPrevious()) currentIterator.previous();
			}
		}

		private void advanceToPreviousNonEmptyIteratorIfNeeded() {
			while (currentIterator != null && !currentIterator.hasPrevious() && currentKey > logToReplay.firstKey()) {
				this.currentIterator = logToReplay.get(--currentKey).listIterator();
				while (currentIterator.hasNext()) currentIterator.next(); //fast forward the iterator
			}
		}

		@Override
		public boolean hasNext() {
			advanceToNextNonEmptyIteratorIfNeeded();
			return currentIterator != null && currentIterator.hasNext();
		}

		@Override
		public Buffer next() {
			advanceToNextNonEmptyIteratorIfNeeded();
			Buffer toReturn = currentIterator.next();
			numberOfBuffersLeft--;
			return toReturn;
		}

		@Override
		public Buffer peekNext() {
			advanceToNextNonEmptyIteratorIfNeeded();
			Buffer toReturn = currentIterator.next();
			currentIterator.previous();
			return toReturn;
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
		public String toString() {
			return "ReplayIterator{" +

				"startKey=" + startKey +
				", currentKey=" + currentKey +
				", currentIterator=" + currentIterator +
				", logToReplay=" + representLogAsString(logToReplay) +
				", numberOfBuffersLeft=" + numberOfBuffersLeft +
				", reduceCount=" + logToReplay.values().stream().mapToInt(List::size).sum() +
				", lists=" + logToReplay.values().stream().map(l -> "[" + l.stream().map(x -> "*").collect(Collectors.joining(", ")) + "]").collect(Collectors.joining("; ")) +
				'}';
		}

	}

	private static String representLogAsString(SortedMap<Long, List<Buffer>> toStringify) {
		return "{" + toStringify.entrySet().stream().map(e -> e.getKey() + " -> " + "[" + e.getValue().stream().map(x -> "*").collect(Collectors.joining(", ")) + "]").collect(Collectors.joining(", ")) + "}";
	}

}
