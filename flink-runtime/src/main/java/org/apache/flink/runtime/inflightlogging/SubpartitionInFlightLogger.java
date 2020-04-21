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
package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SubpartitionInFlightLogger implements InFlightLog {

	private static final Logger LOG = LoggerFactory.getLogger(SubpartitionInFlightLogger.class);

	private SortedMap<Long, List<Buffer>> slicedLog;





	public SubpartitionInFlightLogger() {
		clearLog();
	}


	public void log(Buffer buffer) {
		getCurrentSlice().add(buffer.retainBuffer());
	}

	@Override
	public void logCheckpointBarrier(Buffer buffer, long checkpointId) {
		getCurrentSlice().add(buffer.retainBuffer());
		slicedLog.put(checkpointId, new LinkedList<>());
	}

	private List<Buffer> getCurrentSlice() {
		return slicedLog.get(slicedLog.lastKey());
	}

	@Override
	public void clearLog() {
		slicedLog = new TreeMap<>();

		//Perhaps we should use array lists, initialized to the size of the previous epoch.
		slicedLog.put(0l, new LinkedList<>());
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {

		List<Long> toRemove = new LinkedList<>();

		//keys are in ascending order
		for (long checkpointBarrierId : slicedLog.keySet()) {
			if (checkpointBarrierId < completedCheckpointId) {
				toRemove.add(checkpointBarrierId);
			}
		}

		for (long checkpointBarrierId : toRemove) {
			slicedLog.remove(checkpointBarrierId);
		}
	}

	@Override
	public SizedListIterator<Buffer> getInFlightFromCheckpoint(long checkpointId) {
		return new ReplayIterator(checkpointId, slicedLog);
	}

	public static class ReplayIterator implements SizedListIterator<Buffer> {
		long currentKey;
		ListIterator<Buffer> currentIterator;
		SortedMap<Long, List<Buffer>> logToReplay;
		int numberOfBuffersLeft;

		public ReplayIterator(long lastCompletedCheckpointOfFailed, SortedMap<Long, List<Buffer>> logToReplay) {
			//Failed at checkpoint x, so we replay starting at epoch x
			this.currentKey = lastCompletedCheckpointOfFailed;
			this.logToReplay = logToReplay.tailMap(lastCompletedCheckpointOfFailed);
			this.currentIterator = logToReplay.get(currentKey).listIterator();
			numberOfBuffersLeft = logToReplay.values()
				.stream().mapToInt(List::size).sum(); //add up the sizes of each slice
		}

		private void advanceToNextNonEmptyIteratorIfNeeded(){
			while(!currentIterator.hasNext() && currentKey < logToReplay.lastKey())
				this.currentIterator = logToReplay.get(++currentKey).listIterator();
		}

		private void advanceToPreviousNonEmptyIteratorIfNeeded(){
			while(!currentIterator.hasPrevious() && currentKey > logToReplay.firstKey())
				this.currentIterator = logToReplay.get(--currentKey).listIterator();
		}

		@Override
		public boolean hasNext() {
			advanceToNextNonEmptyIteratorIfNeeded();
			return currentIterator.hasNext();
		}

		@Override
		public boolean hasPrevious() {
			advanceToPreviousNonEmptyIteratorIfNeeded();
			return currentIterator.hasPrevious();
		}

		@Override
		public Buffer next() {
			advanceToNextNonEmptyIteratorIfNeeded();
			Buffer toReturn = currentIterator.next();
			numberOfBuffersLeft--;
			return toReturn;
		}

		@Override
		public Buffer previous() {
			advanceToPreviousNonEmptyIteratorIfNeeded();
			Buffer toReturn = currentIterator.previous();
			numberOfBuffersLeft++;
			return toReturn;
		}

		@Override
		public int nextIndex() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int previousIndex() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void set(Buffer buffer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void add(Buffer buffer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int numberRemaining() {
			return numberOfBuffersLeft;
		}
	}

}
