/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.SortedMap;
import java.util.TreeMap;

public class SlicedDeduplicator implements CheckpointListener {

	private SortedMap<Long, BloomFilter> checkpointIdToFilter;
	private SortedMap<Long, MemorySegment> checkpointIdToMemorySegment;
	private long currentCheckpoint;

	private static final float ACCEPTABLE_FALSE_POSITIVE_PROBABILITY = 0.0001f;
	private static final int EXPECTED_NUM_ENTRIES = 750000;


	public SlicedDeduplicator() {
		checkpointIdToFilter = new TreeMap<>();
		checkpointIdToMemorySegment = new TreeMap<>();
		createBloomFilter(0l);
	}

	private void createBloomFilter(long checkpointId){
		int bitsSize = BloomFilter.optimalNumOfBits(EXPECTED_NUM_ENTRIES,ACCEPTABLE_FALSE_POSITIVE_PROBABILITY);
		bitsSize = bitsSize + (Long.SIZE - (bitsSize % Long.SIZE));
		int byteSize = bitsSize >>> 3;
		MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
		BloomFilter toUse = new BloomFilter(EXPECTED_NUM_ENTRIES, byteSize);
		toUse.setBitsLocation(memorySegment,0);
		checkpointIdToFilter.put(checkpointId, toUse);
		checkpointIdToMemorySegment.put(checkpointId, memorySegment);
	}

	public boolean testRecord(int hash) {
		boolean found = false;
		for (BloomFilter f : checkpointIdToFilter.values())
			found |= f.testHash(hash);
		if (!found)
			this.checkpointIdToFilter.get(this.currentCheckpoint).addHash(hash);
		return found;
	}


	public void notifyCheckpointBarrier(long checkpointId) {
		if (checkpointId < this.currentCheckpoint)
			return;
		this.currentCheckpoint = checkpointId;
		createBloomFilter(currentCheckpoint);

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		for (Long l : checkpointIdToFilter.keySet()) {
			if (l >= checkpointId)
				break;
			checkpointIdToFilter.remove(l);
			checkpointIdToMemorySegment.remove(l).free();
		}
	}


}
