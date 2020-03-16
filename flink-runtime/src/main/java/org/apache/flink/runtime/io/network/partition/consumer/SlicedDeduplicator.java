package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.SortedMap;
import java.util.TreeMap;

public class SlicedDeduplicator implements CheckpointListener {

	private SortedMap<Long, BloomFilter> checkpointIdToFilter;
	private long currentCheckpoint;


	public SlicedDeduplicator() {
		checkpointIdToFilter = new TreeMap<>();
		currentCheckpoint = 0l;
		checkpointIdToFilter.put(currentCheckpoint, new BloomFilter(750000, 1000));
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
		checkpointIdToFilter.put(this.currentCheckpoint, new BloomFilter(750000, 1000));

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		for (Long l : checkpointIdToFilter.keySet()) {
			if (l >= checkpointId)
				break;
			checkpointIdToFilter.remove(l);
		}
	}


}
