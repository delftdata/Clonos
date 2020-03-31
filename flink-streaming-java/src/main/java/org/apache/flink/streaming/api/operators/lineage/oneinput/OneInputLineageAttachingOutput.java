package org.apache.flink.streaming.api.operators.lineage.oneinput;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public interface OneInputLineageAttachingOutput {
	/**
	 * Inform the LineageAttachingOutput that the operator has started processing a new input record.
	 */
	void notifyInputRecord(StreamRecord<?> input);

}
