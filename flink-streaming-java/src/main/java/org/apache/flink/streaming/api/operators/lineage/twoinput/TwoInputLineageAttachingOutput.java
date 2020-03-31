package org.apache.flink.streaming.api.operators.lineage.twoinput;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public interface TwoInputLineageAttachingOutput {
	/**
	 * Inform the LineageAttachingOutput that the operator has started processing a new input record.
	 */
	void notifyInputRecord1(StreamRecord<?> input);

	/**
	 * Inform the LineageAttachingOutput that the operator has started processing a new input record.
	 */
	void notifyInputRecord2(StreamRecord<?> input);

}
