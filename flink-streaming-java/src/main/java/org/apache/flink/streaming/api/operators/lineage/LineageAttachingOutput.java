package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An {@link Output} that attaches lineage-based {@link org.apache.flink.streaming.runtime.streamrecord.RecordID} to the records it emits.
 * Each implementation has different requirements. Some may need to persist some state, and thus must provide implementations for initializeState and snapshotState.
 *
 * @param <OUT> The type of Outputted record.
 */
public interface LineageAttachingOutput<OUT> extends Output<StreamRecord<OUT>> {

	/**
	 * Inform the LineageAttachingOutput that the operator has started processing a new input record.
	 */
	void notifyInputRecord(StreamRecord<?> input);

	/**
	 * Initialize the state of this LineageAttachingOutput. If recovering loads the previous state.
	 *
	 * @param context
	 * @throws Exception
	 */
	void initializeState(StateInitializationContext context) throws Exception;

	/**
	 * Take a snapshot of the state. Ensure all state is persisted in the managed state.
	 *
	 * @param context
	 * @throws Exception
	 */
	void snapshotState(StateSnapshotContext context) throws Exception;


}
