package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;


/**
 * One to one operators (Map, Filter, ...) may simply store the input record's {@link RecordID} and forward it.
 *
 * @param <OUT>
 */
public class OneToOneLineageAttachingOutput<OUT> extends AbstractLineageAttachingOutput<OUT> {

	private RecordID toOutput;

	public OneToOneLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		//Avoid cloning as it wont be changed. Possibly dont even have to attach it, if the same StreamRecord instance is used.
		toOutput = input.getRecordID();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		//skip
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		//skip
	}

	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		return toOutput;
	}
}
