package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class OneToNLineageAttachingOutput<OUT> extends AbstractLineageAttachingOutput<OUT> {

	private int counter;
	private RecordID inputBase;
	private RecordID outputResult;


	public OneToNLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
		this.counter = 0;
		this.outputResult = new RecordID();
	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		counter = 0;
		this.inputBase = input.getRecordID();
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
		hashFunction.hashInt(inputBase.hashCode() + counter).writeBytesTo(outputResult.getId(), 0, RecordID.NUMBER_OF_BYTES);
		return outputResult;
	}
}
