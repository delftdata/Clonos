package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockLineageAttachingOutput<OUT> extends AbstractLineageAttachingOutput<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(MockLineageAttachingOutput.class);

	private static final String outputLine = "Call to MockLineageAttachingOutput.{}. If you see this, you likely need to provide an implementation for some operator you are using.";


	public MockLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		LOG.info(outputLine, "notifyInputRecord");
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		LOG.info(outputLine, "initializeState");

	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		LOG.info(outputLine, "snapshotState");
	}

	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		LOG.info(outputLine, "getRecordIDForNextOutputRecord");
		return new RecordID();
	}
}
