package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

import java.io.IOException;

public interface OperatorChainOutput<OUT> extends OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<OUT>> {
	public void emitStreamStatus(StreamStatus streamStatus);

	public void broadcastEvent(AbstractEvent event) throws IOException;

	public void flush() throws IOException;

}
