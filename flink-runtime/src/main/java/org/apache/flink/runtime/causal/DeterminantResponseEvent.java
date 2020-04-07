package org.apache.flink.runtime.causal;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;

public class DeterminantResponseEvent extends TaskEvent {

	VertexCausalLogDelta vertexCausalLogDelta;

	public DeterminantResponseEvent(VertexCausalLogDelta vertexCausalLogDelta) {
		this.vertexCausalLogDelta = vertexCausalLogDelta;
	}

	public DeterminantResponseEvent() {
	}

	public VertexCausalLogDelta getVertexCausalLogDelta() {
		return vertexCausalLogDelta;
	}

	public void setVertexCausalLogDelta(VertexCausalLogDelta vertexCausalLogDelta) {
		this.vertexCausalLogDelta = vertexCausalLogDelta;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(vertexCausalLogDelta.getVertexId().getVertexId());
		out.write(vertexCausalLogDelta.getLogDelta().length);
		out.write(vertexCausalLogDelta.getLogDelta());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		short id = in.readShort();
		int logDeltaLength = in.readInt();
		byte[] logDelta = new byte[logDeltaLength];
		in.read(logDelta, 0, logDeltaLength);
		this.vertexCausalLogDelta = new VertexCausalLogDelta(new VertexId(id), logDelta, 0);
	}
}
