package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.causal.VertexId;
import org.apache.flink.runtime.event.AbstractEvent;

import java.io.IOException;

public class DeterminantRequestEvent extends AbstractEvent {

	private VertexId failedVertex;

	public DeterminantRequestEvent(VertexId failedVertex) {
		this.failedVertex = failedVertex;
	}

	public DeterminantRequestEvent() {
	}

	public VertexId getFailedVertex() {
		return failedVertex;
	}

	public void setFailedVertex(VertexId failedVertex) {
		this.failedVertex = failedVertex;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(this.failedVertex.getVertexId());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.setFailedVertex(new VertexId(in.readShort()));
	}
}
