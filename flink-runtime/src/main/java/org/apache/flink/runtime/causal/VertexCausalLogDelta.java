package org.apache.flink.runtime.causal;

public class VertexCausalLogDelta {
	VertexId vertexId;
	byte[] logDelta;

	public VertexCausalLogDelta(VertexId vertexId, byte[] logDelta) {
		this.logDelta = logDelta;
		this.vertexId = vertexId;
	}

	public VertexId getVertexId() {
		return vertexId;
	}

	public void setVertexId(VertexId vertexId) {
		this.vertexId = vertexId;
	}

	public byte[] getLogDelta() {
		return logDelta;
	}

	public void setLogDelta(byte[] logDelta) {
		this.logDelta = logDelta;
	}
}
