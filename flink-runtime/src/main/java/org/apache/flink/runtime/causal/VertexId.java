package org.apache.flink.runtime.causal;

/*
A compact ID to represent a job specific (sub)task. Needs to be compact because it is sent on StreamElements. Task-level
because standby failover is done at task level.

Flink has no SubTask specific IDs. It has only JobVertexIDs + subtask index.
Flink's AbstractID has two issues. 1. it is statistical, 2. it is too large. Compressing that large space into a short
increases the probability of collisions.
 */
public class VertexId {
	short id;

	public VertexId(short taskID) {
		this.id = taskID;
	}


	public short getVertexId() {
		return id;
	}

	public void setVertexId(short id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		return id;
	}
}
