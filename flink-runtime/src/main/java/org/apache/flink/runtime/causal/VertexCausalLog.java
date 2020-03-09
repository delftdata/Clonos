package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.state.CheckpointListener;

/**
 * Used to log a single upstream (or the current task's) task's determinants.
 * Is responsible for garbage collection of determinants which have been checkpointed or sent to all downstream tasks.
 * It is responsible for remembering what determinants it has sent to which downstream tasks.
 */
public interface VertexCausalLog extends CheckpointListener {

	byte[] getDeterminants();


	void appendDeterminants(byte[] determinants);

	byte[] getNextDeterminantsForDownstream(VertexId vertexId);

	void notifyCheckpointBarrier(long checkpointId);

	void notifyDownstreamFailure(VertexId vertexId);
}
