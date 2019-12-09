package org.apache.flink.runtime.causal;

/**
 * The cache used to log a single operators determinants.
 * Is responsible for garbage collection of determinants which have been checkpointed or sent to all downstream operators.
 * It is responsible for remembering what determinants it has sent to which downstream operators.
 */
public interface DeterminantLog {

	byte[] getDeterminants();

	void appendDeterminants(byte[] determinants);

	void notifyCheckpointBarrier(String checkpointId);

	void notifyCheckpointComplete(String checkpointId);

	byte[] getNextDeterminantsForDownstream(String operatorId);

}
