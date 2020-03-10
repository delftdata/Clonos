package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.List;

/**
 * A CausalLog contains the determinant logs of all upstream operators and itself.
 */
public interface CausalLog extends CheckpointListener {


	List<VertexCausalLogDelta> getDeterminants();

	/*
	Forwards the request to the Vertex specific causal log
	 */
	void appendDeterminantsToVertexLog(VertexId vertexId, byte[] determinants);

	/*
	Appends determinants to this tasks log.
	 */
	void addDeterminant(byte[] determinants);

	/*
	Encodes and appends to this tasks log
	 */
	void addDeterminant(Determinant determinant);

	/*
	Returns a list of deltas containing the updates that have since been obtained for all upstream vertexes or this vertex.
	 */
	List<VertexCausalLogDelta> getNextDeterminantsForDownstream(int channel);

	void notifyCheckpointBarrier(long checkpointId);

	void notifyDownstreamFailure(int channel);

	byte[] getDeterminantsOfUpstream(VertexId vertexId);

}
