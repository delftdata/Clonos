package org.apache.flink.runtime.causal;

import java.util.List;

/**
 * A CausalLog contains the determinant logs of all upstream operators and itself.
 */
public interface CausalLog {


	List<VertexCausalLogDelta> getDeterminants();

	/*
	Forwards the request to the Vertex specific causal log
	 */
	void appendDeterminantsToVertexLog(VertexId vertexId, byte[] determinants);

	/*
	Returns a list of deltas containing the updates that have since been obtained for all upstream vertexes or this vertex.
	 */
	List<VertexCausalLogDelta> getNextDeterminantsForDownstream(VertexId vertexId);

	void notifyCheckpointBarrier(long checkpointId);

	void notifyDownstreamFailure(VertexId vertexId);

	byte[] getDeterminantsOfUpstream(VertexId vertexId);

}
