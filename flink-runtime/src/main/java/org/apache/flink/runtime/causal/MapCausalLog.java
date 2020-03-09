package org.apache.flink.runtime.causal;

import java.util.*;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class MapCausalLog implements CausalLog {
	private Map<VertexId, VertexCausalLog> determinantLogs;

	public MapCausalLog(Collection<VertexId> upstreamVertexIds, Collection<VertexId> downStreamVertexIds) {
		this.determinantLogs = new HashMap<>();
		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularVertexCausalLog(downStreamVertexIds));

	}

	@Override
	public List<VertexCausalLogDelta> getDeterminants() {
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new VertexCausalLogDelta(key, determinantLogs.get(key).getDeterminants()));
		}
		return results;
	}

	@Override
	public void appendDeterminantsToVertexLog(VertexId vertexId, byte[] determinants) {
		this.determinantLogs.get(vertexId).appendDeterminants(determinants);
	}

	@Override
	public List<VertexCausalLogDelta> getNextDeterminantsForDownstream(VertexId vertexId) {
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new VertexCausalLogDelta(key, determinantLogs.get(key).getNextDeterminantsForDownstream(vertexId)));
		}
		return results;
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointBarrier(checkpointId);
	}

	@Override
	public void notifyDownstreamFailure(VertexId vertexId) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyDownstreamFailure(vertexId);
	}

	@Override
	public byte[] getDeterminantsOfUpstream(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}
}
