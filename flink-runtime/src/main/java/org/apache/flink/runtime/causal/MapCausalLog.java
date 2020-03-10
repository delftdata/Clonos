package org.apache.flink.runtime.causal;

import java.util.*;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class MapCausalLog implements CausalLog {
	private Map<VertexId, VertexCausalLog> determinantLogs;
	private VertexId myVertexId;

	public MapCausalLog(VertexId myVertexId, Collection<VertexId> upstreamVertexIds, int numDownstreamChannels) {
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;
		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularVertexCausalLog(numDownstreamChannels));
		determinantLogs.put(this.myVertexId, new CircularVertexCausalLog(numDownstreamChannels));
	}

	@Override
	public List<VertexCausalLogDelta> getDeterminants() {
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new VertexCausalLogDelta(key, determinantLogs.get(key).getDeterminants()));
		}
		results.add(new VertexCausalLogDelta(this.myVertexId, determinantLogs.get(this.myVertexId).getDeterminants()));
		return results;
	}

	@Override
	public void appendDeterminantsToVertexLog(VertexId vertexId, byte[] determinants) {
		this.determinantLogs.get(vertexId).appendDeterminants(determinants);
	}

	@Override
	public void addDeterminant(byte[] determinants) {
		this.determinantLogs.get(this.myVertexId).appendDeterminants(determinants);
	}

	@Override
	public List<VertexCausalLogDelta> getNextDeterminantsForDownstream(int channel) {
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			byte[] newDet = determinantLogs.get(key).getNextDeterminantsForDownstream(channel);
			if (newDet.length != 0)
				results.add(new VertexCausalLogDelta(key, newDet));
		}

		byte[] newDet = determinantLogs.get(this.myVertexId).getNextDeterminantsForDownstream(channel);
		if (newDet.length != 0)
			results.add(new VertexCausalLogDelta(this.myVertexId, newDet));
		return results;
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointBarrier(checkpointId);
	}

	@Override
	public void notifyDownstreamFailure(int channel) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyDownstreamFailure(channel);
	}

	@Override
	public byte[] getDeterminantsOfUpstream(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointComplete(checkpointId);
	}
}
