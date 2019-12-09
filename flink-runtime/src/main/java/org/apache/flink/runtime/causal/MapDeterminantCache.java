package org.apache.flink.runtime.causal;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapDeterminantCache implements DeterminantCache {
	private Map<String, DeterminantLog> determinantLogs;

	public MapDeterminantCache(List<String> upstreamOperatorIds, List<String> downStreamOperatorIds) {
		this.determinantLogs = new HashMap<>();
		for (String u : upstreamOperatorIds)
			determinantLogs.put(u, new CircularDeterminantLog(downStreamOperatorIds));

	}

	@Override
	public List<DeterminantLogDelta> getDeterminants() {
		List<DeterminantLogDelta> results = new LinkedList<>();
		for (String key : this.determinantLogs.keySet()) {
			results.add(new DeterminantLogDelta(key, determinantLogs.get(key).getDeterminants()));
		}
		return results;
	}

	@Override
	public void appendDeterminantsToOperatorLog(String operatorId, byte[] determinants) {
		this.determinantLogs.get(operatorId).appendDeterminants(determinants);
	}

	@Override
	public void notifyCheckpointBarrier(String checkpointId) {
		this.determinantLogs.values().forEach(determinantLog -> determinantLog.notifyCheckpointBarrier(checkpointId));

	}

	@Override
	public void notifyCheckpointComplete(String checkpointId) {
		this.determinantLogs.values().forEach(determinantLog -> determinantLog.notifyCheckpointComplete(checkpointId));
	}

	@Override
	public List<DeterminantLogDelta> getNextDeterminantsForDownstream(String operatorId) {
		List<DeterminantLogDelta> results = new LinkedList<>();
		for (String key : this.determinantLogs.keySet()) {
			results.add(new DeterminantLogDelta(key, determinantLogs.get(key).getNextDeterminantsForDownstream(operatorId)));
		}
		return results;
	}
}
