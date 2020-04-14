package org.apache.flink.runtime.causal;

import java.util.List;

public interface DeterminantCarrier {

	void enrich(List<VertexCausalLogDelta> logDeltaList);

	List<VertexCausalLogDelta> getLogDeltas();

}
