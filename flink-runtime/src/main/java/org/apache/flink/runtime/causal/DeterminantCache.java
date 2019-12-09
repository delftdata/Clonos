package org.apache.flink.runtime.causal;

import java.util.List;

/**
 * A DeterminantCache is contains the determinant logs of all upstream operators and itself.
 */
public interface DeterminantCache {

	List<DeterminantLogDelta> getDeterminants();

	void appendDeterminantsToOperatorLog(String operatorId, byte[] determinants);

	void notifyCheckpointBarrier(String checkpointId);

	void notifyCheckpointComplete(String checkpointId);

	List<DeterminantLogDelta> getNextDeterminantsForDownstream(String operatorId);

	public class DeterminantLogDelta {
		String operatorId;
		byte[] logDelta;

		public DeterminantLogDelta(String operatorId, byte[] logDelta) {
			this.operatorId = operatorId;
			this.logDelta = logDelta;
		}


		public String getOperatorId() {
			return operatorId;
		}

		public void setOperatorId(String operatorId) {
			this.operatorId = operatorId;
		}

		public byte[] getLogDelta() {
			return logDelta;
		}

		public void setLogDelta(byte[] logDelta) {
			this.logDelta = logDelta;
		}
	}

}
