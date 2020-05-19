/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal;

import java.util.Arrays;
import java.util.Objects;

/**
 * Contains an update on the state of a causal log. Contains the new determinants (the delta) added to the log since the last update.
 */
public class CausalLogDelta {
	/**
	 * The {@link VertexId} of the vertex that this delta refers to
	 */
	VertexId vertexId;
	/**
	 * The determinants themselves
	 */
	byte[] rawDeterminants;

	/**
	 * The offset from the current epoch.
	 */
	int offsetFromEpoch;

	public CausalLogDelta(VertexId vertexId, byte[] rawDeterminants, int offsetFromEpoch) {
		this.rawDeterminants = rawDeterminants;
		this.vertexId = vertexId;
		this.offsetFromEpoch = offsetFromEpoch;
	}


	public VertexId getVertexId() {
		return vertexId;
	}

	public byte[] getRawDeterminants() {
		return rawDeterminants;
	}

	public int getOffsetFromEpoch() {
		return offsetFromEpoch;
	}

	@Override
	public String toString() {
		return "VertexCausalLogDelta{" +
			"vertexId=" + vertexId +
			", rawDeterminants=" + Arrays.toString(rawDeterminants) +
			", offsetFromEpoch=" + offsetFromEpoch +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CausalLogDelta that = (CausalLogDelta) o;
		return getOffsetFromEpoch() == that.getOffsetFromEpoch() &&
			getVertexId().equals(that.getVertexId()) &&
			Arrays.equals(getRawDeterminants(), that.getRawDeterminants());
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(getVertexId(), getOffsetFromEpoch());
		result = 31 * result + Arrays.hashCode(getRawDeterminants());
		return result;
	}
}
