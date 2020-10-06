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

import java.io.Serializable;

/*
A compact ID to represent a job specific (sub)task. Needs to be compact because it is sent on StreamElements. Task-level
because standby failover is done at task level.

Flink has no SubTask specific IDs. It has only JobVertexIDs + subtask index.
Flink's AbstractID has two issues. 1. it is statistical, 2. it is too large. Compressing that large space into a short
increases the probability of collisions.
 */
public class VertexID implements Serializable {

	private static final long serialVersionUID = 1L;

	short id;

	public VertexID(short taskID) {
		this.id = taskID;
	}


	public short getVertexID() {
		return id;
	}

	public void setVertexId(short id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		return id;
	}

	@Override
	public String toString() {
		return "VertexId{" +
			"id=" + id +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		VertexID vertexId = (VertexID) o;
		return id == vertexId.id;
	}
}
