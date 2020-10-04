/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal.log.job.hierarchy;

import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class VertexCausalLogs {
	private final short vertexID;

	public AtomicReference<ThreadCausalLog> mainThreadLog;
	public ConcurrentMap<IntermediateResultPartitionID, PartitionCausalLogs> partitionCausalLogs;

	public VertexCausalLogs(short vertexID) {
		this.vertexID = vertexID;
		this.partitionCausalLogs = new ConcurrentHashMap<>();
		mainThreadLog = new AtomicReference<>();
	}

	public short getVertexID() {
		return vertexID;
	}

	public ThreadCausalLog getMainThreadLog() {
		return mainThreadLog.get();
	}

	public ConcurrentMap<IntermediateResultPartitionID,PartitionCausalLogs> getPartitionCausalLogs() {
		return partitionCausalLogs;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		VertexCausalLogs that = (VertexCausalLogs) o;
		return getVertexID() == that.getVertexID();
	}

	@Override
	public int hashCode() {
		return Objects.hash(getVertexID());
	}
}
