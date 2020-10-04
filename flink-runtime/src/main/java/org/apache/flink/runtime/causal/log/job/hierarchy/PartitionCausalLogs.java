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

public class PartitionCausalLogs {
	public final IntermediateResultPartitionID intermediateResultPartitionID;

	public ConcurrentMap<Byte, ThreadCausalLog> subpartitionLogs;

	public PartitionCausalLogs(IntermediateResultPartitionID intermediateResultPartitionID) {
		this.intermediateResultPartitionID = intermediateResultPartitionID;
		subpartitionLogs = new ConcurrentHashMap<>(10);
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	public ConcurrentMap<Byte, ThreadCausalLog> getSubpartitionLogs() {
		return subpartitionLogs;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PartitionCausalLogs that = (PartitionCausalLogs) o;
		return getIntermediateResultPartitionID().equals(that.getIntermediateResultPartitionID());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getIntermediateResultPartitionID());
	}
}
