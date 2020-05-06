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

package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

public class VertexGraphInformation {

	private final VertexId thisTasksVertexId;
	private final List<VertexId> upstreamVertexes;
	private final List<VertexId> downstreamVertexes;
	private final int numberOfDirectDownstreamNeighbours;

	public VertexGraphInformation(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID, int subtaskIndex) {

		this.thisTasksVertexId = CausalGraphUtils.computeVertexId(sortedJobVertexes, jobVertexID, subtaskIndex);
		this.upstreamVertexes = CausalGraphUtils.getUpstreamVertexIds(sortedJobVertexes, jobVertexID);
		this.downstreamVertexes = CausalGraphUtils.getDownstreamVertexIds(sortedJobVertexes, jobVertexID);
		this.numberOfDirectDownstreamNeighbours = CausalGraphUtils.getNumberOfDirectDownstreamNeighbours(sortedJobVertexes, jobVertexID);
	}

	public VertexId getThisTasksVertexId() {
		return thisTasksVertexId;
	}

	public List<VertexId> getUpstreamVertexes() {
		return upstreamVertexes;
	}

	public List<VertexId> getDownstreamVertexes() {
		return downstreamVertexes;
	}

	public int getNumberOfDirectDownstreamNeighbours() {
		return numberOfDirectDownstreamNeighbours;
	}

	public boolean hasUpstream(){
		return !this.upstreamVertexes.isEmpty();
	}

	public boolean hasDownstream(){
		return !this.downstreamVertexes.isEmpty();
	}
}
