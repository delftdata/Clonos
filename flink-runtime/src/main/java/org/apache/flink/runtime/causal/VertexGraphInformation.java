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

	private final VertexID thisTasksVertexID;
	private final List<VertexID> upstreamVertexes;
	private final List<VertexID> downstreamVertexes;
	private final int numberOfDirectDownstreamNeighbours;
	private final List<JobVertex> sortedJobVertexes;
	private final int subtaskIndex;
	private final JobVertexID jobVertexID;
	private final JobVertex jobVertex;

	private final List<JobVertex> directlyUpstreamJobVertexes;
	private final List<JobVertex> nonDirectlyUpstreamJobVertexes;

	public VertexGraphInformation(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID, int subtaskIndex) {

		this.sortedJobVertexes = sortedJobVertexes;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
		this.jobVertex = CausalGraphUtils.fromSortedList(sortedJobVertexes, jobVertexID);
		this.thisTasksVertexID = CausalGraphUtils.computeVertexId(sortedJobVertexes, jobVertexID, subtaskIndex);
		this.upstreamVertexes = CausalGraphUtils.getUpstreamVertexIds(sortedJobVertexes, jobVertexID);
		this.downstreamVertexes = CausalGraphUtils.getDownstreamVertexIds(sortedJobVertexes, jobVertexID);
		this.numberOfDirectDownstreamNeighbours = CausalGraphUtils.getNumberOfDirectDownstreamNeighbours(sortedJobVertexes, jobVertexID);

		this.directlyUpstreamJobVertexes = CausalGraphUtils.computeDirectlyUpstreamJobVertexes(sortedJobVertexes, jobVertexID);
		this.nonDirectlyUpstreamJobVertexes = CausalGraphUtils.computeNonDirectlyUpstreamJobVertexes(sortedJobVertexes, jobVertexID);
	}

	public VertexID getThisTasksVertexID() {
		return thisTasksVertexID;
	}

	public List<VertexID> getUpstreamVertexes() {
		return upstreamVertexes;
	}

	public List<VertexID> getDownstreamVertexes() {
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


	public List<JobVertex> getSortedJobVertexes() {
		return sortedJobVertexes;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public JobVertex getJobVertex() {
		return jobVertex;
	}


	public List<JobVertex> getDirectlyUpstreamJobVertexes() {
		return directlyUpstreamJobVertexes;
	}

	public List<JobVertex> getNonDirectlyUpstreamJobVertexes() {
		return nonDirectlyUpstreamJobVertexes;
	}
}
