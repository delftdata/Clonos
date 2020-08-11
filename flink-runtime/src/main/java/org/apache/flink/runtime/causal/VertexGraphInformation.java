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
import java.util.Map;
import java.util.stream.Collectors;

public class VertexGraphInformation {

	private final List<VertexID> upstreamVertexes;
	private final List<VertexID> downstreamVertexes;
	private final int numberOfDirectDownstreamNeighbours;
	private final List<JobVertex> sortedJobVertexes;
	private final boolean hasUpstream;
	private final boolean hasDownstream;

	private final VertexID thisTasksVertexID;
	private final int subtaskIndex;
	private final JobVertexID jobVertexID;
	private final JobVertex jobVertex;


	/**
	 * Encodes the distance from this vertex to all other vertexes.
	 * Not being present represents disconnected components.
	 * Negative distance means upstream, positive distance represents downstream.
	 */
	private final Map<VertexID, Integer> distancesToVertex;

	public VertexGraphInformation(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID, int subtaskIndex) {

		this.sortedJobVertexes = sortedJobVertexes;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
		this.jobVertex = CausalGraphUtils.fromSortedList(sortedJobVertexes, jobVertexID);
		this.thisTasksVertexID = CausalGraphUtils.computeVertexId(sortedJobVertexes, jobVertexID, subtaskIndex);

		this.distancesToVertex = CausalGraphUtils.computeDistances(sortedJobVertexes, jobVertexID, subtaskIndex);

		this.upstreamVertexes = _getUpstreamVertexes();
		this.downstreamVertexes = _getDownstreamVertexes();
		this.numberOfDirectDownstreamNeighbours = _getNumberOfDirectDownstreamNeighbours();
		this.hasDownstream = _hasDownstream();
		this.hasUpstream = _hasUpstream();

	}

	public VertexID getThisTasksVertexID() {
		return thisTasksVertexID;
	}

	public List<VertexID> getUpstreamVertexes() {
		return this.upstreamVertexes;
	}
	public List<VertexID> _getUpstreamVertexes() {
		return distancesToVertex.entrySet().stream().filter(e -> e.getValue() < 0).map(Map.Entry::getKey).collect(Collectors.toList());
	}

	public List<VertexID> getDownstreamVertexes() {
		return this.downstreamVertexes;
	}
	public List<VertexID> _getDownstreamVertexes() {
		return distancesToVertex.entrySet().stream().filter(e -> e.getValue() > 0).map(Map.Entry::getKey).collect(Collectors.toList());
	}

	public int getNumberOfDirectDownstreamNeighbours() {
		return this.numberOfDirectDownstreamNeighbours;
	}
	public int _getNumberOfDirectDownstreamNeighbours() {
		return (int) distancesToVertex.values().stream().filter(v -> v == 1).count();
	}

	public boolean hasUpstream(){
		return this.hasUpstream;
	}
	public boolean _hasUpstream(){
		return distancesToVertex.values().stream().anyMatch(v -> v < 0);
	}

	public boolean hasDownstream(){
		return this.hasDownstream;
	}
	public boolean _hasDownstream(){
		return distancesToVertex.values().stream().anyMatch(v -> v > 0);
	}

	public int getDistanceTo(VertexID vertexID){
		return distancesToVertex.get(vertexID);
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

	public Map<VertexID, Integer> getDistances() {
		return distancesToVertex;
	}
}
