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

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CausalGraphUtils {

	public static VertexId computeVertexId(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID, int subtaskIndex) {
		short idCounter = 0;
		for (JobVertex jobVertex : sortedJobVertexes) {
			if (jobVertex.getID().equals(jobVertexID)) {
				idCounter += subtaskIndex;
				break;
			}
			idCounter += jobVertex.getParallelism();
		}
		return new VertexId(idCounter);
	}

	private static JobVertex fromSortedList(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID) {
		Map<JobVertexID, JobVertex> map = sortedJobVertexes.stream().collect(Collectors.toMap(JobVertex::getID, jobVertex -> jobVertex));
		return map.get(jobVertexID);
	}

	private static List<VertexId> toVertexIdList(List<JobVertex> sortedList, List<JobVertex> jobVertexesToCompute) {
		return jobVertexesToCompute.stream().flatMap(v -> IntStream.range(0, v.getParallelism()).boxed().map(i -> computeVertexId(sortedList, v.getID(), i))).collect(Collectors.toList());
	}

	public static List<VertexId> getUpstreamVertexIds(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID) {
		JobVertex target = fromSortedList(sortedJobVertexes, jobVertexID);

		List<JobVertex> upstreamVertexes = new LinkedList<>();
		Deque<JobVertex> unexplored = target.getInputs().stream().map(je -> je.getSource().getProducer()).distinct().collect(Collectors.toCollection(ArrayDeque::new));

		while (!unexplored.isEmpty()) {
			JobVertex toExplore = unexplored.pop();
			upstreamVertexes.add(toExplore);

			toExplore.getInputs().forEach(jobEdge -> unexplored.add(jobEdge.getSource().getProducer()));
		}

		upstreamVertexes = upstreamVertexes.stream().distinct().collect(Collectors.toList());

		return toVertexIdList(sortedJobVertexes, upstreamVertexes);
	}

	public static List<VertexId> getDownstreamVertexIds(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID) {
		JobVertex target = fromSortedList(sortedJobVertexes, jobVertexID);

		List<JobVertex> downstreamVertexes = new LinkedList<>();
		Deque<JobVertex> unexplored = target.getProducedDataSets().stream().flatMap(ds -> ds.getConsumers().stream().map(JobEdge::getTarget)).distinct().collect(Collectors.toCollection(ArrayDeque::new));

		while (!unexplored.isEmpty()) {
			JobVertex toExplore = unexplored.pop();
			downstreamVertexes.add(toExplore);

			unexplored.addAll(toExplore.getProducedDataSets().stream().flatMap(ds -> ds.getConsumers().stream().map(JobEdge::getTarget)).distinct().collect(Collectors.toList()));
		}

		downstreamVertexes = downstreamVertexes.stream().distinct().collect(Collectors.toList());

		return toVertexIdList(sortedJobVertexes, downstreamVertexes);
	}

	public static int getNumberOfDirectDownstreamNeighbours(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID){
		JobVertex target = fromSortedList(sortedJobVertexes, jobVertexID);

		return target.getProducedDataSets().stream().map(IntermediateDataSet::getConsumers).flatMap(je -> je.stream().map(JobEdge::getTarget)).distinct().mapToInt(JobVertex::getParallelism).sum();
	}
}
