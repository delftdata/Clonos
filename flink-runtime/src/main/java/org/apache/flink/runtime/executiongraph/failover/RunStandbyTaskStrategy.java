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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.execution.ExecutionState;

import org.apache.flink.runtime.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Failover strategy that maintains a standby task for each task
 * on the execution graph along with its state and substitutes a failed
 * task with its associated one.
 *
 */
public class RunStandbyTaskStrategy extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(RunStandbyTaskStrategy.class);

	/** The execution graph to recover */
	private final ExecutionGraph executionGraph;

	/** The executor for creating, connecting, scheduling, and running a STANDBY task */
	private final Executor callbackExecutor;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting all tasks
	 * of the execution graph.
	 * 
	 * @param executionGraph The execution graph to handle.
	 */
	public RunStandbyTaskStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.callbackExecutor = checkNotNull(executionGraph.getFutureExecutor());
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final CompletableFuture<ExecutionState> terminationFuture = taskExecution.getTerminalStateFuture();
		final ExecutionVertex vertexToRecover = taskExecution.getVertex();

		try {
			LOG.info(getStrategyName() + "failover strategy is triggered for the recovery of task " +
					vertexToRecover.getTaskNameWithSubtaskIndex() +
					". Activating standby task for this task.");
			vertexToRecover.runStandbyExecution();
		}
		catch (IllegalStateException e) {
			executionGraph.failGlobal(
					new Exception("Error during standby task recovery: no standby execution to run -- triggering full recovery", e));
		}
		catch (Exception e) {
			executionGraph.failGlobal(
					new Exception("Error during standby task recovery -- triggering full recovery", e));
		}
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final boolean isStandby = true;
                final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

                for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {

				final CompletableFuture<Void> currentExecutionFuture =
					// TODO: Anti-affinity constraint
					CompletableFuture.runAsync(
							() -> waitForExecutionToReachRunningState(executionVertex));
				currentExecutionFuture.whenComplete(
						(Void ignored, Throwable t) -> {
							if (t == null) {
								// this should aalso respect the topological order
								final CompletableFuture<Void> standbyExecutionFuture =
									executionVertex.addStandbyExecution();
								schedulingFutures.add(standbyExecutionFuture);
							} else {
								schedulingFutures.add(
										new CompletableFuture<>());
								schedulingFutures.get(schedulingFutures.size() - 1)
									.completeExceptionally(t);
							}
						});
			}
		}

		final CompletableFuture<Void> allSchedulingFutures = FutureUtils.waitForAll(schedulingFutures);
		allSchedulingFutures.whenComplete((Void ignored, Throwable t) -> {
			if (t != null) {
				LOG.warn("Scheduling of standby tasks in '" +
					getStrategyName() + "' failed. Cancelling the scheduling of standby tasks.");
				for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
					for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
						executionVertex.cancelStandbyExecution();
					}
				}
			}
		});
	}

	private void waitForExecutionToReachRunningState(ExecutionVertex executionVertex) {
		ExecutionState executionState = ExecutionState.CREATED;
		do {
			executionState = executionVertex.getExecutionState();
		} while (executionState == ExecutionState.CREATED ||
				executionState == ExecutionState.SCHEDULED ||
				executionState == ExecutionState.DEPLOYING);
	}

	@Override
	public String getStrategyName() {
		return "run standby task";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RunStandbyTaskStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RunStandbyTaskStrategy(executionGraph);
		}
	}
}
