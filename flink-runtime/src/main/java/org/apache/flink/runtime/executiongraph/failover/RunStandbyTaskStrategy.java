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

import akka.remote.Ack;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.EstablishedResourceManagerConnection;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Failover strategy that maintains a standby task for each task
 * on the execution graph along with its state and substitutes a failed
 * task with its associated one.
 */
public class RunStandbyTaskStrategy extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(RunStandbyTaskStrategy.class);

	/**
	 * The execution graph to recover
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * The executor for creating, connecting, scheduling, and running a STANDBY task
	 */
	private final Executor callbackExecutor;

	private final int numStandbyTasksToMaintain;


	/**
	 * Creates a new failover strategy that recovers from failures by restarting all tasks
	 * of the execution graph.
	 *
	 * @param executionGraph            The execution graph to handle.
	 * @param numStandbyTasksToMaintain
	 */
	public RunStandbyTaskStrategy(ExecutionGraph executionGraph, int numStandbyTasksToMaintain) {
		this.executionGraph = checkNotNull(executionGraph);
		this.callbackExecutor = checkNotNull(executionGraph.getFutureExecutor());
		this.numStandbyTasksToMaintain = numStandbyTasksToMaintain;
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final ExecutionVertex vertexToRecover = taskExecution.getVertex();

		//The plan to recover the vertex is the following:
		// If a standby already exists:
		// 		Concurrently remove the failed slots and start the standby
		// else
		//		Sequentially remove the failed slots, then schedule a new standby and dispatch state to it. Doing this
		//		avoids scheduling the new standby to the failed TM. Following that, start it.
		// If an error occurs anywhere in this process, we fallback to the global restart strategy

		LOG.info(getStrategyName() + "failover strategy is triggered for the recovery of task " +
			vertexToRecover.getTaskNameWithSubtaskIndex() + ".");


		LOG.debug("Discarding pending checkpoints unacknowledged by failed task and restarting checkpoint coordinator with backoff");
		//In order to allow the task to recover while still making progress, we reset the checkpoint coordinator timeout
		assert this.executionGraph.getCheckpointCoordinator() != null;
		ExecutionVertex vertex = taskExecution.getVertex();
		callbackExecutor.execute(() -> {
			this.executionGraph.getCheckpointCoordinator().rpcIgnoreUnacknowledgedPendingCheckpointsFor(vertex, taskExecution.getAttemptId(), new Exception("Task failed and is recovering causally."));
			this.executionGraph.getCheckpointCoordinator().restartBackoffCheckpointScheduler();
		});

		CompletableFuture<Void> removeSlotsFuture = asyncRemoveFailedSlots(taskExecution);

		//By default, there should already be a standby ready
		CompletableFuture<Void> standbyReady = CompletableFuture.completedFuture(null);

		//If there isnt, we need to wait for the remove slots to complete, before scheduling a new standby
		//This guarantees we do not reschedule to the same slot
		if (vertexToRecover.getStandbyExecutions().size() == 0)
			standbyReady = composePrepareNewStandby(vertexToRecover, removeSlotsFuture);

		//If there was a standby, this runs without waiting for the failed vertex to be removed, otherwise it performs
		//the necessary steps first
		standbyReady.thenAcceptAsync((Void) -> {
			LOG.info("Running the standby execution.");
			vertexToRecover.runStandbyExecution();
			LOG.info("Done requesting standby execution");
		}, callbackExecutor);

		//In case of exceptions during the whole execution, trigger full recovery
		standbyReady.exceptionally((Throwable t) -> {
			executionGraph.failGlobal(
				new Exception("Error during standby task recovery, triggering full recovery: ", t));
			return null;
		});
	}

	private CompletableFuture<Void> composePrepareNewStandby(ExecutionVertex vertexToRecover,
															 CompletableFuture<Void> removeSlotsFuture) {
		return removeSlotsFuture.thenComposeAsync((Void) -> {
			//I believe the compose call should then wait for the addStandbyExecution to complete
			LOG.info("Adding a new standby execution");
			return vertexToRecover.addStandbyExecution();
		}, callbackExecutor).thenComposeAsync((Void) -> {
			LOG.info("Waiting for standby to be ready");
			Execution standby = vertexToRecover.getStandbyExecutions().get(0);
			while (true) {
				if (standby.getState() == ExecutionState.STANDBY)
					break;
			}
			LOG.info("Standby is ready.");
			LOG.info("Dispatching latest state.");
			try {
				executionGraph.getCheckpointCoordinator().dispatchLatestCheckpointedStateToStandbyTasks(
					Collections.singletonMap(vertexToRecover.getJobvertexId(), vertexToRecover.getJobVertex()),
					false, true);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
			return CompletableFuture.completedFuture(null);
		}, callbackExecutor);
	}

	private CompletableFuture<Void> asyncRemoveFailedSlots(Execution taskExecution) {

		ResourceID resourceIDOfFailedTM = taskExecution.getAssignedResourceLocation().getResourceID();
		Exception disconnectionCause = new FlinkException("disconnecting TM preventatively");

		return CompletableFuture.supplyAsync(() -> {
			LOG.info("Releasing failed slots");
			//Note: this happens synchronously, even if it returns a future.
			executionGraph.getSlotPool().releaseTaskManager(resourceIDOfFailedTM, disconnectionCause);

			LOG.info("Disconnect current TM to avoid rescheduling to failed TM");
			EstablishedResourceManagerConnection resManCon = executionGraph.getResourceManagerConnection();
			resManCon.getResourceManagerGateway().disconnectTaskManager(resourceIDOfFailedTM, disconnectionCause);

			return null;
		}, callbackExecutor);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

		for (int i = 0; i < numStandbyTasksToMaintain; i++) {
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

		int numStandbyTasksToMaintain;

		public Factory(int numStandbyTasksToMaintain) {
			this.numStandbyTasksToMaintain = numStandbyTasksToMaintain;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RunStandbyTaskStrategy(executionGraph, numStandbyTasksToMaintain);
		}
	}
}
