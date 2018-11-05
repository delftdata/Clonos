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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.api.common.time.Time;

import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.mockito.verification.Timeout;

public class RunStandbyTaskStrategyTest extends TestLogger {

	private final static int NUM_TASKS = 1;

	@Test
	public void testSetupStandbyTaskStrategyWaitCurrentExecutionNotRunning() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		// Not enough instances to schedule both the normal and the standby tasks.
		executionGraph.scheduleForExecution();

		// standby task scheduling does not happen unless currentExecution switches to RUNNING state.
		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(0));
			}
		}

		verify(tmg1, new Timeout(500L, times(NUM_TASKS))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
	}

	@Test
	public void testSetupStandbyTaskStrategyFailNoInstancesAvailable() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		// Not enough instances to schedule both the normal and the standby tasks.
		executionGraph.scheduleForExecution();

		// standby task scheduling is completed exceptionally and triggers cancelStandbyExecution().
		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				executionVertex.getCurrentExecutionAttempt().setState(ExecutionState.RUNNING);
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(0));
			}
		}

		verify(tmg1, new Timeout(500L, times(NUM_TASKS))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
	}

	@Test
	public void testStandbyTaskStrategyCancelGraph() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.cancelTask(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS*2);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				executionVertex.getCurrentExecutionAttempt().setState(ExecutionState.RUNNING);
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				Thread.sleep(50);

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(1));
				Execution standbyExecution = standbyExecutions.get(0);
				assertThat(standbyExecution.getState(), is(ExecutionState.DEPLOYING));
			}
		}

		// One submit task call for the normal task and one for the standby task.
		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		executionGraph.cancel();

		assertEquals(JobStatus.CANCELLING, executionGraph.getState());

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.CANCELING));
				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(0));
			}
		}

		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).cancelTask(any(ExecutionAttemptID.class), any(Time.class));

	}

	@Test
	public void testStandbyTaskStrategyFailGlobal() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.cancelTask(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS*2);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				executionVertex.getCurrentExecutionAttempt().setState(ExecutionState.RUNNING);
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				Thread.sleep(50);

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(1));
				Execution standbyExecution = standbyExecutions.get(0);
				assertThat(standbyExecution.getState(), is(ExecutionState.DEPLOYING));
			}
		}

		// One submit task call for the normal task and one for the standby task.
		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		// failGlobal() does not trigger any failover startegy. The other way around happens with RestartAllStrategy.
		executionGraph.failGlobal(new Exception("Kill graph to see if the standby graphs will be killed too."));

		assertEquals(JobStatus.FAILING, executionGraph.getState());

		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).cancelTask(any(ExecutionAttemptID.class), any(Time.class));
	}

	@Test
	public void testStandbyTaskStrategyTriggerButStandbyTaskNotInStandbyState() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.cancelTask(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.switchStandbyTaskToRunning(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS*2);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				executionVertex.getCurrentExecutionAttempt().setState(ExecutionState.RUNNING);
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				Thread.sleep(50);

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(1));
				Execution standbyExecution = standbyExecutions.get(0);
				assertThat(standbyExecution.getState(), is(ExecutionState.DEPLOYING));
			}
		}

		// One submit task call for the normal task and one for the standby task.
		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));

				// Standby task is not in STANDBY state, therefore it will be cancelled.
				executionVertex.getCurrentExecutionAttempt().fail(
						new Exception("Fail task to kick-start failover strategy."));

				verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).cancelTask(
						any(ExecutionAttemptID.class), any(Time.class));

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(0));
			}
		}
	}

	@Test
	public void testStandbyTaskStrategyTrigger() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		TaskManagerGateway tmg1 = mock(TaskManagerGateway.class);
		when(tmg1.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.cancelTask(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(tmg1.switchStandbyTaskToRunning(any(ExecutionAttemptID.class), any(Time.class)))
				.thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		Instance instance = ExecutionGraphTestUtils.getInstance(tmg1, NUM_TASKS*2);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"TestJob",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			new RunStandbyTaskStrategy.Factory(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.DEPLOYING));
				executionVertex.getCurrentExecutionAttempt().setState(ExecutionState.RUNNING);
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				Thread.sleep(50);

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(1));
				Execution standbyExecution = standbyExecutions.get(0);
				assertThat(standbyExecution.getState(), is(ExecutionState.DEPLOYING));

				assertThat(standbyExecution.setState(ExecutionState.STANDBY), is(true));
				assertThat(standbyExecution.getState(), is(ExecutionState.STANDBY));
			}
		}

		// One submit task call for the normal task and one for the standby task.
		verify(tmg1, new Timeout(500L, times(NUM_TASKS*2))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				assertThat(executionVertex.getCurrentExecutionAttempt().getState(), is(ExecutionState.RUNNING));
				executionVertex.getCurrentExecutionAttempt().fail(
						new Exception("Fail task to kick-start failover strategy."));

				// The normal task is cancelled (see Execution.fail()).
				verify(tmg1, new Timeout(500L, times(NUM_TASKS))).cancelTask(
						any(ExecutionAttemptID.class), any(Time.class));
				// The standby task is kick-started.
				verify(tmg1, new Timeout(500L, times(NUM_TASKS))).switchStandbyTaskToRunning(
						any(ExecutionAttemptID.class), any(Time.class));

				ArrayList<Execution> standbyExecutions = executionVertex.getStandbyExecutions();
				assertThat(standbyExecutions.size(), is(1));
				Execution standbyExecution = standbyExecutions.get(0);
				assertThat(standbyExecution.getState(), is(ExecutionState.STANDBY));
			}
		}
		// This is more of a documentation note. As far as the ExecutionGraph is concerned
		// the job is running normally. Only failGlobal() switches the job status to
		// failing. From one perspective this stands on reason. If a local failure is treated
		// locally, then the overall job status may stay intact. Let's revisit this rationale.
		assertEquals(JobStatus.RUNNING, executionGraph.getState());
	}
}
