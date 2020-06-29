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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.SerializableObject;

import org.slf4j.Logger;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.hamcrest.Matchers.is;

/**
 * Tests concerning the state dispatch of each checkpointed running task to its counterpart standby execution.
 */
public class CheckpointStateDispatchToStandbyTasksTest {

	/**
	 * Tests that a running task's state is set for each stateful standby task after the successful completion of a checkpoint.
	 */
	@Test
	public void testDispatchLatestCheckpointedStateToStandbyTasks() {
		try {

			KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
			List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
			final KeyedStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			JobVertex jobVertexStateful = new JobVertex("NoOpInvokable", statefulId);
			final JobVertexID statelessId = new JobVertexID();
			JobVertex jobVertexStateless = new JobVertex("NoOpInvokable", statelessId);
	
			JobGraph jobGraph = new JobGraph("TestJob", jobVertexStateful, jobVertexStateless);
	
			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();

			Execution statefulStandbyExec1 = mockExecution(ExecutionState.STANDBY);
			Execution statefulStandbyExec2 = mockExecution(ExecutionState.STANDBY);
			Execution statefulStandbyExec3 = mockExecution(ExecutionState.STANDBY);
			Execution statelessStandbyExec1 = mockExecution(ExecutionState.STANDBY);
			Execution statelessStandbyExec2 = mockExecution(ExecutionState.STANDBY);

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec1)), statefulId, 0, 3);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec2)), statefulId, 1, 3);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec3)), statefulId, 2, 3);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1,
					new ArrayList<Execution>(Arrays.asList(statelessStandbyExec1)), statelessId, 0, 2);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2,
					new ArrayList<Execution>(Arrays.asList(statelessStandbyExec2)), statelessId, 1, 2);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);

			FailoverStrategy.Factory runStandbyTaskStrategy = new RunStandbyTaskStrategy.Factory(1);
			ExecutionGraph executionGraph = mockExecutionGraph(map, runStandbyTaskStrategy);
			when(stateful1.getExecutionGraph()).thenReturn(executionGraph);
			when(stateful2.getExecutionGraph()).thenReturn(executionGraph);
			when(stateful3.getExecutionGraph()).thenReturn(executionGraph);
			when(stateless1.getExecutionGraph()).thenReturn(executionGraph);
			when(stateless2.getExecutionGraph()).thenReturn(executionGraph);

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000L,
				600000L,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY);

			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();

			final TaskStateSnapshot subtaskStates = new TaskStateSnapshot();

			subtaskStates.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(statefulId),
				new OperatorSubtaskState(
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.singleton(serializedKeyGroupStates),
					StateObjectCollection.empty()));

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointId));

			// The pending checkpoint becomes a successfully completed one.
			assertThat(pending.isDiscarded(), is(true));
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// verify that each stateful vertex got the state
			BaseMatcher<JobManagerTaskRestore> matcher = new BaseMatcher<JobManagerTaskRestore>() {
				@Override
				public boolean matches(Object o) {
					if (o instanceof JobManagerTaskRestore) {
						JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
						return Objects.equals(taskRestore.getTaskStateSnapshot(), subtaskStates);
					}
					return false;
				}

				@Override
				public void describeTo(Description description) {
					description.appendValue(subtaskStates);
				}
			};

			verify(stateful1, times(1)).getStandbyExecutions();
			verify(stateful2, times(1)).getStandbyExecutions();
			verify(stateful3, times(1)).getStandbyExecutions();
			verify(stateless1, times(0)).getStandbyExecutions();
			verify(stateless2, times(0)).getStandbyExecutions();
			verify(statefulExec1, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec2, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec3, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statelessExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statelessExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statefulStandbyExec1, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statefulStandbyExec2, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statefulStandbyExec3, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statelessStandbyExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statelessStandbyExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that state dispatch does not happen when the StandbyTaskStrategy failover strategy is not enabled.
	 */
	@Test
	public void testDispatchLatestCheckpointedStateNonStandbyTaskFailoverStrategy() {
		try {

			KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
			List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
			final KeyedStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			JobVertex jobVertexStateful = new JobVertex("NoOpInvokable", statefulId);
			final JobVertexID statelessId = new JobVertexID();
			JobVertex jobVertexStateless = new JobVertex("NoOpInvokable", statelessId);
	
			JobGraph jobGraph = new JobGraph("TestJob", jobVertexStateful, jobVertexStateless);
	
			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();

			Execution statefulStandbyExec1 = mockExecution(ExecutionState.STANDBY);
			Execution statefulStandbyExec2 = mockExecution(ExecutionState.STANDBY);
			Execution statefulStandbyExec3 = mockExecution(ExecutionState.STANDBY);
			Execution statelessStandbyExec1 = mockExecution(ExecutionState.STANDBY);
			Execution statelessStandbyExec2 = mockExecution(ExecutionState.STANDBY);

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec1)), statefulId, 0, 3);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec2)), statefulId, 1, 3);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3,
					new ArrayList<Execution>(Arrays.asList(statefulStandbyExec3)), statefulId, 2, 3);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1,
					new ArrayList<Execution>(Arrays.asList(statelessStandbyExec1)), statelessId, 0, 2);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2,
					new ArrayList<Execution>(Arrays.asList(statelessStandbyExec2)), statelessId, 1, 2);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);

			// Use another failover strategy
			FailoverStrategy.Factory restartAllStrategy = new RestartAllStrategy.Factory();
			ExecutionGraph executionGraph = mockExecutionGraph(map, restartAllStrategy);
			when(stateful1.getExecutionGraph()).thenReturn(executionGraph);
			when(stateful2.getExecutionGraph()).thenReturn(executionGraph);
			when(stateful3.getExecutionGraph()).thenReturn(executionGraph);
			when(stateless1.getExecutionGraph()).thenReturn(executionGraph);
			when(stateless2.getExecutionGraph()).thenReturn(executionGraph);

			CheckpointCoordinator coord = new CheckpointCoordinator(
				jid,
				600000L,
				600000L,
				0,
				Integer.MAX_VALUE,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				Executors.directExecutor(),
				SharedStateRegistry.DEFAULT_FACTORY);

			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			assertTrue(coord.triggerCheckpoint(timestamp, false));

			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();

			final TaskStateSnapshot subtaskStates = new TaskStateSnapshot();

			subtaskStates.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(statefulId),
				new OperatorSubtaskState(
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.singleton(serializedKeyGroupStates),
					StateObjectCollection.empty()));

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointId, new CheckpointMetrics(), subtaskStates));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId));
			assertThat(pending.isDiscarded(), is(false));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointId));

			// The pending checkpoint becomes a successfully completed one.
			assertThat(pending.isDiscarded(), is(true));
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// verify that each stateful vertex got the state
			BaseMatcher<JobManagerTaskRestore> matcher = new BaseMatcher<JobManagerTaskRestore>() {
				@Override
				public boolean matches(Object o) {
					if (o instanceof JobManagerTaskRestore) {
						JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
						return Objects.equals(taskRestore.getTaskStateSnapshot(), subtaskStates);
					}
					return false;
				}

				@Override
				public void describeTo(Description description) {
					description.appendValue(subtaskStates);
				}
			};

			verify(stateful1, times(0)).getStandbyExecutions();
			verify(stateful2, times(0)).getStandbyExecutions();
			verify(stateful3, times(0)).getStandbyExecutions();
			verify(stateless1, times(0)).getStandbyExecutions();
			verify(stateless2, times(0)).getStandbyExecutions();
			verify(statefulExec1, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec2, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec3, times(0)).setInitialState(Mockito.argThat(matcher));
			verify(statelessExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statelessExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statefulStandbyExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statefulStandbyExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statefulStandbyExec3, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statelessStandbyExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
			verify(statelessStandbyExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that the allow non restored state flag is correctly handled.
	 *
	 * <p>The flag only applies for state that is part of the checkpoint.
	 */
	@Test
	public void testDispatchLatestCheckpointedStateNonRestored() throws Exception {
		// --- (1) Create tasks to restore checkpoint with ---
		JobVertexID jobVertexId1 = new JobVertexID();
		JobVertexID jobVertexId2 = new JobVertexID();

		OperatorID operatorId1 = OperatorID.fromJobVertexID(jobVertexId1);

		Execution vertexExec11 = mockExecution();
		Execution vertexExec12 = mockExecution();
		Execution vertexExec13 = mockExecution();
		Execution vertexExec21 = mockExecution();
		Execution vertexExec22 = mockExecution();

		Execution vertexStandbyExec11 = mockExecution(ExecutionState.STANDBY);
		Execution vertexStandbyExec12 = mockExecution(ExecutionState.STANDBY);
		Execution vertexStandbyExec13 = mockExecution(ExecutionState.STANDBY);
		Execution vertexStandbyExec21 = mockExecution(ExecutionState.STANDBY);
		Execution vertexStandbyExec22 = mockExecution(ExecutionState.STANDBY);

		// 1st JobVertex: stateful
		ExecutionVertex vertex11 = mockExecutionVertex(vertexExec11,
				new ArrayList<Execution>(Arrays.asList(vertexStandbyExec11)), jobVertexId1, 0, 3);
		ExecutionVertex vertex12 = mockExecutionVertex(vertexExec12,
				new ArrayList<Execution>(Arrays.asList(vertexStandbyExec12)), jobVertexId1, 1, 3);
		ExecutionVertex vertex13 = mockExecutionVertex(vertexExec13,
				new ArrayList<Execution>(Arrays.asList(vertexStandbyExec13)), jobVertexId1, 2, 3);
		// 2nd JobVertex: stateless
		ExecutionVertex vertex21 = mockExecutionVertex(vertexExec21,
				new ArrayList<Execution>(Arrays.asList(vertexStandbyExec21)), jobVertexId2, 0, 2);
		ExecutionVertex vertex22 = mockExecutionVertex(vertexExec22,
				new ArrayList<Execution>(Arrays.asList(vertexStandbyExec22)), jobVertexId2, 1, 2);

		ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(jobVertexId1, new ExecutionVertex[] { vertex11, vertex12, vertex13 });
		ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(jobVertexId2, new ExecutionVertex[] { vertex21, vertex22 });

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexId1, jobVertex1);
		tasks.put(jobVertexId2, jobVertex2);

		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			new ExecutionVertex[] { vertex11, vertex12, vertex13, vertex21, vertex22 },
			new ExecutionVertex[] { vertex11, vertex12, vertex13, vertex21, vertex22 },
			new ExecutionVertex[] { vertex11, vertex12, vertex13, vertex21, vertex22 },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY);

		KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
		List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
		final KeyedStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

		final TaskStateSnapshot subtaskStates = new TaskStateSnapshot();

		subtaskStates.putSubtaskStateByOperatorID(
			OperatorID.fromJobVertexID(jobVertexId1),
			new OperatorSubtaskState(
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.singleton(serializedKeyGroupStates),
				StateObjectCollection.empty()));

		// --- (2) Checkpoint misses state for a jobVertex (should work) ---
		Map<OperatorID, OperatorState> checkpointTaskStates = new HashMap<>();
		{
			OperatorState taskState = new OperatorState(operatorId1, 3, 3);
			taskState.putState(0, new OperatorSubtaskState(
						StateObjectCollection.empty(),
						StateObjectCollection.empty(),
						StateObjectCollection.singleton(serializedKeyGroupStates),
						StateObjectCollection.empty()));
			taskState.putState(1, new OperatorSubtaskState(
						StateObjectCollection.empty(),
						StateObjectCollection.empty(),
						StateObjectCollection.singleton(serializedKeyGroupStates),
						StateObjectCollection.empty()));
			taskState.putState(2, new OperatorSubtaskState(
						StateObjectCollection.empty(),
						StateObjectCollection.empty(),
						StateObjectCollection.singleton(serializedKeyGroupStates),
						StateObjectCollection.empty()));

			checkpointTaskStates.put(operatorId1, taskState);
		}
		CompletedCheckpoint checkpoint = new CompletedCheckpoint(
			new JobID(),
			0,
			1,
			2,
			new HashMap<>(checkpointTaskStates),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation());

		coord.getCheckpointStore().addCheckpoint(checkpoint);
		assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
		assertEquals(0, coord.getNumberOfPendingCheckpoints());

		// verify that each stateful vertex got the state
		BaseMatcher<JobManagerTaskRestore> matcher = new BaseMatcher<JobManagerTaskRestore>() {
			@Override
			public boolean matches(Object o) {
				if (o instanceof JobManagerTaskRestore) {
					JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
					return Objects.equals(taskRestore.getTaskStateSnapshot(), subtaskStates);
				}
				return false;
			}

			@Override
			public void describeTo(Description description) {
				description.appendValue(subtaskStates);
			}
		};

		assertTrue(coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, true, false));
		verify(vertex11, times(1)).getStandbyExecutions();
		verify(vertex12, times(1)).getStandbyExecutions();
		verify(vertex13, times(1)).getStandbyExecutions();
		verify(vertex21, times(0)).getStandbyExecutions();
		verify(vertex22, times(0)).getStandbyExecutions();
		verify(vertexExec11, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec12, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec13, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec11, times(1)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec12, times(1)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec13, times(1)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());

		assertTrue(coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, true, true));
		verify(vertex11, times(2)).getStandbyExecutions();
		verify(vertex12, times(2)).getStandbyExecutions();
		verify(vertex13, times(2)).getStandbyExecutions();
		verify(vertex21, times(0)).getStandbyExecutions();
		verify(vertex22, times(0)).getStandbyExecutions();
		verify(vertexExec11, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec12, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec13, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec11, times(2)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec12, times(2)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec13, times(2)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());

		// --- (3) JobVertex missing for task state that is part of the checkpoint ---
		JobVertexID newJobVertexID = new JobVertexID();
		OperatorID newOperatorID = OperatorID.fromJobVertexID(newJobVertexID);

		// There is no task for this
		{
			OperatorState taskState = new OperatorState(newOperatorID, 1, 1);
			taskState.putState(0, new OperatorSubtaskState());

			checkpointTaskStates.put(newOperatorID, taskState);
		}

		checkpoint = new CompletedCheckpoint(
			new JobID(),
			1,
			2,
			3,
			new HashMap<>(checkpointTaskStates),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation());

		coord.getCheckpointStore().addCheckpoint(checkpoint);

		// (i) Allow non restored state (should succeed)
		assertTrue(coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, true, true));
		verify(vertex11, times(3)).getStandbyExecutions();
		verify(vertex12, times(3)).getStandbyExecutions();
		verify(vertex13, times(3)).getStandbyExecutions();
		verify(vertex21, times(0)).getStandbyExecutions();
		verify(vertex22, times(0)).getStandbyExecutions();
		verify(vertexExec11, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec12, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec13, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec11, times(3)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec12, times(3)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec13, times(3)).setInitialState(Mockito.argThat(matcher));
		verify(vertexStandbyExec21, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
		verify(vertexStandbyExec22, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());

		// (ii) Don't allow non restored state (should fail)
		try {
			coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, true, false);
			fail("Did not throw the expected Exception.");
		} catch (IllegalStateException ignored) {
		}
	}

	// ------------------------------------------------------------------------

	private Execution mockExecution() {
		return mockExecution(ExecutionState.RUNNING);
	}

	private Execution mockExecution(ExecutionState state) {
		Execution mock = mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		// However, the following is useless because setInitialState() references the state field directly.
		// For a new Execution object state equals to CREATED always.
		when(mock.getState()).thenReturn(state);
		return mock;
	}

	private ExecutionVertex mockExecutionVertex(Execution execution, ArrayList<Execution> standbyExecutions, JobVertexID vertexId, int subtask, int parallelism) {
		ExecutionVertex mock = mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		when(mock.getStandbyExecutions()).thenReturn(standbyExecutions);
		when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(mock.getMaxParallelism()).thenReturn(parallelism);
		return mock;
	}

	private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getMaxParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		when(vertex.getOperatorIDs()).thenReturn(Collections.singletonList(OperatorID.fromJobVertexID(id)));
		when(vertex.getUserDefinedOperatorIDs()).thenReturn(Collections.<OperatorID>singletonList(null));

		for (ExecutionVertex v : vertices) {
			when(v.getJobVertex()).thenReturn(vertex);
		}
		return vertex;
	}

	private ExecutionGraph mockExecutionGraph(Map<JobVertexID, ExecutionJobVertex> vertices, FailoverStrategy.Factory failoverStrategyFactory) {
		ExecutionGraph executionGraph = mock(ExecutionGraph.class);
		when(executionGraph.getFutureExecutor()).thenReturn(TestingUtils.defaultExecutor());
		FailoverStrategy failoverStrategy = failoverStrategyFactory.create(executionGraph);
		when(executionGraph.getAllVertices()).thenReturn(vertices);
		when(executionGraph.getFailoverStrategy()).thenReturn(failoverStrategy);
		return executionGraph;
	}


}
