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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.RecoverableCompletedCheckpointStore;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the state snapshot tranfer to standby tasks triggered at successful completion of a checkpoint
 * in the checkpoint coordinator when the RunStandbyTaskStrategy failover strategy is enabled.
 */
public class CheckpointCoordinatorDispatchStateToStandbyTasksTest {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	/**
	 * Tests that the checkpointed partitioned and non-partitioned state is assigned properly to
	 * the standby {@link Execution} upon a successful checkpoint.
	 *
	 * @throws Exception
	 */
	@Test
	public void testDispatchLatestCheckpointedStateToStandbyTasks() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices =
				allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexID1, jobVertex1);
		tasks.put(jobVertexID2, jobVertex2);
		FailoverStrategy.Factory runStandbyTaskStrategy = new RunStandbyTaskStrategy.Factory(1);
		ExecutionGraph executionGraph = mockExecutionGraph(tasks, runStandbyTaskStrategy);
		for (ExecutionVertex executionVertex : allExecutionVertices) {
			when(executionVertex.getExecutionGraph()).thenReturn(executionGraph);
		}

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			TaskStateSnapshot subtaskState = mockSubtaskState(jobVertexID1, index, keyGroupPartitions1.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			TaskStateSnapshot subtaskState = mockSubtaskState(jobVertexID2, index, keyGroupPartitions2.get(index));

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					subtaskState);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		for (ExecutionVertex executionVertex : allExecutionVertices) {
			verify(executionVertex.getStandbyExecutions().get(0), times(1)).setInitialState(any());
		}

		// TODO: Verify that we don't need to register the state of standby tasks in the shared registry.
		//       Otherwise a) the following block of test code is required.
		//       And b) check out CheckpointCoordinatorTest.testSharedStateRegistrationOnRestore
		/*for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
			for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
				for (OperatorSubtaskState subtaskState : taskState.getStates()) {
					verify(subtaskState, times(2)).registerSharedStates(any(SharedStateRegistry.class));
				}
			}
		}*/

		// Verify the dispatchred state.
		verifyStateDispatch(jobVertexID1, jobVertex1, keyGroupPartitions1);
		verifyStateDispatch(jobVertexID2, jobVertex2, keyGroupPartitions2);
	}

	/**
	 * Tests that the checkpointed state dispatch fails if the max parallelism of the job vertices has
	 * changed.
	 *
	 * @throws Exception
	 */
	@Test(expected=IllegalStateException.class)
	public void testDispatchLatestCheckpointFailureWhenMaxParallelismChanges() throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 2;
		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices = allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 = StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(null, null, keyGroupState, null);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
				taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}


		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			KeyGroupsStateHandle keyGroupState = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(null, null, keyGroupState, null);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);
			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		int newMaxParallelism1 = 20;
		int newMaxParallelism2 = 42;

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
			jobVertexID1,
			parallelism1,
			newMaxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			jobVertexID2,
			parallelism2,
			newMaxParallelism2);

		tasks.put(jobVertexID1, newJobVertex1);
		tasks.put(jobVertexID2, newJobVertex2);

		coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, true, false);

		fail("The restoration should have failed because the max parallelism changed.");
	}

	@Test
	public void testDispatchLatestCheckpointedStateScaleIn() throws Exception {
		testDispatchLatestCheckpointedStateWithChangingParallelism(false);
	}

	@Test
	public void testDispatchLatestCheckpointedStateScaleOut() throws Exception {
		testDispatchLatestCheckpointedStateWithChangingParallelism(true);
	}

	@Test
	public void testStateDispatchWhenTopologyChangeOut() throws Exception {
		testStateDispatchWithTopologyChange(0);
	}

	@Test
	public void testStateDispatchWhenTopologyChangeIn() throws Exception {
		testStateDispatchWithTopologyChange(1);
	}

	@Test
	public void testStateDispatchWhenTopologyChange() throws Exception {
		testStateDispatchWithTopologyChange(2);
	}


	/**
	 * Tests the checkpointed state dispatch with changing parallelism of job vertex with partitioned
	 * state.
	 *
	 * @throws Exception
	 */
	private void testDispatchLatestCheckpointedStateWithChangingParallelism(boolean scaleOut) throws Exception {
		final JobID jid = new JobID();
		final long timestamp = System.currentTimeMillis();

		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = scaleOut ? 2 : 13;

		int maxParallelism1 = 42;
		int maxParallelism2 = 13;

		int newParallelism2 = scaleOut ? 13 : 2;

		final ExecutionJobVertex jobVertex1 = mockExecutionJobVertex(
				jobVertexID1,
				parallelism1,
				maxParallelism1);
		final ExecutionJobVertex jobVertex2 = mockExecutionJobVertex(
				jobVertexID2,
				parallelism2,
				maxParallelism2);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

		allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

		ExecutionVertex[] arrayExecutionVertices =
				allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexID1, jobVertex1);
		tasks.put(jobVertexID2, jobVertex2);
		FailoverStrategy.Factory runStandbyTaskStrategy = new RunStandbyTaskStrategy.Factory(1);
		ExecutionGraph executionGraph = mockExecutionGraph(tasks, runStandbyTaskStrategy);
		for (ExecutionVertex executionVertex : allExecutionVertices) {
			when(executionVertex.getExecutionGraph()).thenReturn(executionGraph);
		}

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			jid,
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			arrayExecutionVertices,
			arrayExecutionVertices,
			arrayExecutionVertices,
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY);

		// trigger the checkpoint
		coord.triggerCheckpoint(timestamp, false);

		assertTrue(coord.getPendingCheckpoints().keySet().size() == 1);
		long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

		List<KeyGroupRange> keyGroupPartitions1 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
		List<KeyGroupRange> keyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		//vertex 1
		for (int index = 0; index < jobVertex1.getParallelism(); index++) {
			OperatorStateHandle opStateBackend = generatePartitionableStateHandle(jobVertexID1, index, 2, 8, false);
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), true);
			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(opStateBackend, null, keyedStateBackend, keyedStateRaw);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex1.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		//vertex 2
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesBackend = new ArrayList<>(jobVertex2.getParallelism());
		final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesRaw = new ArrayList<>(jobVertex2.getParallelism());
		for (int index = 0; index < jobVertex2.getParallelism(); index++) {
			KeyGroupsStateHandle keyedStateBackend = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
			KeyGroupsStateHandle keyedStateRaw = generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), true);
			OperatorStateHandle opStateBackend = generatePartitionableStateHandle(jobVertexID2, index, 2, 8, false);
			OperatorStateHandle opStateRaw = generatePartitionableStateHandle(jobVertexID2, index, 2, 8, true);
			expectedOpStatesBackend.add(new ChainedStateHandle<>(Collections.singletonList(opStateBackend)));
			expectedOpStatesRaw.add(new ChainedStateHandle<>(Collections.singletonList(opStateRaw)));

			OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(opStateBackend, opStateRaw, keyedStateBackend, keyedStateRaw);
			TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
			taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);

			AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					jid,
					jobVertex2.getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

			coord.receiveAcknowledgeMessage(acknowledgeCheckpoint);
		}

		for (ExecutionVertex executionVertex : allExecutionVertices) {
			verify(executionVertex, times(1)).getStandbyExecutions();
			verify(executionVertex.getStandbyExecutions().get(0), times(1)).setInitialState(any());
		}

		List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

		assertEquals(1, completedCheckpoints.size());

		Map<JobVertexID, ExecutionJobVertex> newTasks = new HashMap<>();

		List<KeyGroupRange> newKeyGroupPartitions2 =
				StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
				jobVertexID1,
				parallelism1,
				maxParallelism1);

		// rescale vertex 2
		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
				jobVertexID2,
				newParallelism2,
				maxParallelism2);

		newTasks.put(jobVertexID1, newJobVertex1);
		newTasks.put(jobVertexID2, newJobVertex2);
		coord.dispatchLatestCheckpointedStateToStandbyTasks(newTasks, true, false);

		List<ExecutionVertex> newAllExecutionVertices = new ArrayList<>(parallelism1 + newParallelism2);

		newAllExecutionVertices.addAll(Arrays.asList(newJobVertex1.getTaskVertices()));
		newAllExecutionVertices.addAll(Arrays.asList(newJobVertex2.getTaskVertices()));
		for (ExecutionVertex executionVertex : newAllExecutionVertices) {
			verify(executionVertex, times(1)).getStandbyExecutions();
			verify(executionVertex.getStandbyExecutions().get(0), times(1)).setInitialState(any());
		}

		// verify the dispatchred state
		verifyStateDispatch(jobVertexID1, newJobVertex1, keyGroupPartitions1);
		List<List<Collection<OperatorStateHandle>>> actualOpStatesBackend = new ArrayList<>(newJobVertex2.getParallelism());
		List<List<Collection<OperatorStateHandle>>> actualOpStatesRaw = new ArrayList<>(newJobVertex2.getParallelism());
		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

			List<OperatorID> operatorIDs = newJobVertex2.getOperatorIDs();

			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), true);

			JobManagerTaskRestore taskRestore = newJobVertex2.getTaskVertices()[i].getStandbyExecutions().get(0).getTaskRestore();
			Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot taskStateHandles = taskRestore.getTaskStateSnapshot();

			final int headOpIndex = operatorIDs.size() - 1;
			List<Collection<OperatorStateHandle>> allParallelManagedOpStates = new ArrayList<>(operatorIDs.size());
			List<Collection<OperatorStateHandle>> allParallelRawOpStates = new ArrayList<>(operatorIDs.size());

			for (int idx = 0; idx < operatorIDs.size(); ++idx) {
				OperatorID operatorID = operatorIDs.get(idx);
				OperatorSubtaskState opState = taskStateHandles.getSubtaskStateByOperatorID(operatorID);
				Collection<OperatorStateHandle> opStateBackend = opState.getManagedOperatorState();
				Collection<OperatorStateHandle> opStateRaw = opState.getRawOperatorState();
				allParallelManagedOpStates.add(opStateBackend);
				allParallelRawOpStates.add(opStateRaw);
				if (idx == headOpIndex) {
					Collection<KeyedStateHandle> keyedStateBackend = opState.getManagedKeyedState();
					Collection<KeyedStateHandle> keyGroupStateRaw = opState.getRawKeyedState();
					compareKeyedState(Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
					compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
				}
			}
			actualOpStatesBackend.add(allParallelManagedOpStates);
			actualOpStatesRaw.add(allParallelRawOpStates);
		}

		comparePartitionableState(expectedOpStatesBackend, actualOpStatesBackend);
		comparePartitionableState(expectedOpStatesRaw, actualOpStatesRaw);
	}

	private static Tuple2<JobVertexID, OperatorID> generateIDPair() {
		JobVertexID jobVertexID = new JobVertexID();
		OperatorID operatorID = OperatorID.fromJobVertexID(jobVertexID);
		return new Tuple2<>(jobVertexID, operatorID);
	}
	
	/**
	 * old topology
	 * [operator1,operator2] * parallelism1 -> [operator3,operator4] * parallelism2
	 *
	 *
	 * new topology
	 *
	 * [operator5,operator1,operator3] * newParallelism1 -> [operator3, operator6] * newParallelism2
	 *
	 * scaleType:
	 * 0  increase parallelism
	 * 1  decrease parallelism
	 * 2  same parallelism
	 */
	public void testStateDispatchWithTopologyChange(int scaleType) throws Exception {

		/*
		 * Old topology
		 * CHAIN(op1 -> op2) * parallelism1 -> CHAIN(op3 -> op4) * parallelism2
		 */
		Tuple2<JobVertexID, OperatorID> id1 = generateIDPair();
		Tuple2<JobVertexID, OperatorID> id2 = generateIDPair();
		int parallelism1 = 10;
		int maxParallelism1 = 64;

		Tuple2<JobVertexID, OperatorID> id3 = generateIDPair();
		Tuple2<JobVertexID, OperatorID> id4 = generateIDPair();
		int parallelism2 = 10;
		int maxParallelism2 = 64;

		List<KeyGroupRange> keyGroupPartitions2 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

		Map<OperatorID, OperatorState> operatorStates = new HashMap<>();

		//prepare vertex1 state
		for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id1, id2)) {
			OperatorState taskState = new OperatorState(id.f1, parallelism1, maxParallelism1);
			operatorStates.put(id.f1, taskState);
			for (int index = 0; index < taskState.getParallelism(); index++) {
				OperatorStateHandle subManagedOperatorState =
					generatePartitionableStateHandle(id.f0, index, 2, 8, false);
				OperatorStateHandle subRawOperatorState =
					generatePartitionableStateHandle(id.f0, index, 2, 8, true);
				OperatorSubtaskState subtaskState = new OperatorSubtaskState(
					subManagedOperatorState,
					subRawOperatorState,
					null,
					null);
				taskState.putState(index, subtaskState);
			}
		}

		List<List<ChainedStateHandle<OperatorStateHandle>>> expectedManagedOperatorStates = new ArrayList<>();
		List<List<ChainedStateHandle<OperatorStateHandle>>> expectedRawOperatorStates = new ArrayList<>();
		//prepare vertex2 state
		for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id3, id4)) {
			OperatorState operatorState = new OperatorState(id.f1, parallelism2, maxParallelism2);
			operatorStates.put(id.f1, operatorState);
			List<ChainedStateHandle<OperatorStateHandle>> expectedManagedOperatorState = new ArrayList<>();
			List<ChainedStateHandle<OperatorStateHandle>> expectedRawOperatorState = new ArrayList<>();
			expectedManagedOperatorStates.add(expectedManagedOperatorState);
			expectedRawOperatorStates.add(expectedRawOperatorState);

			for (int index = 0; index < operatorState.getParallelism(); index++) {
				OperatorStateHandle subManagedOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, false)
						.get(0);
				OperatorStateHandle subRawOperatorState =
					generateChainedPartitionableStateHandle(id.f0, index, 2, 8, true)
						.get(0);
				KeyGroupsStateHandle subManagedKeyedState = id.f0.equals(id3.f0)
					? generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), false)
					: null;
				KeyGroupsStateHandle subRawKeyedState = id.f0.equals(id3.f0)
					? generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), true)
					: null;

				expectedManagedOperatorState.add(ChainedStateHandle.wrapSingleHandle(subManagedOperatorState));
				expectedRawOperatorState.add(ChainedStateHandle.wrapSingleHandle(subRawOperatorState));

				OperatorSubtaskState subtaskState = new OperatorSubtaskState(
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);
				operatorState.putState(index, subtaskState);
			}
		}

		/*
		 * New topology
		 * CHAIN(op5 -> op1 -> op2) * newParallelism1 -> CHAIN(op3 -> op6) * newParallelism2
		 */
		Tuple2<JobVertexID, OperatorID> id5 = generateIDPair();
		int newParallelism1 = 10;

		Tuple2<JobVertexID, OperatorID> id6 = generateIDPair();
		int newParallelism2 = parallelism2;

		if (scaleType == 0) {
			newParallelism2 = 20;
		} else if (scaleType == 1) {
			newParallelism2 = 8;
		}

		List<KeyGroupRange> newKeyGroupPartitions2 =
			StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

		final ExecutionJobVertex newJobVertex1 = mockExecutionJobVertex(
			id5.f0,
			Arrays.asList(id2.f1, id1.f1, id5.f1),
			newParallelism1,
			maxParallelism1);

		final ExecutionJobVertex newJobVertex2 = mockExecutionJobVertex(
			id3.f0,
			Arrays.asList(id6.f1, id3.f1),
			newParallelism2,
			maxParallelism2);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();

		tasks.put(id5.f0, newJobVertex1);
		tasks.put(id3.f0, newJobVertex2);

		JobID jobID = new JobID();
		StandaloneCompletedCheckpointStore standaloneCompletedCheckpointStore =
			spy(new StandaloneCompletedCheckpointStore(1));

		CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(
				jobID,
				2,
				System.currentTimeMillis(),
				System.currentTimeMillis() + 3000,
				operatorStates,
				Collections.<MasterState>emptyList(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				new TestCompletedCheckpointStorageLocation());

		when(standaloneCompletedCheckpointStore.getLatestCheckpoint()).thenReturn(completedCheckpoint);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator coord = new CheckpointCoordinator(
			new JobID(),
			600000,
			600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			newJobVertex1.getTaskVertices(),
			new StandaloneCheckpointIDCounter(),
			standaloneCompletedCheckpointStore,
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY);

		List<ExecutionVertex> allExecutionVertices = new ArrayList<>(newParallelism1 + newParallelism2);
		allExecutionVertices.addAll(Arrays.asList(newJobVertex1.getTaskVertices()));
		allExecutionVertices.addAll(Arrays.asList(newJobVertex2.getTaskVertices()));

		coord.dispatchLatestCheckpointedStateToStandbyTasks(tasks, false, true);
		for (ExecutionVertex executionVertex : allExecutionVertices) {
			verify(executionVertex, times(1)).getStandbyExecutions();
			verify(executionVertex.getStandbyExecutions().get(0), times(1)).setInitialState(any());
		}

		for (int i = 0; i < newJobVertex1.getParallelism(); i++) {

			final List<OperatorID> operatorIds = newJobVertex1.getOperatorIDs();

			JobManagerTaskRestore taskRestore = newJobVertex1.getTaskVertices()[i].getStandbyExecutions().get(0).getTaskRestore();
			Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			OperatorSubtaskState headOpState = stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIds.size() - 1));
			assertTrue(headOpState.getManagedKeyedState().isEmpty());
			assertTrue(headOpState.getRawKeyedState().isEmpty());

			// operator5
			{
				int operatorIndexInChain = 2;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				assertTrue(opState.getManagedOperatorState().isEmpty());
				assertTrue(opState.getRawOperatorState().isEmpty());
			}
			// operator1
			{
				int operatorIndexInChain = 1;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				OperatorStateHandle expectedManagedOpState = generatePartitionableStateHandle(
					id1.f0, i, 2, 8, false);
				OperatorStateHandle expectedRawOpState = generatePartitionableStateHandle(
					id1.f0, i, 2, 8, true);

				Collection<OperatorStateHandle> managedOperatorState = opState.getManagedOperatorState();
				assertEquals(1, managedOperatorState.size());
				assertTrue(CommonTestUtils.isSteamContentEqual(expectedManagedOpState.openInputStream(),
					managedOperatorState.iterator().next().openInputStream()));

				Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
				assertEquals(1, rawOperatorState.size());
				assertTrue(CommonTestUtils.isSteamContentEqual(expectedRawOpState.openInputStream(),
					rawOperatorState.iterator().next().openInputStream()));
			}
			// operator2
			{
				int operatorIndexInChain = 0;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				OperatorStateHandle expectedManagedOpState = generatePartitionableStateHandle(
					id2.f0, i, 2, 8, false);
				OperatorStateHandle expectedRawOpState = generatePartitionableStateHandle(
					id2.f0, i, 2, 8, true);

				Collection<OperatorStateHandle> managedOperatorState = opState.getManagedOperatorState();
				assertEquals(1, managedOperatorState.size());
				assertTrue(CommonTestUtils.isSteamContentEqual(expectedManagedOpState.openInputStream(),
					managedOperatorState.iterator().next().openInputStream()));

				Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
				assertEquals(1, rawOperatorState.size());
				assertTrue(CommonTestUtils.isSteamContentEqual(expectedRawOpState.openInputStream(),
					rawOperatorState.iterator().next().openInputStream()));
			}
		}

		List<List<Collection<OperatorStateHandle>>> actualManagedOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());
		List<List<Collection<OperatorStateHandle>>> actualRawOperatorStates = new ArrayList<>(newJobVertex2.getParallelism());

		for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

			final List<OperatorID> operatorIds = newJobVertex2.getOperatorIDs();

			JobManagerTaskRestore taskRestore = newJobVertex2.getTaskVertices()[i].getStandbyExecutions().get(0).getTaskRestore();
			Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			// operator 3
			{
				int operatorIndexInChain = 1;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));

				List<Collection<OperatorStateHandle>> actualSubManagedOperatorState = new ArrayList<>(1);
				actualSubManagedOperatorState.add(opState.getManagedOperatorState());

				List<Collection<OperatorStateHandle>> actualSubRawOperatorState = new ArrayList<>(1);
				actualSubRawOperatorState.add(opState.getRawOperatorState());

				actualManagedOperatorStates.add(actualSubManagedOperatorState);
				actualRawOperatorStates.add(actualSubRawOperatorState);
			}

			// operator 6
			{
				int operatorIndexInChain = 0;
				OperatorSubtaskState opState =
					stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIndexInChain));
				assertTrue(opState.getManagedOperatorState().isEmpty());
				assertTrue(opState.getRawOperatorState().isEmpty());

			}

			KeyGroupsStateHandle originalKeyedStateBackend = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), false);
			KeyGroupsStateHandle originalKeyedStateRaw = generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), true);

			OperatorSubtaskState headOpState =
				stateSnapshot.getSubtaskStateByOperatorID(operatorIds.get(operatorIds.size() - 1));

			Collection<KeyedStateHandle> keyedStateBackend = headOpState.getManagedKeyedState();
			Collection<KeyedStateHandle> keyGroupStateRaw = headOpState.getRawKeyedState();


			compareKeyedState(Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
			compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
		}

		comparePartitionableState(expectedManagedOperatorStates.get(0), actualManagedOperatorStates);
		comparePartitionableState(expectedRawOperatorStates.get(0), actualRawOperatorStates);
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static KeyGroupsStateHandle generateKeyGroupState(
			JobVertexID jobVertexID,
			KeyGroupRange keyGroupPartition, boolean rawState) throws IOException {

		List<Integer> testStatesLists = new ArrayList<>(keyGroupPartition.getNumberOfKeyGroups());

		// generate state for one keygroup
		for (int keyGroupIndex : keyGroupPartition) {
			int vertexHash = jobVertexID.hashCode();
			int seed = rawState ? (vertexHash * (31 + keyGroupIndex)) : (vertexHash + keyGroupIndex);
			Random random = new Random(seed);
			int simulatedStateValue = random.nextInt();
			testStatesLists.add(simulatedStateValue);
		}

		return generateKeyGroupState(keyGroupPartition, testStatesLists);
	}

	public static KeyGroupsStateHandle generateKeyGroupState(
			KeyGroupRange keyGroupRange,
			List<? extends Serializable> states) throws IOException {

		Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() == states.size());

		Tuple2<byte[], List<long[]>> serializedDataWithOffsets =
				serializeTogetherAndTrackOffsets(Collections.<List<? extends Serializable>>singletonList(states));

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, serializedDataWithOffsets.f1.get(0));

		ByteStreamStateHandle allSerializedStatesHandle = new ByteStreamStateHandle(
				String.valueOf(UUID.randomUUID()),
				serializedDataWithOffsets.f0);

		return new KeyGroupsStateHandle(keyGroupRangeOffsets, allSerializedStatesHandle);
	}

	public static Tuple2<byte[], List<long[]>> serializeTogetherAndTrackOffsets(
			List<List<? extends Serializable>> serializables) throws IOException {

		List<long[]> offsets = new ArrayList<>(serializables.size());
		List<byte[]> serializedGroupValues = new ArrayList<>();

		int runningGroupsOffset = 0;
		for(List<? extends Serializable> list : serializables) {

			long[] currentOffsets = new long[list.size()];
			offsets.add(currentOffsets);

			for (int i = 0; i < list.size(); ++i) {
				currentOffsets[i] = runningGroupsOffset;
				byte[] serializedValue = InstantiationUtil.serializeObject(list.get(i));
				serializedGroupValues.add(serializedValue);
				runningGroupsOffset += serializedValue.length;
			}
		}

		//write all generated values in a single byte array, which is index by groupOffsetsInFinalByteArray
		byte[] allSerializedValuesConcatenated = new byte[runningGroupsOffset];
		runningGroupsOffset = 0;
		for (byte[] serializedGroupValue : serializedGroupValues) {
			System.arraycopy(
					serializedGroupValue,
					0,
					allSerializedValuesConcatenated,
					runningGroupsOffset,
					serializedGroupValue.length);
			runningGroupsOffset += serializedGroupValue.length;
		}
		return new Tuple2<>(allSerializedValuesConcatenated, offsets);
	}

	public static OperatorStateHandle generatePartitionableStateHandle(
		JobVertexID jobVertexID,
		int index,
		int namedStates,
		int partitionsPerState,
		boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return generatePartitionableStateHandle(statesListsMap);
	}

	public static ChainedStateHandle<OperatorStateHandle> generateChainedPartitionableStateHandle(
			JobVertexID jobVertexID,
			int index,
			int namedStates,
			int partitionsPerState,
			boolean rawState) throws IOException {

		Map<String, List<? extends Serializable>> statesListsMap = new HashMap<>(namedStates);

		for (int i = 0; i < namedStates; ++i) {
			List<Integer> testStatesLists = new ArrayList<>(partitionsPerState);
			// generate state
			int seed = jobVertexID.hashCode() * index + i * namedStates;
			if (rawState) {
				seed = (seed + 1) * 31;
			}
			Random random = new Random(seed);
			for (int j = 0; j < partitionsPerState; ++j) {
				int simulatedStateValue = random.nextInt();
				testStatesLists.add(simulatedStateValue);
			}
			statesListsMap.put("state-" + i, testStatesLists);
		}

		return ChainedStateHandle.wrapSingleHandle(generatePartitionableStateHandle(statesListsMap));
	}

	private static OperatorStateHandle generatePartitionableStateHandle(
		Map<String, List<? extends Serializable>> states) throws IOException {

		List<List<? extends Serializable>> namedStateSerializables = new ArrayList<>(states.size());

		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			namedStateSerializables.add(entry.getValue());
		}

		Tuple2<byte[], List<long[]>> serializationWithOffsets = serializeTogetherAndTrackOffsets(namedStateSerializables);

		Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(states.size());

		int idx = 0;
		for (Map.Entry<String, List<? extends Serializable>> entry : states.entrySet()) {
			offsetsMap.put(
				entry.getKey(),
				new OperatorStateHandle.StateMetaInfo(
					serializationWithOffsets.f1.get(idx),
					OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
			++idx;
		}

		ByteStreamStateHandle streamStateHandle = new ByteStreamStateHandle(
			String.valueOf(UUID.randomUUID()),
			serializationWithOffsets.f0);

		return new OperatorStreamStateHandle(offsetsMap, streamStateHandle);
	}

	static ExecutionJobVertex mockExecutionJobVertex(
			JobVertexID jobVertexID,
			int parallelism,
			int maxParallelism) {

		return mockExecutionJobVertex(
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			parallelism,
			maxParallelism
		);
	}

	static ExecutionJobVertex mockExecutionJobVertex(
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism) {
		final ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		ExecutionVertex[] executionVertices = new ExecutionVertex[parallelism];

		for (int i = 0; i < parallelism; i++) {
			executionVertices[i] = mockExecutionVertex(
				new ExecutionAttemptID(),
				jobVertexID,
				jobVertexIDs,
				parallelism,
				maxParallelism,
				ExecutionState.RUNNING);

			when(executionVertices[i].getParallelSubtaskIndex()).thenReturn(i);
		}

		when(executionJobVertex.getJobVertexId()).thenReturn(jobVertexID);
		when(executionJobVertex.getTaskVertices()).thenReturn(executionVertices);
		when(executionJobVertex.getParallelism()).thenReturn(parallelism);
		when(executionJobVertex.getMaxParallelism()).thenReturn(maxParallelism);
		when(executionJobVertex.isMaxParallelismConfigured()).thenReturn(true);
		when(executionJobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);
		when(executionJobVertex.getUserDefinedOperatorIDs()).thenReturn(Arrays.asList(new OperatorID[jobVertexIDs.size()]));

		return executionJobVertex;
	}

	static ExecutionVertex mockExecutionVertex(ExecutionAttemptID attemptID) {
		JobVertexID jobVertexID = new JobVertexID();
		return mockExecutionVertex(
			attemptID,
			jobVertexID,
			Collections.singletonList(OperatorID.fromJobVertexID(jobVertexID)),
			1,
			1,
			ExecutionState.RUNNING);
	}

	private static ExecutionVertex mockExecutionVertex(
		ExecutionAttemptID attemptID,
		JobVertexID jobVertexID,
		List<OperatorID> jobVertexIDs,
		int parallelism,
		int maxParallelism,
		ExecutionState state,
		ExecutionState ... successiveStates) {

		ExecutionVertex vertex = mock(ExecutionVertex.class);

		final Execution exec = spy(new Execution(
			mock(Executor.class),
			vertex,
			1,
			1L,
			1L,
			Time.milliseconds(500L)
		));
		when(exec.getAttemptId()).thenReturn(attemptID);
		when(exec.getState()).thenReturn(state, successiveStates);

		final Execution standbyExec = spy(new Execution(
			mock(Executor.class),
			vertex,
			1,
			1L,
			1L,
			Time.milliseconds(500L)
		));
		when(standbyExec.getAttemptId()).thenReturn(attemptID);
		// However, the following is useless because setInitialState() references the state field directly.
		// For a new Execution object state equals to CREATED always.
		when(standbyExec.getState()).thenReturn(ExecutionState.STANDBY);

		when(vertex.getJobvertexId()).thenReturn(jobVertexID);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(maxParallelism);
		when(vertex.getStandbyExecutions()).thenReturn(new ArrayList<Execution>(Arrays.asList(standbyExec)));

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getOperatorIDs()).thenReturn(jobVertexIDs);
		
		when(vertex.getJobVertex()).thenReturn(jobVertex);

		return vertex;
	}

	private static ExecutionGraph mockExecutionGraph(Map<JobVertexID, ExecutionJobVertex> vertices, FailoverStrategy.Factory failoverStrategyFactory) {
		ExecutionGraph executionGraph = mock(ExecutionGraph.class);
		when(executionGraph.getFutureExecutor()).thenReturn(TestingUtils.defaultExecutor());
		FailoverStrategy failoverStrategy = failoverStrategyFactory.create(executionGraph);
		when(executionGraph.getAllVertices()).thenReturn(vertices);
		when(executionGraph.getFailoverStrategy()).thenReturn(failoverStrategy);
		return executionGraph;
	}

	static TaskStateSnapshot mockSubtaskState(
		JobVertexID jobVertexID,
		int index,
		KeyGroupRange keyGroupRange) throws IOException {

		OperatorStateHandle partitionableState = generatePartitionableStateHandle(jobVertexID, index, 2, 8, false);
		KeyGroupsStateHandle partitionedKeyGroupState = generateKeyGroupState(jobVertexID, keyGroupRange, false);

		TaskStateSnapshot subtaskStates = spy(new TaskStateSnapshot());
		OperatorSubtaskState subtaskState = spy(new OperatorSubtaskState(
			partitionableState, null, partitionedKeyGroupState, null)
		);

		subtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID), subtaskState);

		return subtaskStates;
	}

	public static void verifyStateDispatch(
			JobVertexID jobVertexID, ExecutionJobVertex executionJobVertex,
			List<KeyGroupRange> keyGroupPartitions) throws Exception {

		for (int i = 0; i < executionJobVertex.getParallelism(); i++) {

			Execution standbyExecution = executionJobVertex.getTaskVertices()[i].getStandbyExecutions().get(0);
			JobManagerTaskRestore taskRestore = standbyExecution.getTaskRestore();

			Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
			TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

			OperatorSubtaskState operatorState = stateSnapshot.getSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID));

			ChainedStateHandle<OperatorStateHandle> expectedOpStateBackend =
					generateChainedPartitionableStateHandle(jobVertexID, i, 2, 8, false);

			assertTrue(CommonTestUtils.isSteamContentEqual(
					expectedOpStateBackend.get(0).openInputStream(),
					operatorState.getManagedOperatorState().iterator().next().openInputStream()));

			KeyGroupsStateHandle expectPartitionedKeyGroupState = generateKeyGroupState(
					jobVertexID, keyGroupPartitions.get(i), false);
			compareKeyedState(Collections.singletonList(expectPartitionedKeyGroupState), operatorState.getManagedKeyedState());
		}
	}

	public static void compareKeyedState(
			Collection<KeyGroupsStateHandle> expectPartitionedKeyGroupState,
			Collection<? extends KeyedStateHandle> actualPartitionedKeyGroupState) throws Exception {

		KeyGroupsStateHandle expectedHeadOpKeyGroupStateHandle = expectPartitionedKeyGroupState.iterator().next();
		int expectedTotalKeyGroups = expectedHeadOpKeyGroupStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		int actualTotalKeyGroups = 0;
		for(KeyedStateHandle keyedStateHandle: actualPartitionedKeyGroupState) {
			assertTrue(keyedStateHandle instanceof KeyGroupsStateHandle);

			actualTotalKeyGroups += keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups();
		}

		assertEquals(expectedTotalKeyGroups, actualTotalKeyGroups);

		try (FSDataInputStream inputStream = expectedHeadOpKeyGroupStateHandle.openInputStream()) {
			for (int groupId : expectedHeadOpKeyGroupStateHandle.getKeyGroupRange()) {
				long offset = expectedHeadOpKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
				inputStream.seek(offset);
				int expectedKeyGroupState =
						InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
				for (KeyedStateHandle oneActualKeyedStateHandle : actualPartitionedKeyGroupState) {

					assertTrue(oneActualKeyedStateHandle instanceof KeyGroupsStateHandle);

					KeyGroupsStateHandle oneActualKeyGroupStateHandle = (KeyGroupsStateHandle) oneActualKeyedStateHandle;
					if (oneActualKeyGroupStateHandle.getKeyGroupRange().contains(groupId)) {
						long actualOffset = oneActualKeyGroupStateHandle.getOffsetForKeyGroup(groupId);
						try (FSDataInputStream actualInputStream = oneActualKeyGroupStateHandle.openInputStream()) {
							actualInputStream.seek(actualOffset);
							int actualGroupState = InstantiationUtil.
									deserializeObject(actualInputStream, Thread.currentThread().getContextClassLoader());
							assertEquals(expectedKeyGroupState, actualGroupState);
						}
					}
				}
			}
		}
	}

	public static void comparePartitionableState(
			List<ChainedStateHandle<OperatorStateHandle>> expected,
			List<List<Collection<OperatorStateHandle>>> actual) throws Exception {

		List<String> expectedResult = new ArrayList<>();
		for (ChainedStateHandle<OperatorStateHandle> chainedStateHandle : expected) {
			for (int i = 0; i < chainedStateHandle.getLength(); ++i) {
				OperatorStateHandle operatorStateHandle = chainedStateHandle.get(i);
				collectResult(i, operatorStateHandle, expectedResult);
			}
		}
		Collections.sort(expectedResult);

		List<String> actualResult = new ArrayList<>();
		for (List<Collection<OperatorStateHandle>> collectionList : actual) {
			if (collectionList != null) {
				for (int i = 0; i < collectionList.size(); ++i) {
					Collection<OperatorStateHandle> stateHandles = collectionList.get(i);
					Assert.assertNotNull(stateHandles);
					for (OperatorStateHandle operatorStateHandle : stateHandles) {
						collectResult(i, operatorStateHandle, actualResult);
					}
				}
			}
		}

		Collections.sort(actualResult);
		Assert.assertEquals(expectedResult, actualResult);
	}

	private static void collectResult(int opIdx, OperatorStateHandle operatorStateHandle, List<String> resultCollector) throws Exception {
		try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {
				for (long offset : entry.getValue().getOffsets()) {
					in.seek(offset);
					Integer state = InstantiationUtil.
							deserializeObject(in, Thread.currentThread().getContextClassLoader());
					resultCollector.add(opIdx + " : " + entry.getKey() + " : " + state);
				}
			}
		}
	}
}
