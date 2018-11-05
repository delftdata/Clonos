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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionVertexSchedulingTest {

	@Test
	public void testSlotReleasedWhenScheduledImmediately() {
		try {
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			// a slot than cannot be deployed to
			final Instance instance = getInstance(new ActorTaskManagerGateway(DummyActorGateway.INSTANCE));
			final SimpleSlot slot = instance.allocateSimpleSlot();
			
			slot.releaseSlot();
			assertTrue(slot.isReleased());

			Scheduler scheduler = mock(Scheduler.class);
			CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
			future.complete(slot);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false, LocationPreferenceConstraint.ALL);

			// will have failed
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlotReleasedWhenScheduledQueued() {
		try {
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			// a slot than cannot be deployed to
			final Instance instance = getInstance(new ActorTaskManagerGateway(DummyActorGateway.INSTANCE));
			final SimpleSlot slot = instance.allocateSimpleSlot();

			slot.releaseSlot();
			assertTrue(slot.isReleased());

			final CompletableFuture<LogicalSlot> future = new CompletableFuture<>();

			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, true, LocationPreferenceConstraint.ALL);

			// future has not yet a slot
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());

			future.complete(slot);

			// will have failed
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testScheduleToDeploying() {
		try {
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final Instance instance = getInstance(new ActorTaskManagerGateway(
				new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.defaultExecutionContext())));
			final SimpleSlot slot = instance.allocateSimpleSlot();

			Scheduler scheduler = mock(Scheduler.class);
			CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
			future.complete(slot);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false, LocationPreferenceConstraint.ALL);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRunStandbyExecutionEmpty() throws Exception {
		try {
			Scheduler scheduler = mock(Scheduler.class);
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID(), scheduler);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// Try to run standby execution without having created one.
			vertex.runStandbyExecution();
			fail("Exception expected");
		}
		catch (IllegalStateException e) {
			String message = new String("No standby execution to run.");
			assertThat(e.getMessage(), is(message));
		}
	}

	@Test
	public void testRunStandbyExecutionNotReady() throws Exception {
		try {
			final Instance instance = getInstance(new ActorTaskManagerGateway(
				new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.defaultExecutionContext())));
			SimpleSlot slot = instance.allocateSimpleSlot();

			Scheduler scheduler = mock(Scheduler.class);
			CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
			future.complete(slot);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID(), scheduler);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// Try to deploy a standby execution.
			vertex.addStandbyExecution();

			// Try to run it although it is in DEPLOYING state.
			vertex.runStandbyExecution();
			fail ("Exception expected");
		}
		catch (IllegalStateException e) {
			String message = new String("Tried to run a standby execution that is not in STANDBY state, but in DEPLOYING state.");
			assertThat(e.getMessage(), is(message));
		}
	}

	@Test
	public void testRunStandbyExecution() throws Exception {
		try {
			final Instance instance = getInstance(new ActorTaskManagerGateway(
				new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.defaultExecutionContext())));
			SimpleSlot slot = instance.allocateSimpleSlot();

			Scheduler scheduler = mock(Scheduler.class);
			CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
			future.complete(slot);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID(), scheduler);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// Try to deploy a standby execution.
			vertex.addStandbyExecution();
			ArrayList<Execution> standbyExecutions = vertex.getStandbyExecutions();
			assertThat(standbyExecutions.size(), is(1));
			Execution thisStandbyExecution = standbyExecutions.get(0);
			assertThat(thisStandbyExecution.getIsStandby(), is(true));
			assertThat(thisStandbyExecution.getState(), is(ExecutionState.DEPLOYING));

			thisStandbyExecution.setState(ExecutionState.STANDBY);
			assertThat(thisStandbyExecution.getState(), is(ExecutionState.STANDBY));

			// Standby task now in STANDBY state. Try to run it.
			vertex.runStandbyExecution();
		}
		catch (IllegalStateException e) {
			e.printStackTrace();
			fail (e.getMessage());
		}
	}

	@Test
	public void testAddStandbyExecution() {
		try {
			final Instance instance = getInstance(new ActorTaskManagerGateway(
				new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.defaultExecutionContext())));
			SimpleSlot slot = instance.allocateSimpleSlot();

			Scheduler scheduler = mock(Scheduler.class);
			CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
			future.complete(slot);
			when(scheduler.allocateSlot(any(SlotRequestId.class), any(ScheduledUnit.class), anyBoolean(), any(SlotProfile.class), any(Time.class))).thenReturn(future);

			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID(), scheduler);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// Try to deploy a standby execution.
			vertex.addStandbyExecution();

			ArrayList<Execution> standbyExecutions = vertex.getStandbyExecutions();
			assertThat(standbyExecutions.size(), is(1));

			Execution thisStandbyExecution = standbyExecutions.get(0);
			assertThat(thisStandbyExecution.getIsStandby(), is(true));
			assertThat(thisStandbyExecution.getState(), is(ExecutionState.DEPLOYING));

			Map<ExecutionAttemptID, Execution> currentExecutions = ejv.getGraph().getRegisteredExecutions();
			Execution getThisStandbyExecutionFromRegistered =
				currentExecutions.get(thisStandbyExecution.getAttemptId());
			assertNotNull(getThisStandbyExecutionFromRegistered);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
