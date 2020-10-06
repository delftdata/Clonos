/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.ProcessingTimeForceable;
import org.apache.flink.runtime.causal.RecordCountProvider;
import org.apache.flink.runtime.causal.determinant.ProcessingTimeCallbackID;
import org.apache.flink.runtime.causal.determinant.TimerTriggerDeterminant;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.recovery.RecoveryManager;
import org.apache.flink.api.common.services.TimeService;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ProcessingTimeService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
public class SystemProcessingTimeService extends ProcessingTimeService implements ProcessingTimeForceable {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);

	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;
	private static final int STATUS_SHUTDOWN = 2;

	// ------------------------------------------------------------------------

	/**
	 * The containing task that owns this time service provider.
	 */
	private final AsyncExceptionHandler task;

	/**
	 * The lock that timers acquire upon triggering.
	 */
	private final Object checkpointLock;

	/**
	 * The executor service that schedules and calls the triggers of this task.
	 */
	private final ScheduledThreadPoolExecutor timerService;

	private final AtomicInteger status;

	private final TimeService timeService;
	private final EpochProvider epochProvider;
	private final RecordCountProvider recordCountProvider;
	private final ThreadCausalLog mainThreadCausalLog;
	private final RecoveryManager recoveryManager;
	private TimerTriggerDeterminant reuseTimerTriggerDeterminant;

	private final Map<ProcessingTimeCallbackID, PreregisteredTimer> preregisteredTimerTasks;


	public SystemProcessingTimeService(AsyncExceptionHandler failureHandler, Object checkpointLock) {
		this(failureHandler, checkpointLock, null);
	}

	public <OUT> SystemProcessingTimeService(
		AsyncExceptionHandler task,
		Object checkpointLock,
		ThreadFactory threadFactory) {
		this(task, checkpointLock, threadFactory, null, null, null, null, null);
	}

	public <OUT> SystemProcessingTimeService(AsyncExceptionHandler task, Object checkpointLock, ThreadFactory threadFactory, TimeService timeService, EpochProvider epochProvider, RecordCountProvider recordCountProvider, JobCausalLog causalLog, RecoveryManager recoveryManager) {
		this.task = checkNotNull(task);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.timeService = timeService;

		this.epochProvider = epochProvider;
		this.recordCountProvider = recordCountProvider;
		this.mainThreadCausalLog = causalLog.getThreadCausalLog(new CausalLogID(recoveryManager.getTaskVertexID().getVertexID()));
		this.recoveryManager = recoveryManager;

		this.preregisteredTimerTasks = new HashMap<>();
		this.reuseTimerTriggerDeterminant = new TimerTriggerDeterminant();

		this.status = new AtomicInteger(STATUS_ALIVE);

		if (threadFactory == null) {
			this.timerService = new ScheduledThreadPoolExecutor(1);
		} else {
			this.timerService = new ScheduledThreadPoolExecutor(1, threadFactory);
		}

		// tasks should be removed if the future is canceled
		this.timerService.setRemoveOnCancelPolicy(true);

		// make sure shutdown removes all pending tasks
		this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

	}

	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	@Override
	public long getCurrentProcessingTimeCausal() {
		return timeService.currentTimeMillis();
	}

	/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time)
	 * @param target    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 * even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;
		ScheduledFuture<?> future;
		TriggerTask toRegister = new TriggerTask(status, task, checkpointLock, target, timestamp, mainThreadCausalLog,
			epochProvider, recordCountProvider, reuseTimerTriggerDeterminant);
		if (recoveryManager.isRunning())
			future = registerTimerRunning(toRegister, delay);
		else
			future = registerTimerRecovering(toRegister, delay);

		return future;
	}

	private ScheduledFuture<?> registerTimerRecovering(TriggerTask toRegister, long delay) {
		LOG.debug("We are recovering, differing one-shot timer registration!");
		long submissionTime = getCurrentProcessingTime();
		ProcessingTimeCallbackID id = toRegister.getTarget().getID();
		preregisteredTimerTasks.put(id, new PreregisteredTimer(toRegister, delay, submissionTime));
		return new PreregisteredCompleteableFuture<>(id);
	}

	private ScheduledFuture<?> registerTimerRunning(TriggerTask toRegister, long delay) {
		LOG.debug("We are running, directly registering one-shot timer!");
		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.schedule(
				toRegister, delay, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);
			} else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			} else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		long nextTimestamp = getCurrentProcessingTime() + initialDelay;

		RepeatedTriggerTask toRegister = new RepeatedTriggerTask(status, task, checkpointLock, callback, nextTimestamp,
			period, mainThreadCausalLog, epochProvider, recordCountProvider, reuseTimerTriggerDeterminant);
		ScheduledFuture<?> future;
		if (recoveryManager.isRunning())
			future = registerAtFixedRateRunning(initialDelay, period, toRegister);
		else
			future = registerAtFixedRateRecovering(initialDelay, period, toRegister);
		return future;
	}

	private ScheduledFuture<?> registerAtFixedRateRecovering(long initialDelay, long period, RepeatedTriggerTask toRegister) {
		LOG.debug("We are recovering, differing fixed rate timer registration!");
		long submissionTime = getCurrentProcessingTime();
		ProcessingTimeCallbackID id = toRegister.getTarget().getID();
		preregisteredTimerTasks.put(id, new PreregisteredTimer(toRegister, initialDelay, submissionTime));
		return new PreregisteredCompleteableFuture<>(id);

	}

	private ScheduledFuture<?> registerAtFixedRateRunning(long initialDelay, long period, RepeatedTriggerTask toRegister) {
		LOG.debug("We are running, directly registering fixed rate timer!");
		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.scheduleAtFixedRate(
				toRegister,
				initialDelay,
				period,
				TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			} else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			} else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	/**
	 * @return {@code true} is the status of the service
	 * is {@link #STATUS_ALIVE}, {@code false} otherwise.
	 */
	@VisibleForTesting
	boolean isAlive() {
		return status.get() == STATUS_ALIVE;
	}

	@Override
	public boolean isTerminated() {
		return status.get() == STATUS_SHUTDOWN;
	}

	@Override
	public void quiesce() throws InterruptedException {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}
	}

	@Override
	public void awaitPendingAfterQuiesce() throws InterruptedException {
		if (!timerService.isTerminated()) {
			Preconditions.checkState(timerService.isTerminating() || timerService.isShutdown());

			// await forever (almost)
			timerService.awaitTermination(365L, TimeUnit.DAYS);
		}
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
			status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	@Override
	public boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return timerService.awaitTermination(time, timeUnit);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

		boolean shutdownComplete = false;
		boolean receivedInterrupt = false;

		do {
			try {
				// wait for a reasonable time for all pending timer threads to finish
				shutdownComplete = shutdownAndAwaitPending(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException iex) {
				receivedInterrupt = true;
				LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
			}
		} while (deadline.hasTimeLeft() && !shutdownComplete);

		if (receivedInterrupt) {
			Thread.currentThread().interrupt();
		}

		return shutdownComplete;
	}

	// safety net to destroy the thread pool
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		timerService.shutdownNow();
	}

	@VisibleForTesting
	int getNumTasksScheduled() {
		BlockingQueue<?> queue = timerService.getQueue();
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	@Override
	public void forceExecution(ProcessingTimeCallbackID id, long timestamp) {
		LOG.debug("Forcing execution of task with callback id {} for timestamp {}", id, timestamp);
		PreregisteredTimer timerTask = preregisteredTimerTasks.get(id);
		if (timerTask == null)
			throw new RuntimeException("Timer not found during recovery");
		Runnable runnable = timerTask.getTask();

		if (runnable instanceof TriggerTask) {
			preregisteredTimerTasks.remove(id);
			((TriggerTask) runnable).runTask(timestamp);
		} else if (runnable instanceof RepeatedTriggerTask) {
			((RepeatedTriggerTask) runnable).runTask(timestamp);
		}else {
			throw new RuntimeException("Unknown timer task type or null");
		}


	}

	@Override
	public void concludeReplay() {
		LOG.info("Concluded replay, moving preregistered timers to main registry");
		for (PreregisteredTimer preregisteredTimer : preregisteredTimerTasks.values()) {
			LOG.debug("Preregistered timer: {}", preregisteredTimer);
			if (preregisteredTimer.task instanceof TriggerTask)
				registerTimerRunning((TriggerTask) preregisteredTimer.task, preregisteredTimer.delay);
			else if (preregisteredTimer.task instanceof RepeatedTriggerTask)//register using period as delay, since it has been executed a few times already
				registerAtFixedRateRunning(((RepeatedTriggerTask) preregisteredTimer.task).period, ((RepeatedTriggerTask) preregisteredTimer.task).period, (RepeatedTriggerTask) preregisteredTimer.task);
		}
		preregisteredTimerTasks.clear();
	}

	// ------------------------------------------------------------------------

	/**
	 * Internal task that is invoked by the timer service and triggers the target.
	 */
	private static final class TriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long timestamp;
		private final AsyncExceptionHandler exceptionHandler;

		private final RecordCountProvider recordCountProvider;
		private final EpochProvider epochProvider;
		private final ThreadCausalLog causalLog;
		private final TimerTriggerDeterminant timerTriggerDeterminantToUse;


		private TriggerTask(
			final AtomicInteger serviceStatus,
			final AsyncExceptionHandler exceptionHandler,
			final Object lock,
			final ProcessingTimeCallback target,
			final long timestamp,
			final ThreadCausalLog causalLog,
			final EpochProvider epochProvider,
			final RecordCountProvider recordCountProvider,
			final TimerTriggerDeterminant toUse) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.timestamp = timestamp;
			this.causalLog = causalLog;
			this.epochProvider = epochProvider;
			this.recordCountProvider = recordCountProvider;
			this.timerTriggerDeterminantToUse = toUse;
		}


		@Override
		public void run() {
			runTask(this.timestamp);
		}

		public void runTask(long timestamp) {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {
						causalLog.appendDeterminant(
							timerTriggerDeterminantToUse.replace(
								recordCountProvider.getRecordCount(),
								target.getID(),
								timestamp),
							epochProvider.getCurrentEpochID());
						target.onProcessingTime(timestamp);
					}
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing timer.", asyncException);
				}
			}
		}

		public ProcessingTimeCallback getTarget() {
			return target;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}

	/**
	 * Internal task which is repeatedly called by the processing time service.
	 */
	private static final class RepeatedTriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long period;
		private final AsyncExceptionHandler exceptionHandler;
		private final TimerTriggerDeterminant timerTriggerDeterminantToUse;

		private long nextTimestamp;

		private final RecordCountProvider recordCountProvider;
		private final EpochProvider epochProvider;
		private final ThreadCausalLog causalLog;

		private RepeatedTriggerTask(
			final AtomicInteger serviceStatus,
			final AsyncExceptionHandler exceptionHandler,
			final Object lock,
			final ProcessingTimeCallback target,
			final long nextTimestamp,
			final long period,
			final ThreadCausalLog causalLog,
			final EpochProvider epochProvider,
			final RecordCountProvider recordCountProvider,
			final TimerTriggerDeterminant toUse) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.period = period;
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);

			this.nextTimestamp = nextTimestamp;
			this.causalLog = causalLog;
			this.epochProvider = epochProvider;
			this.recordCountProvider = recordCountProvider;
			this.timerTriggerDeterminantToUse = toUse;
		}

		@Override
		public void run() {
			runTask(this.nextTimestamp);
		}

		private void runTask(long timestamp) {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {
						causalLog.appendDeterminant(
							timerTriggerDeterminantToUse.replace(
								recordCountProvider.getRecordCount(),
								target.getID(),
								timestamp),
							epochProvider.getCurrentEpochID());
						target.onProcessingTime(nextTimestamp);
					}

					nextTimestamp = timestamp + period;
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing repeated timer task.", asyncException);
				}
			}
		}

		public ProcessingTimeCallback getTarget() {
			return target;
		}

		public long getPeriod() {
			return period;
		}

		public long getNextTimestamp() {
			return nextTimestamp;
		}
	}

	// ------------------------------------------------------------------------

	private static final class NeverCompleteFuture implements ScheduledFuture<Object> {

		private final Object lock = new Object();

		private final long delayMillis;

		private volatile boolean canceled;

		private NeverCompleteFuture(long delayMillis) {
			this.delayMillis = delayMillis;
		}

		@Override
		public long getDelay(@Nonnull TimeUnit unit) {
			return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(@Nonnull Delayed o) {
			long otherMillis = o.getDelay(TimeUnit.MILLISECONDS);
			return Long.compare(this.delayMillis, otherMillis);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (lock) {
				canceled = true;
				lock.notifyAll();
			}
			return true;
		}

		@Override
		public boolean isCancelled() {
			return canceled;
		}

		@Override
		public boolean isDone() {
			return false;
		}

		@Override
		public Object get() throws InterruptedException {
			synchronized (lock) {
				while (!canceled) {
					lock.wait();
				}
			}
			throw new CancellationException();
		}

		@Override
		public Object get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, TimeoutException {
			synchronized (lock) {
				while (!canceled) {
					unit.timedWait(lock, timeout);
				}

				if (canceled) {
					throw new CancellationException();
				} else {
					throw new TimeoutException();
				}
			}
		}
	}

	private class PreregisteredCompleteableFuture<T> implements ScheduledFuture<T> {

		private final ProcessingTimeCallbackID callbackID;

		public PreregisteredCompleteableFuture(ProcessingTimeCallbackID id) {
			this.callbackID = id;
		}

		@Override
		public long getDelay(TimeUnit timeUnit) {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public int compareTo(Delayed delayed) {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public boolean cancel(boolean b) {
			preregisteredTimerTasks.remove(callbackID);
			return true;
		}

		@Override
		public boolean isCancelled() {
			return preregisteredTimerTasks.containsKey(this.callbackID);
		}

		@Override
		public boolean isDone() {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
			throw new UnsupportedOperationException("Not Implemented");
		}
	}

	private static class PreregisteredTimer {
		Runnable task;
		long delay;
		long submissionTime;

		public PreregisteredTimer(Runnable task, long delay, long submissionTime) {
			this.task = task;
			this.delay = delay;
			this.submissionTime = submissionTime;
		}

		public Runnable getTask() {
			return task;
		}

		public long getDelay() {
			return delay;
		}

		public long getSubmissionTime() {
			return submissionTime;
		}
	}
}
