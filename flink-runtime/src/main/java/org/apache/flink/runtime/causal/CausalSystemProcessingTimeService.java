package org.apache.flink.runtime.causal;

import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;

import java.util.concurrent.ThreadFactory;

public class CausalSystemProcessingTimeService extends SystemProcessingTimeService {

	private final CausalLoggingManager causalLoggingManager;

	public CausalSystemProcessingTimeService(AsyncExceptionHandler task, Object checkpointLock, ThreadFactory threadFactory, CausalLoggingManager causalLoggingManager) {
		super(task, checkpointLock, threadFactory);
		this.causalLoggingManager = causalLoggingManager;
	}


}
