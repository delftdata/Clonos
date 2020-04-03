package org.apache.flink.runtime.event;

import org.apache.flink.runtime.causal.CausalLoggingManager;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.util.event.EventListener;

public class DeterminantResponseEventListener implements EventListener<TaskEvent> {

	private final ClassLoader userCodeClassLoader;

	private CausalLoggingManager causalLoggingManager;

	public DeterminantResponseEventListener(ClassLoader userCodeClassLoader, CausalLoggingManager causalLoggingManager) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.causalLoggingManager = causalLoggingManager;
	}

	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof DeterminantResponseEvent) {
			this.causalLoggingManager.getRecoveryManager().processDeterminantResponseEvent((DeterminantResponseEvent)event);
		}
		else {
			throw new IllegalArgumentException(String.format("Unknown event type: %s.", event));
		}

	}
}
