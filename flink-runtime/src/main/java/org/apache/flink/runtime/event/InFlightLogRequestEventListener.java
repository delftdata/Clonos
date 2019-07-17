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

package org.apache.flink.runtime.event;

//import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;


/**
 * A listener to in-flight log requests.
 */
public class InFlightLogRequestEventListener implements EventListener<TaskEvent> {

	private final ClassLoader userCodeClassLoader;

	private boolean inFlightLogRequestSignalled = false;

	private int channelIndex = -1;

	public InFlightLogRequestEventListener(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
	}

	/** Setup the in-flight log request listener */
	public void setup() {

	}

	/** Barrier will turn the in-flight log request signal to true */
	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof InFlightLogRequestEvent) {
			inFlightLogRequestSignalled = true;
			InFlightLogRequestEvent inFlightLogRequestEvent = (InFlightLogRequestEvent) event;
			channelIndex = inFlightLogRequestEvent.getChannelIndex();
		}
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}
	}

	public int getInFlightLogRequestSignalledChannel() {
		return channelIndex;
	}

	public boolean inFlightLogRequestSignalled() {
		return inFlightLogRequestSignalled;
	}

	public void resetInFlightLogRequestSignalled() {
		inFlightLogRequestSignalled = false;
	}
}
