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
package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.CheckpointListener;

/**
 * An InFlightLog records {@link Buffer} instances which have been sent to other tasks.
 * The processing of a checkpoint barrier n starts an epoch n, however the barrier itself belongs in epoch n-1.
 * Is also in charge of managing reference counts of buffers, as they are released in the network stack.
 * On checkpoint complete, truncates the log by deleting all epochs with an ID < checkpointID.
 * Epoch with ID checkpointID is saved as it starts after this checkpoint.
 * Decreases reference counts of stored buffers.
/*/
public interface InFlightLog extends CheckpointListener {

	/**
	 * Appends the provided buffer to the log slice of the provided epochID
	 */
	public void log(Buffer buffer, long epochID);



	/**
	 * Creates an Iterator starting at the provided epoch.
	 * Also increases the reference counts of stored buffers, as they are freed downstream in the network stack.
	 */
	public InFlightLogIterator<Buffer> getInFlightIterator(long epochID);

}
