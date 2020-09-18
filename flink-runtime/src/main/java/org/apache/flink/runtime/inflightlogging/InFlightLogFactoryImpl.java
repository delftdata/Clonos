/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

import java.io.IOException;

public class InFlightLogFactoryImpl implements InFlightLogFactory {
	private final IOManager ioManager;
	private final InFlightLogConfig config;
	private final NetworkBufferPool networkBufferPool;

	public InFlightLogFactoryImpl(InFlightLogConfig config, IOManager ioManager, NetworkBufferPool networkBufferPool) {
		this.config = config;
		this.ioManager = ioManager;
		this.networkBufferPool = networkBufferPool;
	}

	@Override
	public InFlightLog build() {
		switch (config.getType()) {
			case SPILLABLE:
				BufferPool recoveryBufferPool = null;
				try {
					recoveryBufferPool = networkBufferPool.createBufferPool(config.getNumberOfRecoveryBuffers(),
						config.getNumberOfRecoveryBuffers());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				if(config.getSpillPolicy() == InFlightLogConfig.Policy.EAGER)
					return new SpillableSubpartitionInFlightLogger(ioManager, recoveryBufferPool, true);
				else
					return new SpillableSubpartitionInFlightLogger(ioManager, recoveryBufferPool, false);

			case IN_MEMORY:
			default:
				return new InMemorySubpartitionInFlightLogger();
		}
	}

	@Override
	public InFlightLogConfig getInFlightLogConfig() {
		return config;
	}

}
