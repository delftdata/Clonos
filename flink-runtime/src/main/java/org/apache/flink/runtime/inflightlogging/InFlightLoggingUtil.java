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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;

public class InFlightLoggingUtil {

	public static void exchangeOwnership(Buffer buffer, BufferPool inFlightBufferPool, Object lock, boolean blocking){

		if(buffer.getRecycler() != FreeingBufferRecycler.INSTANCE){
			BufferRecycler owner = buffer.getRecycler();
			Buffer replacement = null;

			try {
				replacement = inFlightBufferPool.requestBuffer();

				while(replacement == null) {
					if(!blocking)
						return;
					lock.wait(100);
					replacement = inFlightBufferPool.requestBuffer();
				}
			}catch (Exception e){
				e.printStackTrace();
			}

			buffer.setRecycler(inFlightBufferPool);
			replacement.setRecycler(owner);
			replacement.recycleBuffer();
		}
	}
}
