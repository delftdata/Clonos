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
package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.causal.DeterminantCarrier;
import org.apache.flink.runtime.causal.VertexCausalLogDelta;

import java.util.List;

public class DeterminantCarryingSerializationDelegate<T extends DeterminantCarrier> extends SerializationDelegate<T> implements DeterminantCarrier {

	public DeterminantCarryingSerializationDelegate(SerializationDelegate<T> serializationDelegate, boolean copyConstructor) {
		super(serializationDelegate, copyConstructor);
	}

	public DeterminantCarryingSerializationDelegate(TypeSerializer<T> serializer) {
		super(serializer);
	}

	@Override
	public void enrich(List<VertexCausalLogDelta> logDeltaList) {
		this.getInstance().enrich(logDeltaList);
	}

	@Override
	public List<VertexCausalLogDelta> getLogDeltas() {
		return this.getInstance().getLogDeltas();
	}
}
