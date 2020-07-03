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

package org.apache.flink.runtime.causal.determinant;

import java.util.Objects;

public class ProcessingTimeCallbackID{

	public enum Type {
		WATERMARK, TIMESTAMP_EXTRACTOR, TIMESTAMP_PERIODIC_WATERMARK_EXTRACTOR, TIMESTAMP_PUNCTUATED_WATERMARK_EXTRACTOR, IDLE, LATENCY, INTERNAL
	}

	private Type type;
	private String name;

	public ProcessingTimeCallbackID(){}

	public ProcessingTimeCallbackID(Type type) {
		this.type = type;
	}

	public ProcessingTimeCallbackID(String name) {
		this.type = Type.INTERNAL;
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public String getName(){return name;}

	public ProcessingTimeCallbackID replace(Type type){
		this.name = null;
		this.type = type;
		return this;
	}

	public ProcessingTimeCallbackID replace(String name){
		this.type = Type.INTERNAL;
		this.name = name;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ProcessingTimeCallbackID that = (ProcessingTimeCallbackID) o;
		return getType() == that.getType() &&
			Objects.equals(getName(), that.getName());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getType(), getName());
	}

	@Override
	public String toString() {
		return "ProcessingTimeCallbackID{" +
			(type.equals(Type.INTERNAL) ? "name="+ name : "type=" + type.name()) +
			'}';
	}
}
