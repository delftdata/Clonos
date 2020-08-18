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
package org.apache.flink.runtime.causal.determinant;


public final class OrderDeterminant extends Determinant {

	private byte channel;

	public OrderDeterminant() {

	}

	public OrderDeterminant(byte channel) {
		this.channel = channel;
	}


	public byte getChannel() {
		return channel;
	}

	public OrderDeterminant replace(byte channel) {
		this.channel = channel;
		return this;
	}

	@Override
	public String toString() {
		return "OrderDeterminant{" +
			"channel=" + channel +
			'}';
	}

	@Override
	public int getEncodedSizeInBytes() {
		return super.getEncodedSizeInBytes() + Byte.BYTES;
	}

	public static byte getTypeTag() {
		return ORDER_DETERMINANT_TAG;
	}
}
