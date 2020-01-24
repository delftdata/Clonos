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

import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSlice<T> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamSlice.class);

	private long sliceId;
	private List<T> sliceData;
	private CheckpointBarrier checkpointBarrier;

	public StreamSlice(long sliceId) {
		this.sliceId = sliceId;
		this.sliceData = new ArrayList<>(); // TODO check what is a sane initial size
	}

	public StreamSlice(long sliceId, List<T> sliceData) {
		this.sliceId = sliceId;
		this.sliceData = sliceData;
	}

	public long getSliceId() {
		return sliceId;
	}

	public void setSliceId(long sliceId) {
		this.sliceId = sliceId;
	}

	public List<T> getSliceData() {
		return sliceData;
	}

	public void setSliceData(List<T> sliceData) {
		this.sliceData = sliceData;
	}

	public void addData(T newData) {
		this.sliceData.add(newData);
		LOG.debug("Logged data {} to position {} of StreamSlice for checkpoint id {}.", newData, this.sliceData.size() - 1, sliceId);
	}

	public void setCheckpointBarrier(CheckpointBarrier checkpointBarrier) {
		this.checkpointBarrier = checkpointBarrier;
	}

	public CheckpointBarrier getCheckpointBarrier() {
		return checkpointBarrier;
	}

}
