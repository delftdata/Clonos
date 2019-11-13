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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSlice<T> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamSlice.class);

	private String sliceId;
	private List<T> sliceRecords;

	public StreamSlice(String sliceId) {
		this.sliceId = sliceId;
		this.sliceRecords = new ArrayList<>(); // TODO check what is a sane initial size
	}

	public StreamSlice(String sliceId, List<T> sliceRecords) {
		this.sliceId = sliceId;
		this.sliceRecords = sliceRecords;
	}

	public String getSliceId() {
		return sliceId;
	}

	public void setSliceId(String sliceId) {
		this.sliceId = sliceId;
	}

	public List<T> getSliceRecords() {
		return sliceRecords;
	}

	public void setSliceRecords(List<T> sliceRecords) {
		this.sliceRecords = sliceRecords;
	}

	public void addRecord(T newRecord) {
		this.sliceRecords.add(newRecord);
		LOG.debug("Logged record {} to position {} of StreamSlice for checkpoint id {}.", newRecord, this.sliceRecords.size() - 1, sliceId);
	}
}
