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

import java.util.ListIterator;

public abstract class InFlightLogIterator<T> implements ListIterator<T> {

	/**
	 *
	 * @return the number of buffers still to be replayed
	 */
	public abstract int numberRemaining();

	/**
	 *
	 * @return the epoch currently being replayed
	 */
	public abstract long getEpoch();

	/**
	 * Returns , but does not remove, the next buffer to be replayed.
	 * Its reference count is not increased by this.
	 */
	public abstract T peekNext();

	/**
	 * Used to close iterator before it is done replaying or after it is done.
	 * Must decrement reference counts of remaining buffers
	 */
	public abstract void close();

	@Override
	public T previous() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasPrevious() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int nextIndex() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int previousIndex() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(T buffer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(T buffer) {
		throw new UnsupportedOperationException();
	}

}
