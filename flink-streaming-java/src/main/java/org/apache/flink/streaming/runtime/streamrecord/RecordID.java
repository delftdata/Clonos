package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * The ID of a record in a Stream. Independent from AbstractID as these are fixed to a pair of longs
 */
public class RecordID implements Comparable<RecordID>, Serializable {

	private static final int NUMBER_OF_BYTES = 4; //32 bit
	private static final Random RND = new Random();
	private static final RecordIDMergingOperation merger = new XORRecordIDMergingOperation();

	private byte[] id;
	private String toString;

	/*
		Copies the first NUMBER_OF_BYTES bytes from the parameter id.
	 */
	public RecordID(byte[] id) {
		this.id = new byte[NUMBER_OF_BYTES];
		System.arraycopy(id, 0, this.id, 0, NUMBER_OF_BYTES);
	}

	public RecordID() {
		this.id = new byte[NUMBER_OF_BYTES];
		RND.nextBytes(id);
	}

	public byte[] getId() {
		return id;
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	// --------------------------------------------------------------------------------------------
	//  Standard Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			AbstractID that = (AbstractID) obj;
			return Arrays.equals(this.getId(), ((AbstractID) obj).getBytes());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.getId());
	}

	@Override
	public String toString() {
		if (this.toString == null) {
			this.toString = StringUtils.byteToHexString(this.getId());
		}

		return this.toString;
	}

	@Override
	public int compareTo(RecordID o) {
		//Arrays.equals is faster but does not help with ordering
		for (int i = NUMBER_OF_BYTES; i > 0; i--) {
			if (this.id[i] < o.getId()[i])
				return -1;
			else if (this.id[i] > o.getId()[i])
				return 1;
		}
		return 0;
	}

	// --------------------------------------------------------------------------------------------
	//  RecordID Utilities
	// --------------------------------------------------------------------------------------------

	public static RecordID merge(RecordID one, RecordID two) {
		return merger.merge(one, two);
	}

	private interface RecordIDMergingOperation {
		RecordID merge(RecordID one, RecordID two);

		RecordID mergeIntoFirst(RecordID one, RecordID two);
	}

	private static class XORRecordIDMergingOperation implements RecordIDMergingOperation {
		@Override
		public RecordID merge(RecordID one, RecordID two) {
			byte[] result = new byte[NUMBER_OF_BYTES];

			for (int i = 0; i < NUMBER_OF_BYTES; i++) {
				result[i] = (byte) (((int) one.getId()[i]) ^ ((int) two.getId()[i]));
			}
			return new RecordID(result);
		}

		@Override
		public RecordID mergeIntoFirst(RecordID one, RecordID two) {
			for (int i = 0; i < NUMBER_OF_BYTES; i++) {
				one.getId()[i] = (byte) (((int) one.getId()[i]) ^ ((int) two.getId()[i]));
			}
			return one;
		}
	}

	private static class ADDRecordIDMergingOperation implements RecordIDMergingOperation {
		@Override
		public RecordID merge(RecordID one, RecordID two) {
			byte[] result = new byte[NUMBER_OF_BYTES];

			for (int i = 0; i < NUMBER_OF_BYTES; i++) {
				result[i] = (byte) (one.getId()[i] + two.getId()[i]);
			}
			return new RecordID(result);
		}

		@Override
		public RecordID mergeIntoFirst(RecordID one, RecordID two) {
			for (int i = 0; i < NUMBER_OF_BYTES; i++) {
				one.getId()[i] = (byte) (((int) one.getId()[i]) + ((int) two.getId()[i]));
			}
			return one;
		}
	}

}
