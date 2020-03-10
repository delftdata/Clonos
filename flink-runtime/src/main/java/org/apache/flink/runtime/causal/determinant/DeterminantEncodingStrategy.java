package org.apache.flink.runtime.causal.determinant;


import java.util.List;

/**
 * Encoding strategy for determinants. Takes a determinant and returns its representation as bytes.
 * The tag must always be written first, as this allows the determinant to later be decoded.
 */
public interface DeterminantEncodingStrategy {

	byte[] encode(Determinant determinant);

	List<Determinant> decode(byte[] determinant);

}
