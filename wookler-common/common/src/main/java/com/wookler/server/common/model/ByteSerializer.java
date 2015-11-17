/**
 * TODO: <comments>
 *
 * @file ByteSerializer.java
 * @author subho
 * @date 17-Nov-2015
 */
package com.wookler.server.common.model;

import java.io.IOException;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 17-Nov-2015
 */
public final class ByteSerializer implements Serializer<byte[]> {

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.model.Serializer#serialize(java.lang.Object
	 * )
	 */
	@Override
	public byte[] serialize(byte[] data) throws IOException {
		return data;
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.model.Serializer#deserialize(byte[])
	 */
	@Override
	public byte[] deserialize(byte[] data) throws IOException {
		return data;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.model.Serializer#accept(java.lang.Class)
	 */
	@Override
	public boolean accept(Class<?> type) {
		if (type.equals(byte[].class)) {
			return true;
		}
		return false;
	}

}