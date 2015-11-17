/**
 * TODO: <comments>
 *
 * @file UTF8StringSerializer.java
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
public final class UTF8StringSerializer implements Serializer<String> {

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.model.Serializer#serialize(java.lang.Object
	 * )
	 */
	@Override
	public byte[] serialize(String data) throws IOException {
		return data.getBytes("UTF-8");
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.model.Serializer#deserialize(byte[])
	 */
	@Override
	public String deserialize(byte[] data) throws IOException {
		return new String(data, "UTF-8");
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.model.Serializer#accept(java.lang.Class)
	 */
	@Override
	public boolean accept(Class<?> type) {
		return type.equals(String.class);
	}
}