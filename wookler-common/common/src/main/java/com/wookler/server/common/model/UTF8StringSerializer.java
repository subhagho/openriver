/*
 * Copyright 2014 Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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