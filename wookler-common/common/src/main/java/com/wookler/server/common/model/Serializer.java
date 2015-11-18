package com.wookler.server.common.model;

import java.io.IOException;

import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;

/**
 * Interface to be implemented to perform byte[] serialization/de-serialization
 * operations.
 *
 * Created by Subho on 2/20/2015.
 */
public interface Serializer<T> {
	@CPath(path = "serializer")
	public static class SerializerConfig {
		@CParam(name = "@class")
		private Class<?>	type;
		@CParam(name = "@handler")
		private Class<?>	handler;

		/**
		 * @return the type
		 */
		public Class<?> getType() {
			return type;
		}

		/**
		 * @param type
		 *            the type to set
		 */
		public void setType(Class<?> type) {
			this.type = type;
		}

		/**
		 * @return the handler
		 */
		public Class<?> getHandler() {
			return handler;
		}

		/**
		 * @param handler
		 *            the handler to set
		 */
		public void setHandler(Class<?> handler) {
			this.handler = handler;
		}

	}

	/**
	 * Serialize the entity object to a byte array.
	 *
	 * @param data
	 *            - Data element to serialize.
	 * @return - Serialized byte[] array.
	 * @throws IOException
	 */
	public byte[] serialize(T data) throws IOException;

	/**
	 * De-serialize the byte array to the entity element.
	 *
	 * @param data
	 *            - Byte array.
	 * @return - Data element.
	 * @throws IOException
	 */
	public T deserialize(byte[] data) throws IOException;

	/**
	 * Can serialize the specified type.
	 *
	 * @param type
	 *            - Type to check.
	 * @return - Can serialize?
	 */
	public boolean accept(Class<?> type);
}
