/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.river;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wookler.server.common.utils.LogUtils;

/**
 * Abstract base class to be implemented to handle message records
 * transformations. Data transformations are in the byte format to be saved into
 * the message queues. Finally messages are converted to bytes using Protocol
 * Buffer.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/08/14
 */
public abstract class ByteConvertor<M> {
	@SuppressWarnings("serial")
	public static class ConversionException extends Exception {
		private static final String _PREFIX_ = "Byte Conversion Exception : ";

		public ConversionException(String mesg) {
			super(_PREFIX_ + mesg);
		}

		public ConversionException(String mesg, Throwable inner) {
			super(_PREFIX_ + mesg, inner);
		}
	}

	/**
	 * Read records and transform into the desired message format.
	 *
	 * @param data
	 *            - Data in bytes.
	 * @return - Message
	 * @throws ConversionException
	 */
	public Message<M> read(byte[] data) throws ConversionException {
		try {
			MessageBuf.MessageProto m = MessageBuf.MessageProto.parseFrom(data);
			Message<M> message = new Message<M>();
			message.header().id(m.getHeader().getId());
			message.header().timestamp(m.getHeader().getTimestamp());

			M d = message(m.getData().toByteArray());
			if (d == null)
				throw new ConversionException(
						"Invalid message. Data serializer returned null message.");
			message.data(d);

			return message;
		} catch (InvalidProtocolBufferException e) {
			throw new ConversionException("Error de-serializing to ProtoBuf.",
					e);
		}
	}

	/**
	 * Get byte transformed records to be persisted in the queues.
	 *
	 * @param message
	 *            - Message Object
	 * @return - Data as byte array.
	 * @throws ConversionException
	 */
	public byte[] write(Message<M> message) throws ConversionException {
		byte[] data = data(message.data());
		if (data == null)
			throw new ConversionException(
					"Invalid Message records. Data serializer returned null.");
		ByteString bs = ByteString.copyFrom(data);

		MessageBuf.HeaderProto header = MessageBuf.HeaderProto.newBuilder()
				.setId(message.header().id())
				.setTimestamp(message.header().timestamp()).build();
		MessageBuf.MessageProto m = MessageBuf.MessageProto.newBuilder()
				.setHeader(header).setData(bs).build();
		byte[] ser = m.toByteArray();
		if (ser.length > 64 * 1024) {
			LogUtils.debug(getClass(),
					"Data length exceeded [" + message.data() + "]");
		}
		return ser;
	}

	/**
	 * Abstract method to serialize the message records to byte format, for
	 * persistence into the queue.
	 *
	 * @param message
	 *            - Message to serialize.
	 * @return - Byte serialized records.
	 * @throws ConversionException
	 */
	protected abstract byte[] data(M message) throws ConversionException;

	/**
	 * Abstract method to de-serialize the byte format to a message.
	 *
	 * @param data
	 *            - Byte records.
	 * @return - Converted message.
	 * @throws ConversionException
	 */
	protected abstract M message(byte[] data) throws ConversionException;
}
