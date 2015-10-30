package com.wookler.server.common.model;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.wookler.server.common.ObjectBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by subghosh on 3/9/15.
 */
public class ObjectByteParser {
	@SuppressWarnings("unchecked")
	public <T> byte[] serialize(T data) throws IOException {
		Serializer<T> serializer = (Serializer<T>) SerializerRegistry.get()
				.get(data.getClass());
		if (serializer == null)
			throw new IOException("No registered serializer found. [class="
					+ data.getClass().getCanonicalName() + "]");
		byte[] da = serializer.serialize(data);
		if (da == null || da.length <= 0)
			throw new IOException(
					"Error serializing entity. NULL/Empty bytes returned.");

		ByteString bs = ByteString.copyFrom(da);
		ObjectBuf.ObjectProto op = ObjectBuf.ObjectProto.newBuilder()
				.setType(data.getClass().getCanonicalName())
				.setSize(da.length).setData(bs).build();

		return op.toByteArray();
	}

	@SuppressWarnings("unchecked")
	public <T> T deserialize(byte[] data) throws IOException {
		try {
			ObjectBuf.ObjectProto op = ObjectBuf.ObjectProto.parseFrom(data);
			Class<?> cls = Class.forName(op.getType());
			Serializer<?> serializer = SerializerRegistry.get().get(cls);
			if (serializer == null)
				throw new IOException("No registered serializer found. [class="
						+ cls.getCanonicalName() + "]");
			Serializer<T> ts = (Serializer<T>) serializer;
			T obj = ts.deserialize(op.getData().toByteArray());
			if (obj == null)
				throw new IOException(
						"Invalid object entity. Parsed object is NULL.");
			return obj;
		} catch (ClassNotFoundException e) {
			throw new IOException("Error de-serializing entity.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> byte[] serialize(List<T> data, Class<?> type) throws IOException {
		Preconditions.checkArgument(data != null && data.size() > 0);

		byte[] buffer = null;
		for (T d : data) {
			Serializer<T> serializer = (Serializer<T>) SerializerRegistry.get()
					.get(d.getClass());
			if (serializer == null)
				throw new IOException("No registered serializer found. [class="
						+ d.getClass().getCanonicalName() + "]");
			byte[] da = serializer.serialize(d);
			if (da == null || da.length <= 0)
				throw new IOException(
						"Error serializing entity. NULL/Empty bytes returned.");
			ObjectBuf.ObjectProto op = ObjectBuf.ObjectProto.newBuilder()
					.setType(d.getClass().getCanonicalName())
					.setSize(da.length).setData(ByteString.copyFrom(da))
					.build();
			append(op.toByteArray(), buffer);
		}
		ObjectBuf.ObjectArrayProto oap = ObjectBuf.ObjectArrayProto
				.newBuilder().setCount(data.size())
				.setType(type.getCanonicalName())
				.setData(ByteString.copyFrom(buffer)).build();

		return oap.toByteArray();
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> deserializeAsList(byte[] data) throws IOException {
		try {
			Preconditions.checkArgument(data != null && data.length > 0);

			ObjectBuf.ObjectArrayProto oap = ObjectBuf.ObjectArrayProto
					.parseFrom(data);
			List<T> list = new ArrayList<>(oap.getCount());
			byte[] buffer = oap.getData().toByteArray();
			int index = 0;
			while (index < buffer.length) {
				byte[] nd = new byte[buffer.length - index];
				System.arraycopy(buffer, index, nd, 0, nd.length);
				ObjectBuf.ObjectProto op = ObjectBuf.ObjectProto.parseFrom(nd);
				Class<?> cls = Class.forName(op.getType());
				Serializer<?> serializer = SerializerRegistry.get().get(cls);
				if (serializer == null)
					throw new IOException(
							"No registered serializer found. [class="
									+ cls.getCanonicalName() + "]");
				Serializer<T> ts = (Serializer<T>) serializer;
				byte[] ed = new byte[op.getSize()];
				System.arraycopy(buffer, index, ed, 0, ed.length);
				T d = ts.deserialize(ed);
				if (d == null)
					throw new IOException(
							"Invalid byte entity. De-serializer return NULL"
									+ " object.");
				list.add(d);
				index += op.getSize();
			}
			if (list.size() < oap.getCount())
				throw new IOException(
						"Invalid entity error. Record counts do not match. "
								+ "[expected=" + oap.getCount() + "][received="
								+ list.size() + "]");
			return list;
		} catch (ClassNotFoundException e) {
			throw new IOException("Error de-serializing entity.", e);
		}
	}

	public static byte[] append(byte[] data, byte[] array) {
		Preconditions.checkArgument(data != null && data.length > 0);
		if (array == null)
			return data;
		byte[] buffer = new byte[array.length + data.length];
		System.arraycopy(array, 0, buffer, 0, array.length);
		System.arraycopy(data, 0, buffer, array.length, data.length);

		return buffer;
	}
}
