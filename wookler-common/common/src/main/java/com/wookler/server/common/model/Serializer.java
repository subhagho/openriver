package com.wookler.server.common.model;

import java.io.IOException;

/**
 * Interface to be implemented to perform byte[] serialization/de-serialization operations.
 *
 * Created by Subho on 2/20/2015.
 */
public interface Serializer<T> {
    /**
     * Serialize the entity object to a byte array.
     *
     * @param data - Data element to serialize.
     * @return - Serialized byte[] array.
     * @throws IOException
     */
    public byte[] serialize(T data) throws IOException;

    /**
     * De-serialize the byte array to the entity element.
     *
     * @param data - Byte array.
     * @return - Data element.
     * @throws IOException
     */
    public T deserialize(byte[] data) throws IOException;

    /**
     * Can serialize the specified type.
     *
     * @param type - Type to check.
     * @return - Can serialize?
     */
    public boolean accept(Class<?> type);
}
