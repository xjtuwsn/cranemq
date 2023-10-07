package com.github.xjtuwsn.cranemq.common.remote.serialize;

public abstract class Serializer {
    /**
     * 序列化
     * @param obj
     * @return
     * @param <T>
     */
    public abstract <T> byte[] serialize(T obj);

    /**
     * 反序列化
     * @param bytes
     * @param clazz
     * @return
     * @param <T>
     */
    public abstract <T> Object deserialize(byte[] bytes, Class<T> clazz);
}