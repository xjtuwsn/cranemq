package com.github.xjtuwsn.cranemq.common.remote.serialize.impl;

import com.caucho.hessian.io.*;
import com.github.xjtuwsn.cranemq.common.remote.serialize.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
public class Hessian1Serializer extends Serializer {
    SerializerFactory serializerFactory = new SerializerFactory();
    /**
     * 进行序列化
     * @param obj
     * @return
     * @param <T>
     */
    @Override
    public <T> byte[] serialize(T obj){
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Hessian2Output ho = new Hessian2Output(os);
        ho.setSerializerFactory(serializerFactory);
        try {
            ho.writeObject(obj);

            ho.flush();

            byte[] result = os.toByteArray();

            return result;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ho.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 反序列化
     * @param bytes
     * @param clazz
     * @return
     * @param <T>
     */
    @Override
    public <T> Object deserialize(byte[] bytes, Class<T> clazz) {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Hessian2Input hi = new Hessian2Input(is);
        hi.setSerializerFactory(serializerFactory);
        try {
            Object result = hi.readObject();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hi.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}