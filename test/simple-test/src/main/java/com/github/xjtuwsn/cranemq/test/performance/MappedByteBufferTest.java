package com.github.xjtuwsn.cranemq.test.performance;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @project:cranemq
 * @file:MappedByteBufferTest
 * @author:wsn
 * @create:2023/10/03-20:58
 */
public class MappedByteBufferTest {
    private static String path = "D:\\cranemq\\store\\00000000000000000000";
    private static File file;
    private static FileChannel fileChannel;
    private static MappedByteBuffer mappedByteBuffer;
    public static void main(String[] args) throws IOException {
        file = new File(path);
        fileChannel = new RandomAccessFile(path, "rw").getChannel();

        long start = System.nanoTime();

        testSmallBlock();
        long end = System.nanoTime();
        double cost = (end - start) / 1e6;
        System.out.println("Cost " + cost + " ms");
        System.out.println();
        fileChannel.close();
    }

    public static void testSmallBlock() throws IOException {
        int total = 500 * 1024 * 1024;
        int epoch = 500 * 1024;
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, total);
        byte[] data = new byte[total / epoch];
        for (int i = 0; i < epoch; i++) {
            mappedByteBuffer.put(data);
            mappedByteBuffer.force();
        }

        mappedByteBuffer.clear();
    }
    public static void testBigBlock() throws IOException {
        int total = 500 * 1024 * 1024;
        int epoch = 500 * 1024;
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, total);
        byte[] data = new byte[total / epoch];
        for (int i = 0; i < epoch; i++) {
            mappedByteBuffer.put(data);
        }
        mappedByteBuffer.force();
        mappedByteBuffer.clear();
    }
    @Test
    public void test2() throws IOException {
        String path = "D:\\cranemq\\store\\00000000000000000000";
        FileInputStream fis = new FileInputStream(path);
        DataInputStream dis = new DataInputStream(fis);
        while (true) {
            long offset = dis.readLong();
            int size = dis.readInt();

            if (offset == 0 && size == 0) {
                break;
            }
            System.out.println(offset + ", " + size);
        }
    }
    @Test
    public void test3() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        byteBuffer.putInt(100);
        byteBuffer.position(0);
        int a = byteBuffer.getInt();
        byteBuffer.position(0);
        int b = byteBuffer.getInt();
        System.out.println(a);
        System.out.println(b);
    }
}
