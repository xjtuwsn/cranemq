package com.github.xjtuwsn.cranemq.test.performance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @project:cranemq
 * @file:MappedByteBufferTest
 * @author:wsn
 * @create:2023/10/03-20:58
 */
public class MappedByteBufferTest {
    private static String path = "D:\\cranemq\\store\\test.txt";
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
}
