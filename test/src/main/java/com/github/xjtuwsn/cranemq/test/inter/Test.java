package com.github.xjtuwsn.cranemq.test.inter;

/**
 * @project:cranemq
 * @file:Test
 * @author:wsn
 * @create:2023/09/27-10:29
 */
public class Test {
    public static void main(String[] args) {
        NetBody body = new NetBody();
        body.command = RequestType.PRODUCE;
        System.out.println(body.command instanceof ResponseType);
    }
}
