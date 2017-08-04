package org.wing4j.rrd.aio2.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

/**
 * Created by 面试1 on 2017/8/1.
 */
public class AioWriteHandler implements CompletionHandler<Integer, ByteBuffer>{
    AsynchronousSocketChannel channel;

    public AioWriteHandler(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        try {
            System.out.println("position:" + attachment.position() + " limit: " + attachment.limit());
            System.out.println(Thread.currentThread().getName() + "merge finish");
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        exc.printStackTrace();
    }
}
