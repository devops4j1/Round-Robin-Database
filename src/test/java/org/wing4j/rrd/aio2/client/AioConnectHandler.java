package org.wing4j.rrd.aio2.client;

import java.util.concurrent.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

public class AioConnectHandler implements CompletionHandler<Void, AsynchronousSocketChannel> {
    private Integer content = 0;

    public AioConnectHandler(Integer value) {
        this.content = value;
    }

    public void completed(Void attachment, final AsynchronousSocketChannel connector) {
        ByteBuffer buffer1 = ByteBuffer.wrap(String.valueOf(content).getBytes());
        connector.write(buffer1, buffer1, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                System.out.println(Thread.currentThread().getName() + " merge finish");
                ByteBuffer clientBuffer = ByteBuffer.allocate(1024);
                connector.read(clientBuffer, clientBuffer, new AioReadHandler(connector));
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {

            }
        });
    }

    public void failed(Throwable exc, AsynchronousSocketChannel attachment) {
        exc.printStackTrace();
    }
}