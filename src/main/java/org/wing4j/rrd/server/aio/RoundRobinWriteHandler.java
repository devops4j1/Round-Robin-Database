package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.server.RoundRobinServer;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class RoundRobinWriteHandler implements CompletionHandler<Integer, ByteBuffer>{
    RoundRobinServer server;
    AsynchronousSocketChannel channel;
    static Logger LOGGER = Logger.getLogger(RoundRobinWriteHandler.class.getName());

    public RoundRobinWriteHandler(RoundRobinServer server, AsynchronousSocketChannel channel) {
        this.server = server;
        this.channel = channel;
    }

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
       if(attachment.hasRemaining()){//如果缓冲区未发送完成，继续进行发送
           channel.write(attachment, attachment, this);
       }else {//发送完则进行读取
           ByteBuffer readBuffer = ByteBuffer.allocate(1024);
           channel.read(readBuffer, readBuffer, new RoundRobinReadHandler(server, channel));
       }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        exc.printStackTrace();
    }
}
