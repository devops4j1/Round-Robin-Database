package org.wing4j.rrd.server.impl;

import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class RoundRobinReadHandler implements CompletionHandler<Integer, ByteBuffer>{
    static Logger LOGGER = Logger.getLogger(RoundRobinReadHandler.class.getName());
    AsynchronousSocketChannel channel;
    public RoundRobinReadHandler(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    static final boolean DEBUG = true;

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if(result < 1){
            return;
        }
        if(DEBUG){
            attachment.flip();
            int size = attachment.getInt();
            System.out.println(size);
            attachment = ByteBuffer.allocate(size);
            Future future = channel.read(attachment);
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            attachment.flip();
            if(attachment.remaining() < size){
                System.out.println("无效报文");
                byte[] da = new byte[attachment.remaining()];
                attachment.get(da);
                System.out.println(new String(da));
                return;
            }
            byte[] data = new byte[size];
            attachment.get(data);
            System.out.println(Thread.currentThread().getName() + " " +HexUtils.toDisplayString(data));

        }
        attachment.flip();
        //TODO 进行接受的数据处理
        attachment.compact();
        //TODO 模拟返回结果
        String resultMessage = "save data finish";

        //写应答数据
        ByteBuffer resultBuffer = ByteBuffer.wrap(resultMessage.getBytes());
        resultBuffer.flip();
        //注册异步写入返回信息
        channel.write(resultBuffer, resultBuffer, new RoundRobinWriteHandler(channel));
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {

    }
}
