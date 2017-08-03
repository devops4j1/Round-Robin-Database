package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.net.format.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class RoundRobinReadHandler implements CompletionHandler<Integer, ByteBuffer> {
    static Logger LOGGER = Logger.getLogger(RoundRobinReadHandler.class.getName());
    RoundRobinServer server;
    AsynchronousSocketChannel channel;

    public RoundRobinReadHandler(RoundRobinServer server, AsynchronousSocketChannel channel) {
        this.server = server;
        this.channel = channel;
    }

    static final boolean DEBUG = true;

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if (result < 1) {
            return;
        }
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
        RoundRobinFormatNetworkV1 format = new RoundRobinFormatNetworkV1();
        format.read(attachment);
        RoundRobinView view = new RoundRobinView(format);
        try {
            RoundRobinConnection connection = server.getDatabase().create(server.getConfig().getDatabaseFilePath() + "/database.rrd");
            connection.merge(view, format.getMergeType());
            connection.close();
//            connection.persistent(FormatType.CSV, 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
//            if(attachment.remaining() < size){
////                System.out.println("无效报文");
////                byte[] da = new byte[attachment.remaining()];
////                attachment.get(da);
////                System.out.println(new String(da));
////                return;
//                attachment.clear();
//                try {
//                    channel.read(attachment).get(1,TimeUnit.MINUTES);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                } catch (TimeoutException e) {
//                    e.printStackTrace();
//                }
//                System.out.println(HexUtils.toDisplayString(attachment.array()));
//            }
//            byte[] data = new byte[size];
//            attachment.get(data);
//            System.out.println(Thread.currentThread().getName() + " " +HexUtils.toDisplayString(data));

//        attachment.flip();
//        //TODO 进行接受的数据处理
//        attachment.compact();
        //TODO 模拟返回结果
        String resultMessage = "save data finish";

        //写应答数据
        ByteBuffer resultBuffer = ByteBuffer.wrap(resultMessage.getBytes());
        resultBuffer.flip();
        //注册异步写入返回信息
        channel.write(resultBuffer, resultBuffer, new RoundRobinWriteHandler(server, channel));
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {

    }
}
