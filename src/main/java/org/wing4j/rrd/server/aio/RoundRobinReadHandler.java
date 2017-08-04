package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.format.net.RoundRobinFormatNetworkV1;
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
        if (attachment.remaining() < size) {
            System.out.println("无效报文");
            return;
        }
        //读取到数据流，构建格式对象
        RoundRobinFormatNetworkV1 format = new RoundRobinFormatNetworkV1(attachment);
        //通过格式对象，构建视图切片对象
        RoundRobinView view = new RoundRobinView(format);
        RoundRobinConnection connection = null;
        try {
            //使用数据库本地数据库打开连接
            connection = server.getDatabase().open();
            //进行合并视图操作
            connection.merge(format.getTableName(), format.getMergeType(), view);
            connection.persistent(FormatType.CSV, 1);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
