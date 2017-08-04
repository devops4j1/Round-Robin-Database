package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.protocol.ProtocolType;
import org.wing4j.rrd.net.protocol.RoundRobinDataSizeProtocolV1;
import org.wing4j.rrd.net.protocol.RoundRobinMergeProtocolV1;
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
    RoundRobinConnection connection = null;

    public RoundRobinReadHandler(RoundRobinServer server, AsynchronousSocketChannel channel) {
        this.server = server;
        this.channel = channel;
        try {
            //使用数据库本地数据库打开连接
            this.connection = server.getDatabase().open();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        //命令类型
        int type = attachment.getInt();
        ProtocolType protocolType = ProtocolType.valueOfCode(type);
        //通信协议版本号
        int version = attachment.getInt();
        if(DebugConfig.DEBUG){
            System.out.println("命令:" + protocolType.getDesc() + "." + version);
        }
       if(protocolType == ProtocolType.GET_DATA_SIZE && version == 1){//获取数据数量
           //读取到数据流
           RoundRobinDataSizeProtocolV1 protocol = new RoundRobinDataSizeProtocolV1();
           protocol.convert(attachment);
           try {
               //进行合并视图操作
               Table table = server.getDatabase().getTable(protocol.getTableName());
               protocol.setDataSize(table.getSize());
           }finally {
               if(connection != null){
                   try {
                       connection.close();
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
               }
           }
           //写应答数据
           ByteBuffer resultBuffer = protocol.convert();
           resultBuffer.flip();
           //注册异步写入返回信息
           channel.write(resultBuffer, resultBuffer, new RoundRobinWriteHandler(server, channel));
           return;
       }else if(protocolType == ProtocolType.MERGE && version == 1){
           //读取到数据流，构建格式对象
           RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
           //通过格式对象，构建视图切片对象
           protocol.convert(attachment);
           RoundRobinView view = new RoundRobinView(protocol.getColumns(), protocol.getCurrent(), protocol.getData());
           RoundRobinConnection connection = null;
           try {
               //使用数据库本地数据库打开连接
               connection = server.getDatabase().open();
               //进行合并视图操作
               connection.merge(protocol.getTableName(), protocol.getMergeType(), view);
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
       }else{
           System.out.println("未知命令");
           return;
       }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {

    }
}
