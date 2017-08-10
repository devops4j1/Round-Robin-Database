package org.wing4j.rrd.net.connector.impl.aio;

import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.ProtocolType;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.net.listener.aio.AioRoundRobinWriteHandler;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConnectHandler implements CompletionHandler<Void, AsynchronousSocketChannel> {
    RoundRobinServerConfig serverConfig;
    RoundRobinDatabase database;
    RoundRobinConnector connector;
    ProtocolType protocolType;
    ByteBuffer buffer;

    public ConnectHandler(String address, int port, RoundRobinServerConfig serverConfig, RoundRobinDatabase database, RoundRobinConnector connector, ProtocolType protocolType, ByteBuffer buffer) {
        this.serverConfig = serverConfig;
        this.database = database;
        this.connector = connector;
        this.protocolType = protocolType;
        this.buffer = buffer;
    }

    @Override
    public void completed(Void result, AsynchronousSocketChannel connector) {
        buffer.flip();
        if (protocolType == ProtocolType.MERGE) {
            connector.write(buffer, buffer, new AioRoundRobinWriteHandler(connector, this.serverConfig, this.database));
        } else if (protocolType == ProtocolType.TABLE_METADATA) {
            Future writeResult = connector.write(buffer);
            try {
                writeResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RoundRobinRuntimeException("发送数据发生失败");
            }
            buffer.clear();
            Future readResult = connector.read(buffer);
            try {
                readResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RoundRobinRuntimeException("接收数据发生失败");
            }
        } else if (protocolType == ProtocolType.INCREASE) {
            connector.write(buffer, buffer, new AioRoundRobinWriteHandler(connector, this.serverConfig, this.database));
        } else if (protocolType == ProtocolType.SLICE) {
            Future writeResult = connector.write(buffer);
            try {
                writeResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
            }
            buffer.clear();
            Future readResult = connector.read(buffer);
            try {
                readResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
            }
            buffer.flip();
        } else if (protocolType == ProtocolType.QUERY_PAGE) {
            Future writeResult = connector.write(buffer);
            try {
                writeResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
            }
            buffer.clear();
            Future readResult = connector.read(buffer);
            try {
                readResult.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
            }
            buffer.flip();
        } else if (protocolType == ProtocolType.EXPAND) {
            connector.write(buffer, buffer, new AioRoundRobinWriteHandler(connector, this.serverConfig, this.database));
        } else if (protocolType == ProtocolType.CREATE_TABLE) {
            connector.write(buffer, buffer, new AioRoundRobinWriteHandler(connector, this.serverConfig, this.database));
        }
    }

    @Override
    public void failed(Throwable exc, AsynchronousSocketChannel connector) {
        exc.printStackTrace();
    }

    public ByteBuffer call(ByteBuffer buffer) {
        return null;
    }
}