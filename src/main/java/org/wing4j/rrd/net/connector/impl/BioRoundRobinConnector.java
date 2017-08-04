package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.core.format.net.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class BioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    Socket socket;

    public BioRoundRobinConnector(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.socket = new Socket(address, port);
    }

    @Override
    public RoundRobinView read(int time, int size, String tableName, String... names) {
        return null;
    }

    @Override
    public RoundRobinConnector write(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(mergeType, time, tableName, view);
        ByteBuffer buffer = format.write();
        byte[] data = new byte[buffer.position()];
        buffer.flip();
        buffer.get(data);
        System.out.println(data.length);
        System.out.println(HexUtils.toDisplayString(data));
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

}
