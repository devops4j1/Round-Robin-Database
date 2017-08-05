package org.wing4j.rrd;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/7/30.
 */
@Data
@ToString
public class RoundRobinView {
    TableMetadata metadata;
    int time;
    /**
     * 时间线
     */
    int[] timeline;
    /**
     * 数据节点
     */
    long[][] data;
    public RoundRobinView(RoundRobinFormat format){
        try {
            this.metadata = new TableMetadata(null, FormatType.CSV, format.getInstance(), "view", format.getColumns(), data.length, TableStatus.UNKNOWN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.data = format.getData();
        this.time = format.getCurrent();
        this.timeline = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            this.timeline[i] = time - data.length;
        }
    }
    public RoundRobinView(String[] columns, int pos, long[][] data) {
        try {
            this.metadata = new TableMetadata(null, FormatType.CSV,"default",  "view", columns, data.length, TableStatus.UNKNOWN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.time = pos;
        this.data = data;
        this.timeline = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            this.timeline[i] = time - data.length;
        }
    }
    public RoundRobinView(String[] columns, int[] timeline, long[][] data) {
        try {
            this.metadata = new TableMetadata(null, FormatType.CSV,"default", "view", columns, data.length, TableStatus.UNKNOWN);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.data = data;
        this.time = timeline[timeline.length - 1];
        this.timeline = timeline;
    }

    public RoundRobinView set(int time, String column, long val) {
        int idx = metadata.columnIndex(column);
        return set(time, idx, val);
    }

    RoundRobinView set(int time, int idx, long val){
        this.data[time][idx] = val;
        return this;
    }
    long get(int time, String column){
        int idx = metadata.columnIndex(column);
        return get(time, idx);
    }
    public long get(int time, int idx){
        return data[time][idx];
    }
    public RoundRobinResultSet read() {
        return read(metadata.getColumns());
    }
    public RoundRobinResultSet read(String... name) {
        long[][] data0 = new long[name.length][data.length];
        for (int i = 0; i < name.length; i++) {
            data0[i] = read(name[i]);
        }
        RoundRobinResultSet resultSet = new RoundRobinResultSet(name, data0);
        return resultSet;
    }

    long[] read(String name) {
        long[] data0 = new long[data.length];
        int idx = metadata.columnIndex(name);
        for (int i = 0; i < data0.length; i++) {
            data0[i] = data[i][idx];
        }
        return data0;
    }

    /**
     * 将视图数据写入流，进行持久化或者传输
     * @throws IOException IO异常
     */
    public ByteBuffer write() throws IOException {
        RoundRobinFormat format = new RoundRobinFormatBinV1("default","view", metadata.getColumns(), data, time);
        ByteBuffer buffer = format.write();
        return buffer;
    }

}
