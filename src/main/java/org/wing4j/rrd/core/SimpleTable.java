package org.wing4j.rrd.core;

import org.wing4j.rrd.*;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/3.
 */
public class SimpleTable implements Table {
    int DAY_SECOND = 24 * 60 * 60;
    TableMetadata metadata;
    long[][] data;

    public TableMetadata getMetadata() {
        return metadata;
    }

    public Table lock() {
        return this;
    }

    public Table unlock() {
        return this;
    }

    public Table increase(String column) {
        return increase(column, 1);
    }

    public Table increase(String column, int i) {
        int idx = metadata.columnIndex(column);
        int time = getCurrent();
        return increase(idx, time, i);
    }

    Table increase(int idx, int time, int i) {
        data[time][idx] += i;
        return this;
    }

    public long[][] getData() {
        return data;
    }

    public long getSize() {
        return data.length;
    }

    public int getCurrent() {
        return ((int) (System.currentTimeMillis() / 1000) + (8 * 60 * 60)) % DAY_SECOND;
    }

    public RoundRobinView slice(int size, String... columns) {
        return null;
    }

    public RoundRobinResultSet read(String... columns) {
        return null;
    }

    public SimpleTable expand(String... columns) {
        lock();
        try {
            metadata.expand(columns);
            for (int i = 0; i < data.length; i++) {
                long[] oldData = data[i];
                long[] newData0 = new long[metadata.columns.length];
                System.arraycopy(oldData, 0, newData0, 0, oldData.length);
                for (int j = oldData.length; j < newData0.length; j++) {
                    newData0[j] = 0L;
                }
                data[i] = newData0;
            }
            return this;
        } finally {
            unlock();
        }
    }

    public SimpleTable merge(RoundRobinView view, int time, MergeType mergeType) {
        long[][] data = view.getData();
        time = time - view.getData().length;
        //进行扩容
        expand(view.getHeader());
        //扩容后一定包含字段
        for (String name : view.getHeader()) {
            int idx0 = metadata.columnIndex(name, view.getHeader());
            int idx1 = metadata.columnIndex(name);
            for (int i = 0; i < data.length; i++) {
                if (mergeType == MergeType.REP) {
                    this.data[time + i][idx1] = data[i][idx0];
                } else if (mergeType == MergeType.ADD) {
                    this.data[time + i][idx1] = this.data[time + i][idx1] + data[i][idx0];
                } else if (mergeType == MergeType.SUB) {
                    this.data[time + i][idx1] = this.data[time + i][idx1] - data[i][idx0];
                } else if (mergeType == MergeType.AVG) {
                    this.data[time + i][idx1] = (this.data[time + i][idx1] + data[i][idx0]) / 2;
                }
            }
        }
        return this;
    }

    public SimpleTable persistent(FormatType formatType, int version) throws IOException {
        String fileName = metadata.getFileName().trim().toLowerCase();
        if (formatType == FormatType.BIN && version == 1) {
            if (!fileName.endsWith("\\.rrd")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatBinV1(metadata.getColumns(), data, getCurrent());
            format.write(fileName + ".rrd");
        } else if (formatType == FormatType.CSV && version == 1) {
            if (!fileName.endsWith("\\.csv")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatCsvV1(metadata.getColumns(), data, getCurrent());
            format.write(fileName + ".csv");
        } else {
            throw new RoundRobinRuntimeException("不支持的文件格式和文件版本");
        }
        return this;
    }

    public SimpleTable persistent() throws IOException {
        return persistent(metadata.getFormatType(), 1);
    }
}
