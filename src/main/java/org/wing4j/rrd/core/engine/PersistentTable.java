package org.wing4j.rrd.core.engine;

import org.wing4j.rrd.*;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wing4j on 2017/8/3.
 * 持久化表
 */
public class PersistentTable implements Table {
    public static final boolean DEBUG = true;
    TableMetadata metadata;
    long[][] data;
    volatile List<RoundRobinTrigger>[] triggers;

    public PersistentTable(File file) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatBinV1();
        format.read(file);
        this.data = format.getData();
        this.metadata = new TableMetadata(file.getCanonicalPath(), FormatType.BIN, format.getTableName(), format.getColumns());
    }

    public PersistentTable(String savePath, String tableName, int maxSize, String... columns) throws IOException {
        String fileName = savePath + File.separator + tableName + ".rrd";
        this.metadata = new TableMetadata(fileName, FormatType.BIN, tableName, columns);
        this.data = new long[maxSize][columns.length];
    }

    public TableMetadata getMetadata() {
        return metadata;
    }

    public Table lock() {
        return this;
    }

    public Table unlock() {
        return this;
    }

    public long increase(String column) {
        return increase(column, 1);
    }

    public long increase(String column, int i) {
        int idx = metadata.columnIndex(column);
        int time = getCurrent();
        return increase(time, idx, i);
    }

    long increase(int time, int idx, int i) {
        this.data[time][idx] += i;
        return this.data[time][idx];
    }

    public long[][] getData() {
        return data;
    }

    public int getSize() {
        return data.length;
    }

    @Override
    public long set(int time, String column, long val) {
        int idx = metadata.columnIndex(column);
        return set(time, idx, val);
    }

    long set(int time, int idx, long val) {
        this.data[time][idx] = val;
        return this.data[time][idx];
    }

    @Override
    public long get(int time, String column) {
        int idx = metadata.columnIndex(column);
        return get(time, idx);
    }

    long get(int time, int idx) {
        return this.data[time][idx];
    }

    public int getCurrent() {
        return ((int) (System.currentTimeMillis() / 1000) + (8 * 60 * 60)) % data.length;
    }

    public RoundRobinView slice(int size, String... columns) {
        return slice(size, getCurrent(), columns);
    }

    @Override
    public RoundRobinView slice(int size, int time, String... columns) {
        long[][] data0 = new long[size][columns.length];
        int[] timeline0 = new int[size];
        int[] indexes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int idx = metadata.columnIndex(columns[i]);
            if (idx < 0) {
                throw new RoundRobinRuntimeException("不存在" + columns[i]);
            }
            indexes[i] = idx;
        }
        int pos = time + 1;
        pos = pos - size;
        if (pos < 0) {
            pos = pos + RoundRobinDatabase.DAY_SECOND;
        }
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < columns.length; j++) {
                int i0 = (pos + i) % RoundRobinDatabase.DAY_SECOND;
                data0[i][j] = data[i0][indexes[j]];
            }
        }
        return new RoundRobinView(columns, timeline0, data0);
    }


    public PersistentTable expand(String... columns) {
        lock();
        try {
            metadata.expand(columns);
            for (int i = 0; i < data.length; i++) {
                long[] oldData = data[i];
                long[] newData0 = new long[metadata.getColumns().length];
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

    public PersistentTable merge(RoundRobinView view, int mergePos, MergeType mergeType) {
        if (DEBUG) {
            System.out.println("table:" + metadata.getName());
            System.out.println("mergeType:" + mergeType);
            System.out.println("table column:" + Arrays.asList(metadata.getColumns()));
            System.out.println("view column:" + Arrays.asList(view.getMetadata().getColumns()));
            System.out.println("time:" + view.getTime());
            System.out.println("mergePos:" + mergePos);
        }
        lock();
        long[][] data = view.getData();
        mergePos = mergePos - view.getData().length + 1;
        //进行扩容
        expand(view.getMetadata().getColumns());
        //扩容后一定包含字段
        for (String name : view.getMetadata().getColumns()) {
            int idx0 = metadata.columnIndex(name, view.getMetadata().getColumns());
            int idx1 = metadata.columnIndex(name);
            for (int i = 0; i < data.length; i++) {
                if (mergeType == MergeType.REP) {
                    this.data[mergePos + i][idx1] = data[i][idx0];
                } else if (mergeType == MergeType.ADD) {
                    this.data[mergePos + i][idx1] = this.data[mergePos + i][idx1] + data[i][idx0];
                } else if (mergeType == MergeType.SUB) {
                    this.data[mergePos + i][idx1] = this.data[mergePos + i][idx1] - data[i][idx0];
                } else if (mergeType == MergeType.AVG) {
                    this.data[mergePos + i][idx1] = (this.data[mergePos + i][idx1] + data[i][idx0]) / 2;
                }
            }
        }
        unlock();
        return this;
    }

    @Override
    public Table merge(RoundRobinView view, MergeType mergeType) {
        return merge(view, getCurrent(), mergeType);
    }

    public PersistentTable persistent(FormatType formatType, int version) throws IOException {
        String fileName = metadata.getFileName().trim().toLowerCase();
        if (formatType == FormatType.BIN && version == 1) {
            if (!fileName.endsWith("\\.rrd")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatBinV1(metadata.getName(), metadata.getColumns(), data, getCurrent());
            format.write(fileName + ".rrd");
        } else if (formatType == FormatType.CSV && version == 1) {
            if (!fileName.endsWith("\\.csv")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatCsvV1(metadata.getName(), metadata.getColumns(), data, getCurrent());
            format.write(fileName + ".csv");
        } else {
            throw new RoundRobinRuntimeException("不支持的文件格式和文件版本");
        }
        return this;
    }

    public PersistentTable persistent() throws IOException {
        return persistent(metadata.getFormatType(), 1);
    }

    @Override
    public void drop() throws IOException {
        if (metadata.getDataFile().exists()) {
            metadata.getDataFile().delete();
        }
        metadata.setStatus(TableStatus.DELETE);
    }

    @Override
    public Table registerTrigger(RoundRobinTrigger trigger) {
        return null;
    }
}
