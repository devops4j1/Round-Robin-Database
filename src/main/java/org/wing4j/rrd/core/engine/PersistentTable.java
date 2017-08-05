package org.wing4j.rrd.core.engine;

import org.wing4j.rrd.*;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.core.format.RoundRobinFormatLoader;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.MessageFormatter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/3.
 * 持久化表
 */
public class PersistentTable implements Table {
    Future future;
    static Logger LOGGER = Logger.getLogger(PersistentTable.class.getName());
    TableMetadata metadata;
    long[][] data;
    volatile List<RoundRobinTrigger>[] triggers;

    public PersistentTable(File file) throws IOException {
        RoundRobinFormatLoader loader = new RoundRobinFormatLoader(file);
        RoundRobinFormat format = loader.load();
        this.data = format.getData();
        this.metadata = new TableMetadata(file.getCanonicalPath(), FormatType.BIN, format.getInstance(), format.getTableName(), format.getColumns(), format.getData().length, TableStatus.NORMAL);
    }

    public PersistentTable(String savePath, String tableName, int maxSize, String... columns) throws IOException {
        this(savePath, "default", tableName, maxSize, columns);
    }

    public PersistentTable(String savePath, String instance, String tableName, int maxSize, String... columns) throws IOException {
        String fileName = savePath + File.separator + instance + File.separator + tableName + ".rrd";
        this.metadata = new TableMetadata(fileName, FormatType.BIN, instance, tableName, columns, maxSize, TableStatus.NORMAL);
        this.data = new long[maxSize][columns.length];
    }

    @Override
    public Table setScheduledFuture(Future future) {
        this.future = future;
        return this;
    }

    @Override
    public Future getScheduledFuture() {
        return future;
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

    public long increase(String column, int val) {
        int idx = metadata.columnIndex(column);
        int pos = getCurrent();
        return increase(pos, idx, val);
    }

    @Override
    public long increase(int pos, String column, int val) {
        if (pos == -1) {
            pos = getCurrent();
        }
        int idx = metadata.columnIndex(column);
        if (idx == -1) {
            throw new RoundRobinRuntimeException(MessageFormatter.format("不存在{}.{}字段", metadata.getName(), column));
        }
        return increase(pos, idx, val);
    }

    long increase(int pos, int idx, int val) {
        this.data[pos][idx] += val;
        return this.data[pos][idx];
    }

    public long[][] getData() {
        return data;
    }

    public int getSize() {
        return data.length;
    }

    @Override
    public long set(int pos, String column, long val) {
        if (pos == -1) {
            pos = getCurrent();
        }
        int idx = metadata.columnIndex(column);
        return set(pos, idx, val);
    }

    long set(int pos, int idx, long val) {
        this.data[pos][idx] = val;
        return this.data[pos][idx];
    }

    @Override
    public long get(int pos, String column) {
        if (pos == -1) {
            pos = getCurrent();
        }
        int idx = metadata.columnIndex(column);
        return get(pos, idx);
    }

    long get(int pos, int idx) {
        return this.data[pos][idx];
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

    public RoundRobinView merge(RoundRobinView view, int pos, MergeType mergeType) {
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("table:{}", metadata.getName()));
            LOGGER.info(MessageFormatter.format("mergeType:{}", mergeType));
            LOGGER.info(MessageFormatter.format("table column:{}", Arrays.asList(metadata.getColumns())));
            LOGGER.info(MessageFormatter.format("view column:{}", Arrays.asList(view.getMetadata().getColumns())));
            LOGGER.info(MessageFormatter.format("pos:{}", pos));
        }
        lock();
        long[][] data = view.getData();
        long[][] newData = new long[view.getData().length][view.getMetadata().getColumns().length];
        int mergePos = pos - view.getData().length + 1;
        //进行扩容
        expand(view.getMetadata().getColumns());
        //扩容后一定包含字段
        for (String name : view.getMetadata().getColumns()) {
            int idx0 = metadata.columnIndex(name, view.getMetadata().getColumns());
            int idx1 = metadata.columnIndex(name);
            for (int i = 0; i < data.length; i++) {
                if (mergeType == MergeType.REP) {
                    this.data[mergePos + i][idx1] = data[i][idx0];
                    newData[i][idx0] = this.data[mergePos + i][idx1];
                } else if (mergeType == MergeType.ADD) {
                    this.data[mergePos + i][idx1] = this.data[mergePos + i][idx1] + data[i][idx0];
                    newData[i][idx0] = this.data[mergePos + i][idx1];
                } else if (mergeType == MergeType.SUB) {
                    this.data[mergePos + i][idx1] = this.data[mergePos + i][idx1] - data[i][idx0];
                    newData[i][idx0] = this.data[mergePos + i][idx1];
                } else if (mergeType == MergeType.AVG) {
                    this.data[mergePos + i][idx1] = (this.data[mergePos + i][idx1] + data[i][idx0]) / 2;
                    newData[i][idx0] = this.data[mergePos + i][idx1];
                }
            }
        }

        unlock();
        return new RoundRobinView(view.getMetadata().getColumns(), view.getTime(), newData);
    }

    @Override
    public RoundRobinView merge(RoundRobinView view, MergeType mergeType) {
        return merge(view, getCurrent(), mergeType);
    }

    public PersistentTable persistent(FormatType formatType, int version) throws IOException {
        LOGGER.info(MessageFormatter.format("persistent table {}.{}", metadata.getInstance(), metadata.getName()));
        String fileName = metadata.getFileName().trim().toLowerCase();
        if (formatType == FormatType.BIN && version == 1) {
            if (!fileName.endsWith("\\.rrd")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatBinV1(metadata.getInstance(), metadata.getName(), metadata.getColumns(), data, getCurrent());
            format.write(fileName + ".rrd");
        } else if (formatType == FormatType.CSV && version == 1) {
            if (!fileName.endsWith("\\.csv")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatCsvV1(metadata.getInstance(), metadata.getName(), metadata.getColumns(), data, getCurrent());
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
        metadata.setStatus(TableStatus.DROP);
    }

    @Override
    public Table registerTrigger(RoundRobinTrigger trigger) {
        return null;
    }
}
