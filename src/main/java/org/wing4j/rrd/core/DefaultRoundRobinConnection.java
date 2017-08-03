package org.wing4j.rrd.core;


import org.wing4j.rrd.*;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by wing4j on 2017/7/29.
 */
public class DefaultRoundRobinConnection implements RoundRobinConnection {
    /**
     * 状态
     */
    int status = Status.NORMAL;
    long[][] data = null;
    String[] header = null;
    volatile RoundRobinDatabase database;
    volatile String fileName;
    volatile List<RoundRobinTrigger>[] triggers;
    FormatType formatType = FormatType.BIN;
    /**
     * 任务执行线程池
     */
    volatile ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(20);

    DefaultRoundRobinConnection(RoundRobinDatabase database, String[] header, long[][] data, String fileName) {
        this.database = database;
        this.header = header;
        this.data = data;
        this.fileName = fileName;
        this.triggers = new ArrayList[header.length];
        for (int i = 0; i < this.triggers.length; i++) {
            this.triggers[i] = new ArrayList<>();
        }
        if (fileName != null && fileName.trim().toLowerCase().endsWith(".csv")) {
            formatType = FormatType.CSV;
        }
    }

    @Override
    public RoundRobinConnection lock() {
        if (status != Status.NORMAL) {
            throw new RuntimeException("数据库未进行冻结");
        }
        status = Status.FREEZEN;
        return this;
    }

    @Override
    public RoundRobinConnection unlock() {
        if (status != Status.FREEZEN) {
            throw new RuntimeException("数据库未进行冻结");
        }
        status = Status.NORMAL;
        return this;
    }

    @Override
    public RoundRobinDatabase getDatabase() {
        return database;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
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
        int idx = getIndex(name, header);
        int current = getCurrent();
        for (int i = 0; i < data0.length; i++) {
            data0[i] = data[(i + current) % data0.length][idx];
        }
        return data0;
    }

    @Override
    public boolean contain(String name) {
        for (String name0 : header) {
            if (name.equals(name0)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RoundRobinConnection increase(int sec, String name) {
        return increase(sec, name, 1);
    }

    int getIndex(String name, String[] names) {
        int idx = -1;
        boolean found = false;
        for (String name0 : names) {
            if (!found) {
                idx++;
            }
            if (name.equals(name0)) {
                if (found) {
                    throw new RoundRobinRuntimeException("存在重复的字段，系统错误");
                }
                found = true;
            }
        }
        if (found) {
            return idx;
        } else {
            return -1;
        }
    }

    @Override
    public RoundRobinConnection increase(String name) {
        int sec = getCurrent();
        return increase(sec, name, 1);
    }

    @Override
    public RoundRobinConnection increase(String name, int i) {
        int sec = getCurrent();
        return increase(sec, name, i);
    }

    @Override
    public RoundRobinConnection increase(final int sec, String name, int i) {
        final int idx = getIndex(name, header);
        if (idx < 0) {
            throw new RoundRobinRuntimeException("不存在" + name);
        }
        synchronized (this.header[idx]) {
            this.data[sec][idx] = this.data[sec][idx] + i;
        }
        final long data0 = this.data[sec][idx];
        final List<RoundRobinTrigger> triggers = this.triggers[idx];
        taskExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (RoundRobinTrigger trigger : triggers) {
                    if (trigger.accept(sec, data0)) {
                        trigger.trigger(sec, data0);
                    }
                }
            }
        });
        return this;
    }

    @Override
    public RoundRobinView slice(int second, String... name) {
        long[][] data0 = new long[second][name.length];
        int[] timeline0 = new int[second];
        int[] indexes = new int[name.length];
        for (int i = 0; i < name.length; i++) {
            int idx = getIndex(name[i], header);
            if (idx < 0) {
                throw new RoundRobinRuntimeException("不存在" + name[i]);
            }
            indexes[i] = idx;
        }
        int pos = getCurrent();
        pos = pos - second;
        if (pos < 0) {
            pos = pos + DAY_SECOND;
        }
        for (int i = 0; i < second; i++) {
            for (int j = 0; j < name.length; j++) {
                int i0 = (pos + i) % DAY_SECOND;
                data0[i][j] = data[i0][indexes[j]];
            }
        }
        return new RoundRobinView(name, timeline0, data0, pos);
    }

    @Override
    public RoundRobinConnection addTrigger(RoundRobinTrigger trigger) {
        this.triggers[getIndex(trigger.getName(), header)].add(trigger);
        return this;
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, MergeType mergeType) {
        return merge(view, view.getTime(), mergeType);
    }

    /**
     * 进行字段扩容
     *
     * @param header0
     */
    void expand(String... header0) {
        int notExistCount = 0;
        for (int i = 0; i < header0.length; i++) {
            int idx1 = getIndex(header0[i], this.header);
            if (idx1 == -1) {
                notExistCount++;
            } else {

            }
        }
        String[] notExistHeader = new String[notExistCount];
        int notExistIndex = 0;
        for (int i = 0; i < header0.length; i++) {
            String name = header0[i];
            int idx1 = getIndex(name, header);
            if (idx1 == -1) {
                notExistHeader[notExistIndex] = name;
                notExistIndex++;
            }
        }
        if (notExistCount != 0) {
            //扩容需要锁定数据库
            synchronized (this) {
                String[] header1 = new String[header.length + notExistCount];
                System.arraycopy(header, 0, header1, 0, header.length);
                System.arraycopy(notExistHeader, 0, header1, header.length, notExistHeader.length);
                this.header = header1;
                for (int i = 0; i < data.length; i++) {
                    long[] oldData = data[i];
                    long[] newData0 = new long[header1.length];
                    System.arraycopy(oldData, 0, newData0, 0, oldData.length);
                    for (int j = oldData.length; j < newData0.length; j++) {
                        newData0[j] = 0L;
                    }
                    data[i] = newData0;
                }
            }
        }
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, int time, MergeType mergeType) {
        if (status != Status.FREEZEN) {
            throw new RuntimeException("数据库未进行冻结");
        }
        long[][] data = null;
        time = time - view.getData().length;
        data = view.getData();
        //进行扩容
        expand(view.getHeader());
        //扩容后一定包含字段
        for (String name : view.getHeader()) {
            int idx0 = getIndex(name, view.getHeader());
            int idx1 = getIndex(name, header);
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

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version) throws IOException {
        String fileName = this.fileName.trim().toLowerCase();
        if (formatType == FormatType.BIN && version == 1) {
            if (!fileName.endsWith("\\.rrd")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatBinV1(header, data, getCurrent());
            format.write(fileName + ".rrd");
        } else if (formatType == FormatType.CSV && version == 1) {
            if (!fileName.endsWith("\\.csv")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            RoundRobinFormat format = new RoundRobinFormatCsvV1(header, data, getCurrent());
            format.write(fileName + ".csv");
        } else {
            throw new RoundRobinRuntimeException("不支持的文件格式和文件版本");
        }
        //序列化数据
        return this;
    }

    public RoundRobinConnection persistent() throws IOException {
        return persistent(formatType, 1);
    }

    /**
     * 异步写入
     *
     * @return
     */
    void asyncWrite() {
        //创建一个定时器，进行定期调用写入数据
    }

    @Override
    public void close() throws IOException {
        if (database.getConfig().isAutoPersistent()) {
            persistent();
        }
        database.close(this);
    }

    public int getCurrent() {
        return ((int) (System.currentTimeMillis() / 1000) + (8 * 60 * 60)) % DAY_SECOND;
    }
}
