package org.wing4j.rrd.core;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinRuntimeException;

/**
 * Created by wing4j on 2017/8/3.
 */
@Data
@ToString
public class TableMetadata {
    String fileName;
    FormatType formatType;
    String name;
    String[] columns;
    public String[] expand(String... columns){
        int notExistCount = 0;
        for (int i = 0; i < columns.length; i++) {
            int idx1 = columnIndex(columns[i], this.columns);
            if (idx1 == -1) {
                notExistCount++;
            } else {

            }
        }
        String[] notExistHeader = new String[notExistCount];
        int notExistIndex = 0;
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            int idx1 = columnIndex(name, this.columns);
            if (idx1 == -1) {
                notExistHeader[notExistIndex] = name;
                notExistIndex++;
            }
        }
        if (notExistCount != 0) {
            //扩容需要锁定数据库
            synchronized (this) {
                String[] header1 = new String[this.columns.length + notExistCount];
                System.arraycopy(this.columns, 0, header1, 0, this.columns.length);
                System.arraycopy(notExistHeader, 0, header1, this.columns.length, notExistHeader.length);
                this.columns = header1;
            }
        }
        return this.columns;
    }
    public boolean contain(String column) {
        return columnIndex(column) != -1;
    }
    int columnIndex(String column, String[] columns){
        int idx = -1;
        boolean found = false;
        for (String column0 : columns) {
            if (!found) {
                idx++;
            }
            if (column0.equals(column)) {
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
    public int columnIndex(String column){
        return columnIndex(column, columns);
    }
}
