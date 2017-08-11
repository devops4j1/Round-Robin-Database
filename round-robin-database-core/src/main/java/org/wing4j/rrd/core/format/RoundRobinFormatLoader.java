package org.wing4j.rrd.core.format;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wing4j on 2017/8/5.
 */
public class RoundRobinFormatLoader {
    File file;
    public RoundRobinFormatLoader(File file){
        this.file = file;
    }
    /**
     * 从文件加载数据库文件
     * @return
     * @throws IOException
     */
    public RoundRobinFormat load(ByteBuffer buffer) throws IOException {
        int format = buffer.getInt();
        int fileLen = buffer.getInt();
        if(buffer.remaining() < fileLen){
            throw new RoundRobinRuntimeException("文件格式不正确");
        }
        int version = buffer.getInt();
        FormatType formatType = FormatType.valueOfCode(format);
        if(formatType == FormatType.BIN && version == 1){
            return new RoundRobinFormatBinV1(buffer);
        }else  if(formatType == FormatType.CSV && version == 1){
            return new RoundRobinFormatCsvV1(buffer);
        }else{
            throw new RoundRobinRuntimeException("不支持的数据格式");
        }
    }
    /**
     * 从文件加载数据库文件
     * @return
     * @throws IOException
     */
    public RoundRobinFormat load() throws IOException {
        FileInputStream fis = new FileInputStream(file);
        FileChannel fileChannel = fis.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
        try {
            try {
                fileChannel.read(buffer);
            } finally {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            }
            buffer.flip();
        } finally {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        return load(buffer);
    }
}
