/**
 * @file	DBFFileSplit.java
 * @author	FanRong
 * @date	2014-12-9
 * Copyright (c) 2014 Telenav
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 13:48:05 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109130 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFFileSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

    private Path file;
    private long start;
    private long length;
    private String hosts[];
    private DBFHeader header;

    DBFFileSplit() {
    }

    /**
     * @deprecated Method FileSplit is deprecated
     */
    public DBFFileSplit(Path file, long start, long length, JobConf conf) {
        this(file, start, length, (String[]) null, null);
    }

    public DBFFileSplit(Path file, long start, long length, String hosts[], DBFHeader header) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.header = header;
    }

    public Path getPath() {
        return file;
    }

    public long getStart() {
        return start;
    }

    public long getLength() {
        return length;
    }

    public DBFHeader getHeader() {
        return header;
    }

    public String toString() {
        return (new StringBuilder()).append(file).append(":").append(start).append("+").append(length).toString();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(file.toString());
        out.writeLong(start);
        out.writeLong(length);
        header.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        file = new Path(in.readUTF());
        start = in.readLong();
        length = in.readLong();
        hosts = null;
        header = new DBFHeader();
        header.readFields(in);
    }

    public String[] getLocations() throws IOException {
        if (hosts == null)
            return new String[0];
        else
            return hosts;
    }
}
