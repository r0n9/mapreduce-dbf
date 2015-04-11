/**
 * @file	DBFRecordReader.java
 * @author	FanRong
 * @date	2014-11-21
 * Copyright (c) 2014 Telenav
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 13:47:37 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109129 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFRecordReader implements RecordReader<LongWritable, MapWritable> {
    protected final LongWritable m_key = new LongWritable();
    protected final MapWritable m_value = new MapWritable();
    protected long start;
    protected long end;
    protected long pos;
    protected DBFHeader dbfHeader;
    protected FSDataInputStream m_dbfStream;
    protected DBFReader m_dbfReader;
    protected List<Text> m_keys;
    protected long m_recno;

    public DBFRecordReader(final InputSplit inputSplit, final JobConf jobConf) throws IOException {
        if (inputSplit instanceof DBFFileSplit) {
            final DBFFileSplit dbfFileSplit = (DBFFileSplit) inputSplit;
            start = dbfFileSplit.getStart();
            end = start + dbfFileSplit.getLength();

            dbfHeader = dbfFileSplit.getHeader();

            m_recno = (start - dbfHeader.getHeadLength()) / dbfHeader.getRecordLength();

            final Path path = dbfFileSplit.getPath();
            m_dbfStream = path.getFileSystem(jobConf).open(path);
            m_dbfStream.seek(start);

            m_dbfReader = new DBFReader(m_dbfStream, dbfHeader);

            final List<DBFField> fields = m_dbfReader.getFields();
            m_keys = new ArrayList<Text>(fields.size());
            for (final DBFField field : fields) {
                m_keys.add(new Text(field.fieldName));
            }

            this.pos = start;
        }
        else {
            throw new IOException("Input split is not an instance of DBFFileSplit");
        }
    }

    @Override
    public LongWritable createKey() {
        return m_key;
    }

    @Override
    public MapWritable createValue() {
        return m_value;
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public boolean next(final LongWritable key, final MapWritable value) throws IOException {

        while (pos <= end && m_dbfReader.nextDataType() != DBFType.END) {
            key.set(m_recno++);

            final int numFields = m_dbfReader.getNumberOfFields();
            for (int i = 0; i < numFields; i++) {
                value.put(m_keys.get(i), m_dbfReader.readFieldWritable(i));
            }

            pos += dbfHeader.getRecordLength();

            return true;
        }
        return false;

    }

    @Override
    public void close() throws IOException {
        if (m_dbfStream != null) {
            m_dbfStream.close();
            m_dbfStream = null;
        }
    }
}
