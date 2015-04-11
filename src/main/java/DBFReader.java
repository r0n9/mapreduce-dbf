/**
 * @file	DBFReader.java
 * @author	FanRong
 * @date	2014-11-21
 * Copyright (c) 2014 Telenav
 */

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 13:47:37 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109129 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFReader {
    private final DataInputStream m_dataInputStream;
    private final DBFHeader m_header;

    public DBFReader(final DataInputStream dataInputStream, final DBFHeader header) throws IOException {
        m_dataInputStream = dataInputStream;
        m_header = header;
    }

    public Map<String, Object> readRecordAsMap(final Map<String, Object> map) throws IOException {
        final byte dataType = nextDataType();
        if (dataType == DBFType.END) {
            return null;
        }
        for (final DBFField field : m_header.fields) {
            map.put(field.fieldName, field.readValue(m_dataInputStream));
        }
        return map;
    }

    public Map<String, Object> readRecordAsMap() throws IOException {
        return readRecordAsMap(new HashMap<String, Object>());
    }

    public Object[] readRecord() throws IOException {
        final byte dataType = nextDataType();
        if (dataType == DBFType.END) {
            return null;
        }
        final int numberOfFields = m_header.numberOfFields;
        final Object values[] = new Object[numberOfFields];
        for (int i = 0; i < numberOfFields; i++) {
            values[i] = readFieldValue(i);
        }
        return values;
    }

    public List<DBFField> getFields() {
        return m_header.fields;
    }

    public int getNumberOfFields() {
        return m_header.numberOfFields;
    }

    public byte nextDataType() throws IOException {
        byte dataType = 0;
        do {
            if (m_dataInputStream.available() <= 0) {
                dataType = DBFType.END;
                break;
            }

            dataType = m_dataInputStream.readByte();

            if (dataType == DBFType.END) {
                break;
            }
            else if (dataType == DBFType.DELETED) {
                skipRecord();
            }
        }
        while (dataType == DBFType.DELETED);
        return dataType;
    }

    public void skipRecord() throws IOException {
        m_dataInputStream.skipBytes(m_header.recordLength - 1);
    }

    public Object readFieldValue(final int index) throws IOException {
        return m_header.getField(index).readValue(m_dataInputStream);
    }

    public Writable readFieldWritable(final int index) throws IOException {
        return m_header.getField(index).readWritable(m_dataInputStream);
    }
}
