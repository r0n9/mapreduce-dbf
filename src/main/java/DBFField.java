/**
 * @file	DBFField.java
 * @author	FanRong
 * @date	2014-11-21
 * Copyright (c) 2014 Telenav
 */


import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 14:57:50 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109144 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFField implements Writable {
    public static final int TERMINATOR = 0x0d;

    public String fieldName; /* 0-10  */
    public byte dataType; /* 11    */
    private int reserved1; /* 12-15 */
    public int fieldLength; /* 16    */
    public byte decimalCount; /* 17    */
    private short reserved2; /* 18-19 */
    private byte workAreaId; /* 20    */
    private short reserved3; /* 21-22 */
    private byte setFieldsFlag; /* 23    */
    private byte[] reserved4 = new byte[7]; /* 24-30 */
    private byte indexFieldFlag; /* 31    */

    private Writable fieldValue; // Only one writable instance for this field

    public static DBFField read(final DataInput in) throws IOException {
        final DBFField field = new DBFField();

        final byte firstByte = in.readByte(); /* 0     */
        if (firstByte == TERMINATOR) {
            return null;
        }

        final byte[] bytes = new byte[11]; /* 1-10  */
        in.readFully(bytes, 1, 10);
        bytes[0] = firstByte;

        int nonZeroIndex = bytes.length - 1;
        while (nonZeroIndex >= 0 && bytes[nonZeroIndex] == 0) {
            nonZeroIndex--;
        }
        field.fieldName = new String(bytes, 0, nonZeroIndex + 1);

        field.dataType = in.readByte(); /* 11    */
        field.reserved1 = in.readInt();// DbfUtils.readLittleEndianInt(in);   /* 12-15 */
        field.fieldLength = in.readUnsignedByte(); /* 16    */
        field.decimalCount = in.readByte(); /* 17    */
        field.reserved2 = in.readShort(); // DbfUtils.readLittleEndianShort(in); /* 18-19 */
        field.workAreaId = in.readByte(); /* 20    */
        field.reserved3 = in.readShort();// DbfUtils.readLittleEndianShort(in); /* 21-22 */
        field.setFieldsFlag = in.readByte(); /* 23    */
        in.readFully(field.reserved4); /* 24-30 */
        field.indexFieldFlag = in.readByte(); /* 31    */

        return field;
    }

    public Object readValue(final DataInputStream dataInputStream) throws IOException {
        final byte bytes[] = new byte[fieldLength];
        dataInputStream.readFully(bytes);

        switch (dataType) {
            case 'C':
                return new String(bytes).trim();
            case 'D':
                return readTimeInMillis(bytes);
            case 'F':
                return readFloat(bytes);
            case 'L':
                return readLogical(bytes);
            case 'N':
                if (decimalCount == 0) {
                    if (fieldLength < 5) {
                        return readShort(bytes);
                    }
                    if (fieldLength < 8) {
                        return readInteger(bytes);
                    }
                    return readLong(bytes);
                }
                else {
                    return readDouble(bytes);
                }
            default:
                return null;
        }
    }

    public Writable readWritable(final DataInputStream dataInputStream) throws IOException {
        final byte bytes[] = new byte[fieldLength];
        dataInputStream.readFully(bytes);

        switch (dataType) {
            case 'C':
                Text txetValue = (Text) fieldValue;
                if (null == txetValue) {
                    txetValue = new Text();
                    fieldValue = txetValue;
                }
                txetValue.set(new String(bytes, "GBK").trim()); // TODO charset GBK
                break;
            case 'D':
                LongWritable longValue = (LongWritable) fieldValue;
                if (null == longValue) {
                    longValue = new LongWritable();
                    fieldValue = longValue;
                }
                longValue.set(readTimeInMillis(bytes));
                break;
            case 'F':
                FloatWritable floatValue = (FloatWritable) fieldValue;
                if (null == floatValue) {
                    floatValue = new FloatWritable();
                    fieldValue = floatValue;
                }
                floatValue.set(readFloat(bytes));
                break;
            case 'L':
                BooleanWritable booleanValue = (BooleanWritable) fieldValue;
                if (null == booleanValue) {
                    booleanValue = new BooleanWritable();
                    fieldValue = booleanValue;
                }
                booleanValue.set(readLogical(bytes));
                break;
            case 'N':
                if (decimalCount == 0) {
                    if (fieldLength < 8) {
                        IntWritable intValue = (IntWritable) fieldValue;
                        if (null == intValue) {
                            intValue = new IntWritable();
                            fieldValue = intValue;
                        }
                        intValue.set(readInteger(bytes));
                        break;
                    }
                    LongWritable longValue2 = (LongWritable) fieldValue;
                    if (null == longValue2) {
                        longValue2 = new LongWritable();
                        fieldValue = longValue2;
                    }
                    longValue2.set(readLong(bytes));
                    break;
                }
                else {
                    DoubleWritable doubleResult = (DoubleWritable) fieldValue;
                    if (null == doubleResult) {
                        doubleResult = new DoubleWritable();
                        fieldValue = doubleResult;
                    }

                    try {
                        doubleResult.set(readDouble(bytes));
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    break;

                }
            default:
                fieldValue = NullWritable.get();
                break;
        }

        return fieldValue;

    }

    private int parseInt(final byte[] bytes, final int from, final int to) {
        int result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += bytes[i] - '0';
        }
        return result;
    }

    private short parseShort(final byte[] bytes, final int from, final int to) {
        short result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += bytes[i] - '0';
        }
        return result;
    }

    private long parseLong(final byte[] bytes, final int from, final int to) {
        long result = 0L;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10L;
            result += bytes[i] - '0';
        }
        return result;
    }

    private int trimSpaces(final byte[] bytes) {
        int i = 0, l = bytes.length;
        while (i < l) {
            if (bytes[i] != ' ') {
                break;
            }
            i++;
        }
        return i;
    }

    private long readTimeInMillis(final byte[] bytes) throws IOException {
        int year = parseInt(bytes, 0, 4);
        int month = parseInt(bytes, 4, 6);
        int day = parseInt(bytes, 6, 8);
        return new GregorianCalendar(year, month - 1, day).getTimeInMillis();
    }

    private boolean readLogical(final byte[] bytes) throws IOException {
        return bytes[0] == 'Y' || bytes[0] == 'y' || bytes[0] == 'T' || bytes[0] == 't';
    }

    private short readShort(final byte[] bytes) throws IOException {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?') {
            return 0;
        }
        return parseShort(bytes, index, bytes.length);
    }

    private int readInteger(final byte[] bytes) throws IOException {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?') {
            return 0;
        }
        return parseInt(bytes, index, bytes.length);
    }

    private long readLong(final byte[] bytes) throws IOException {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?') {
            return 0L;
        }
        return parseLong(bytes, index, bytes.length);
    }

    private float readFloat(final byte[] bytes) throws IOException {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?') {
            return 0.0F;
        }
        return Float.parseFloat(new String(bytes, index, length));
    }

    private double readDouble(final byte[] bytes) throws IOException {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?') {
            return 0.0;
        }
        return Double.parseDouble(new String(bytes, index, length));
    }

    @Override
    public String toString() {
        final char c = (char) dataType;
        final StringBuilder sb = new StringBuilder("DBFField{");
        sb.append("fieldName='").append(fieldName).append('\'');
        sb.append(", dataType='").append(c).append('\'');
        sb.append(", fieldLength=").append(fieldLength);
        sb.append(", decimalCount=").append(decimalCount);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(fieldName);
        out.writeByte(dataType);
        out.writeInt(reserved1);
        out.writeInt(fieldLength);
        out.writeByte(decimalCount);
        out.writeShort(reserved2);
        out.writeByte(workAreaId);
        out.writeShort(reserved3);
        out.writeByte(setFieldsFlag);
        out.write(reserved4);
        out.writeByte(indexFieldFlag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fieldName = in.readUTF();
        dataType = in.readByte();
        reserved1 = in.readInt();
        fieldLength = in.readInt();
        decimalCount = in.readByte();
        reserved2 = in.readShort();
        workAreaId = in.readByte();
        reserved3 = in.readShort();
        setFieldsFlag = in.readByte();
        reserved4 = new byte[7];
        in.readFully(reserved4);
        indexFieldFlag = in.readByte();

    }
}
