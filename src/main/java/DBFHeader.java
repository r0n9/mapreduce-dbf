/**
 * @file	DBFHeader.java
 * @author	FanRong
 * @date	2014-11-21
 * Copyright (c) 2014 Telenav
 */


import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.io.Writable;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 13:47:37 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109129 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFHeader implements Writable {

    public byte signature; /* 0     */
    public byte year; /* 1     */
    public byte month; /* 2     */
    public byte day; /* 3     */
    public int numberOfRecords; /* 4-7   */
    public short headerLength; /* 8-9   */
    public short recordLength; /* 10-11 */
    public short reserved1; /* 12-13 */
    public byte incompleteTransaction; /* 14    */
    public byte encryptionFlag; /* 15    */
    public int freeRecordThread; /* 16-19 */
    public int reserved2; /* 20-23 */
    public int reserved3; /* 24-27 */
    public byte mdxFlag; /* 28    */
    public byte languageDriver; /* 29    */
    public short reserved4; /* 30-31 */
    public List<DBFField> fields; /* each 32 bytes */
    public int numberOfFields;

    public static DBFHeader read(final DataInputStream dataInput) throws IOException {
        final DBFHeader header = new DBFHeader();

        header.signature = dataInput.readByte(); /* 0     */
        header.year = dataInput.readByte(); /* 1     */
        header.month = dataInput.readByte(); /* 2     */
        header.day = dataInput.readByte(); /* 3     */
        header.numberOfRecords = EndianUtils.readSwappedInteger(dataInput); //DbfUtils.readLittleEndianInt(dataInput);  /* 4-7   */

        header.headerLength = EndianUtils.readSwappedShort(dataInput);//DbfUtils.readLittleEndianShort(dataInput);   /* 8-9   */
        header.recordLength = EndianUtils.readSwappedShort(dataInput);//DbfUtils.readLittleEndianShort(dataInput);   /* 10-11 */

        header.reserved1 = dataInput.readShort();//DbfUtils.readLittleEndianShort(dataInput);        /* 12-13 */
        header.incompleteTransaction = dataInput.readByte(); /* 14    */
        header.encryptionFlag = dataInput.readByte(); /* 15    */
        header.freeRecordThread = dataInput.readInt();//DbfUtils.readLittleEndianInt(dataInput); /* 16-19 */
        header.reserved2 = dataInput.readInt(); /* 20-23 */
        header.reserved3 = dataInput.readInt(); /* 24-27 */
        header.mdxFlag = dataInput.readByte(); /* 28    */
        header.languageDriver = dataInput.readByte(); /* 29    */
        header.reserved4 = dataInput.readShort();//DbfUtils.readLittleEndianShort(dataInput);        /* 30-31 */

        header.fields = new ArrayList<DBFField>();
        DBFField field;
        while ((field = DBFField.read(dataInput)) != null) {
            header.fields.add(field);
        }
        header.numberOfFields = header.fields.size();
        return header;
    }

    public long getHeadLength() {
        return headerLength;
    }

    public long getRecordLength() {
        return recordLength;
    }

    public DBFField getField(final int i) {
        return fields.get(i);
    }

    @Override
    public String toString() {
        return "DBFHeader [signature=" + signature + ", year=" + year + ", month=" + month + ", day=" + day + ", numberOfRecords=" + numberOfRecords + ", headerLength=" + headerLength
                + ", recordLength=" + recordLength + ", reserved1=" + reserved1 + ", incompleteTransaction=" + incompleteTransaction + ", encryptionFlag=" + encryptionFlag + ", freeRecordThread="
                + freeRecordThread + ", reserved2=" + reserved2 + ", reserved3=" + reserved3 + ", mdxFlag=" + mdxFlag + ", languageDriver=" + languageDriver + ", reserved4=" + reserved4 + ", fields="
                + fields + ", numberOfFields=" + numberOfFields + "]";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        signature = in.readByte();
        year = in.readByte();
        month = in.readByte();
        day = in.readByte();
        numberOfRecords = in.readInt();
        headerLength = in.readShort();
        recordLength = in.readShort();
        reserved1 = in.readShort();
        incompleteTransaction = in.readByte();
        encryptionFlag = in.readByte();
        freeRecordThread = in.readInt();
        reserved2 = in.readInt();
        reserved3 = in.readInt();
        mdxFlag = in.readByte();
        languageDriver = in.readByte();
        reserved4 = in.readShort();

        numberOfFields = in.readInt();

        fields = new ArrayList<DBFField>(numberOfFields);
        for (int i = 0; i < numberOfFields; i++) {
            DBFField field = new DBFField();
            field.readFields(in);
            fields.add(field);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(signature);
        out.writeByte(year);
        out.writeByte(month);
        out.writeByte(day);
        out.writeInt(numberOfRecords);
        out.writeShort(headerLength);
        out.writeShort(recordLength);
        out.writeShort(reserved1);
        out.writeByte(incompleteTransaction);
        out.writeByte(encryptionFlag);
        out.writeInt(freeRecordThread);
        out.writeInt(reserved2);
        out.writeInt(reserved3);
        out.writeByte(mdxFlag);
        out.writeByte(languageDriver);
        out.writeShort(reserved4);

        out.writeInt(numberOfFields);
        for (int i = 0; i < numberOfFields; i++) {
            fields.get(i).write(out);
        }
    }

}
