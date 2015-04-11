/**
 * @file	DBFSchema.java
 * @author	FanRong
 * @date	2014-11-24
 * Copyright (c) 2014 Telenav
 */


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-02 17:43:30 +0800 (周二, 02 十二�?2014) $
 * $LastChangedRevision: 107794 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFSchema extends TextLine {
    private static final long serialVersionUID = "$Id: DBFSchema.java 107794 2014-12-02 09:43:30Z rfan@telenav.cn $".hashCode();

    public DBFSchema(Fields sourceFields) {
        super(sourceFields);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(DBFInputFormat.class);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        if (tap.getFullIdentifier(conf).toString().endsWith(".zip"))
            throw new IllegalStateException("cannot write zip files: " + FileOutputFormat.getOutputPath(conf));

        if (getSinkCompression() == Compress.DISABLE)
            conf.setBoolean("mapred.output.compress", false);
        else if (getSinkCompression() == Compress.ENABLE)
            conf.setBoolean("mapred.output.compress", true);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class); // be explicit
        conf.setOutputFormat(TextOutputFormat.class);
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        if (!sourceReadInput(sourceCall))
            return false;

        sourceHandleInput(sourceCall);

        return true;
    }

    private boolean sourceReadInput(SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object[] context = sourceCall.getContext();
        return sourceCall.getInput().next(context[0], context[1]);
    }

    @Override
    protected void sourceHandleInput(SourceCall<Object[], RecordReader> sourceCall) {
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();

        int index = 0;

        Object[] context = sourceCall.getContext();

        if (getSourceFields().size() == 2)
            tuple.set(index++, ((LongWritable) context[0]).get());

        tuple.set(index, context);
    }

}
