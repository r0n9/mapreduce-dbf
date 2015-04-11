import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

/**
 * DBFInputFormat is an input format which can be used
 * for DBF files which contain DBF header and records with NO
 * delimiters and NO carriage returns (CR, LF, CRLF) etc. DBF files
 * all have one header record which defines the left actual records.<BR><BR>
 * 
 * Users can configure the records count property before submitting
 * any jobs which use DBFInputFormat. <BR><BR>
 * 
 * DBFInputFormat.RecordCountForSplit(myJob,[myRecordCountForSplit]);<BR><BR>
 * 
 * <BR><BR>
 * This InputFormat returns a DBFRecordReader. <BR><BR>
 * 
 * @see    DBFRecordReader
 *
 * @author FanRong
 *
 * $LastChangedDate: 2014-12-11 13:47:37 +0800 (周四, 11 十二�?2014) $
 * $LastChangedRevision: 109129 $
 * $LastChangedBy: rfan@telenav.cn $
 */
public class DBFInputFormat extends FileInputFormat<LongWritable, MapWritable> {

    public static final String SPLIT_RECORD_COUNT = "mapreduce.input.DBFInputFormat.split.recordcnt";

    private DBFHeader dbfHeader;

    @Override
    protected FileStatus[] listStatus(final JobConf job) throws IOException {
        final FileStatus[] orig = super.listStatus(job);
        final List<FileStatus> list = new ArrayList<FileStatus>(orig.length);
        for (final FileStatus fileStatus : orig) {
            final String name = fileStatus.getPath().getName().toLowerCase();
            if (name.endsWith(".dbf")) {
                list.add(fileStatus);
            }
        }
        final FileStatus[] dest = new FileStatus[list.size()];
        list.toArray(dest);
        return dest;
    }

    @Override
    protected boolean isSplitable(final FileSystem fs, final Path path) {
        return true;
    }

    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(final InputSplit inputSplit, final JobConf jobConf, final Reporter reporter) throws IOException {
        return new DBFRecordReader(inputSplit, jobConf);
    }

    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    private static final double SPLIT_SLOP = 1.1; // 10% slop

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        FileStatus[] files = listStatus(job);

        // Save the number of input files in the job-conf
        job.setLong(NUM_INPUT_FILES, files.length);

        // Set DBF header from the first DBF file
        dbfHeader = DBFHeader.read(files[0].getPath().getFileSystem(job).open(files[0].getPath()));
        long headLength = dbfHeader.getHeadLength();
        long splitSize = dbfHeader.getRecordLength() * getRecordCountForSplit(job);

        // generate splits
        ArrayList<DBFFileSplit> splits = new ArrayList<DBFFileSplit>(numSplits);

        NetworkTopology clusterMap = new NetworkTopology();

        for (FileStatus file : files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job);

            long length = file.getLen();

            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

            length = length - headLength;

            if ((length != 0) && isSplitable(fs, path)) {

                long bytesRemaining = length;

                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    String[] splitHosts = getSplitHosts(blkLocations, length - bytesRemaining + headLength, splitSize, clusterMap);
                    splits.add(new DBFFileSplit(path, length - bytesRemaining + headLength, splitSize, splitHosts, dbfHeader));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    splits.add(new DBFFileSplit(path, length - bytesRemaining + headLength, bytesRemaining, blkLocations[blkLocations.length - 1].getHosts(), dbfHeader));
                }
            }
            else if (length != 0) {
                String[] splitHosts = getSplitHosts(blkLocations, headLength, length, clusterMap);
                splits.add(new DBFFileSplit(path, headLength, length, splitHosts, dbfHeader));
            }
            else {
                //Create empty hosts array for zero length files
                splits.add(new DBFFileSplit(path, headLength, length, new String[0], dbfHeader));
            }
        }
        LOG.debug("Total # of splits: " + splits.size());
        return splits.toArray(new DBFFileSplit[splits.size()]);
    }

    /**
     * Return the int value from the given Configuration found
     * by the SPLIT_RECORD_COUNT property.
     * 
     * @param config
     * @return    int record count value
     */
    private static int getRecordCountForSplit(JobConf job) {

        int recordLength = job.getInt(DBFInputFormat.SPLIT_RECORD_COUNT, 100000);

        LOG.debug("Record count for split is: " + recordLength);

        return recordLength;
    }

    /**
     * Set the length of each record
     * @param job the job to modify
     * @param recordLength the length of a record
     */
    public static void RecordCountForSplit(JobConf job, int splitLength) {
        job.setInt(SPLIT_RECORD_COUNT, splitLength);
    }

}
