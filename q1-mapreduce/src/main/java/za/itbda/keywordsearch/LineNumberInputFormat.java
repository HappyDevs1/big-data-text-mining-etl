package za.itbda.keywordsearch;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Input format that hands the mapper (lineNumber, lineText) pairs.
 *
 * The default TextInputFormat gives a byte offset as the key, which is not a
 * row number. To know the real row of every line, each document must be read
 * from its first line by a single mapper, so files are marked as not splittable.
 * Lines are still streamed one at a time, so memory use stays flat even for big
 * documents.
 */
public class LineNumberInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new LineNumberRecordReader();
    }

    /** Wraps Hadoop's LineRecordReader and swaps the byte offset for a 1 based line counter. */
    public static class LineNumberRecordReader extends RecordReader<LongWritable, Text> {

        private final LineRecordReader lineReader = new LineRecordReader();
        private final LongWritable lineNumber = new LongWritable(0);

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            lineReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!lineReader.nextKeyValue()) {
                return false;
            }
            lineNumber.set(lineNumber.get() + 1);
            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
            return lineNumber;
        }

        @Override
        public Text getCurrentValue() {
            return lineReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException {
            return lineReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineReader.close();
        }
    }
}
