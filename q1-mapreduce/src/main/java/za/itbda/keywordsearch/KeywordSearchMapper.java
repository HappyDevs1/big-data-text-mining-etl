package za.itbda.keywordsearch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Map phase.
 *
 * Input  : (rowNumber, lineText) for one line of one document
 * Output : (documentName, "row,column") for every keyword hit on that line
 *
 * The keyword is read from the job configuration (search.keyword), so it is
 * never hardcoded. Because each document is one unsplit input, one mapper sees
 * a whole document and can report documents that have no match at all.
 */
public class KeywordSearchMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String NO_MATCH = "NONE";

    public enum SearchCounters { LINES_SCANNED, KEYWORD_MATCHES, DOCUMENTS_WITH_MATCH }

    private final Text documentKey = new Text();
    private final Text location = new Text();
    private KeywordMatcher matcher;
    private long matchesInDocument = 0;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String keyword = conf.get(KeywordSearchDriver.KEYWORD_PROPERTY);
        boolean wholeWord = conf.getBoolean(KeywordSearchDriver.WHOLE_WORD_PROPERTY, true);
        matcher = new KeywordMatcher(keyword, wholeWord);

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentKey.set(fileName);
    }

    @Override
    protected void map(LongWritable rowNumber, Text line, Context context)
            throws IOException, InterruptedException {
        context.getCounter(SearchCounters.LINES_SCANNED).increment(1);

        for (int column : matcher.findColumns(line.toString())) {
            location.set(rowNumber.get() + "," + column);
            context.write(documentKey, location);
            matchesInDocument++;
            context.getCounter(SearchCounters.KEYWORD_MATCHES).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (matchesInDocument == 0) {
            // Tell the reducer this document was searched but nothing was found
            location.set(NO_MATCH);
            context.write(documentKey, location);
        } else {
            context.getCounter(SearchCounters.DOCUMENTS_WITH_MATCH).increment(1);
        }
    }
}
