package za.itbda.keywordsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce phase.
 *
 * Input  : (documentName, ["row,column", "row,column", ...])
 * Output : documentName  TAB  keyword=x  matches=n  locations=(r,c) (r,c) ...
 *
 * The shuffle groups every location of a document under one key. The reducer
 * sorts them by row then column so the result reads top to bottom like the
 * original document.
 */
public class KeywordSearchReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();
    private String keyword;

    @Override
    protected void setup(Context context) {
        keyword = context.getConfiguration().get(KeywordSearchDriver.KEYWORD_PROPERTY);
    }

    @Override
    protected void reduce(Text document, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<long[]> locations = new ArrayList<>();

        for (Text value : values) {
            String v = value.toString();
            if (KeywordSearchMapper.NO_MATCH.equals(v)) {
                continue;
            }
            String[] parts = v.split(",");
            locations.add(new long[] { Long.parseLong(parts[0]), Long.parseLong(parts[1]) });
        }

        locations.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));

        StringJoiner joined = new StringJoiner(" ");
        for (long[] loc : locations) {
            joined.add("(" + loc[0] + "," + loc[1] + ")");
        }

        String locationText = locations.isEmpty() ? "not found" : joined.toString();
        result.set("keyword=" + keyword + "\tmatches=" + locations.size() + "\tlocations=" + locationText);
        context.write(document, result);
    }
}
