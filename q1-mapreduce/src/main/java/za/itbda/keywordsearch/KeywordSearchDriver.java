package za.itbda.keywordsearch;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job configuration and entry point.
 *
 * Usage:
 *   hadoop jar keyword-search.jar input_dir output_dir [keyword]
 *
 * The keyword is supplied at run time in one of three ways, checked in order:
 *   1. third command line argument
 *   2. generic option   -D search.keyword=africa
 *   3. typed in at the prompt when neither of the above is given
 *
 * Optional switches:
 *   -D search.wholeword=false   also match inside longer words, for example African
 *   -D search.overwrite=true    delete an existing output folder before running
 */
public class KeywordSearchDriver extends Configured implements Tool {

    public static final String KEYWORD_PROPERTY = "search.keyword";
    public static final String WHOLE_WORD_PROPERTY = "search.wholeword";
    public static final String OVERWRITE_PROPERTY = "search.overwrite";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar keyword-search.jar <input dir> <output dir> [keyword]");
            return 2;
        }

        Configuration conf = getConf();
        String keyword = resolveKeyword(args, conf);
        if (keyword == null || keyword.isEmpty()) {
            System.err.println("No keyword supplied, nothing to search for.");
            return 2;
        }
        conf.set(KEYWORD_PROPERTY, keyword);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output) && conf.getBoolean(OVERWRITE_PROPERTY, false)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "Keyword location search for '" + keyword + "'");
        job.setJarByClass(KeywordSearchDriver.class);

        job.setInputFormatClass(LineNumberInputFormat.class);
        job.setMapperClass(KeywordSearchMapper.class);
        job.setReducerClass(KeywordSearchReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // One reducer gives a single result file sorted by document name
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.out.println("Searching for keyword: " + keyword
                + " (whole word = " + conf.getBoolean(WHOLE_WORD_PROPERTY, true) + ", case insensitive)");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static String resolveKeyword(String[] args, Configuration conf) throws Exception {
        if (args.length >= 3) {
            return args[2].trim();
        }
        String fromConf = conf.get(KEYWORD_PROPERTY);
        if (fromConf != null && !fromConf.trim().isEmpty()) {
            return fromConf.trim();
        }
        System.out.print("Enter the keyword to search for: ");
        System.out.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line = in.readLine();
        return line == null ? null : line.trim();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new KeywordSearchDriver(), args));
    }
}
