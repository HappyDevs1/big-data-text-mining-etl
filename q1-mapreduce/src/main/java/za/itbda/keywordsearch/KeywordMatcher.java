package za.itbda.keywordsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds every case insensitive occurrence of a keyword in a single line and
 * returns the 1 based column where each occurrence starts.
 *
 * In whole word mode "africa" matches "Africa" and "AFRICA" but not "African".
 * In partial mode it matches anywhere, so "African" is counted too.
 */
public class KeywordMatcher {

    private final Pattern pattern;

    public KeywordMatcher(String keyword, boolean wholeWord) {
        if (keyword == null || keyword.trim().isEmpty()) {
            throw new IllegalArgumentException("Keyword must not be empty");
        }
        String quoted = Pattern.quote(keyword.trim());
        String regex = wholeWord
                ? "(?<![\\p{L}\\p{N}])" + quoted + "(?![\\p{L}\\p{N}])"
                : quoted;
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    public List<Integer> findColumns(String line) {
        List<Integer> columns = new ArrayList<>();
        Matcher m = pattern.matcher(line);
        while (m.find()) {
            columns.add(m.start() + 1);
        }
        return columns;
    }
}
