package za.itbda.keywordsearch;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

public class KeywordMatcherTest {

    @Test
    public void matchesIgnoringCase() {
        KeywordMatcher m = new KeywordMatcher("africa", true);
        assertEquals(Arrays.asList(7, 21), m.findColumns("South Africa and AFRICA"));
    }

    @Test
    public void wholeWordSkipsLongerWords() {
        KeywordMatcher m = new KeywordMatcher("africa", true);
        assertEquals(Collections.emptyList(), m.findColumns("African markets grew"));
    }

    @Test
    public void partialModeCountsLongerWords() {
        KeywordMatcher m = new KeywordMatcher("africa", false);
        assertEquals(Arrays.asList(1), m.findColumns("African markets grew"));
    }

    @Test
    public void punctuationCountsAsWordBoundary() {
        KeywordMatcher m = new KeywordMatcher("Africa", true);
        assertEquals(Arrays.asList(4, 13), m.findColumns("in Africa's (Africa)"));
    }
}
