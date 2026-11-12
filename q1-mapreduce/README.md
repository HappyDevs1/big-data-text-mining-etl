# Question 1: MapReduce keyword location search

Searches every document for a keyword, ignoring case, and returns the row and column of every match.

| Class | Role |
|---|---|
| `LineNumberInputFormat` | Keeps each document in one split and gives the mapper `(row number, line)` |
| `KeywordMatcher` | Case insensitive regex search, whole word by default |
| `KeywordSearchMapper` | Emits `(document, "row,column")` for every match, or `NONE` when a document has none |
| `KeywordSearchReducer` | Sorts each document's locations and counts them |
| `KeywordSearchDriver` | Reads the keyword at run time and configures the job |

The keyword is never hardcoded. It can be passed as the third argument, as `-D search.keyword=...`, or typed in when the program asks for it. `-D search.wholeword=false` also counts matches inside longer words such as "African".

Output for `africa`:

```
doc1.txt  keyword=africa  matches=1  locations=(1,64)
doc2.txt  keyword=africa  matches=3  locations=(1,57) (33,24) (58,81)
doc3.txt  keyword=africa  matches=8  locations=(1,38) (3,7) (5,7) (40,146) (46,7) (50,142) (52,86) (54,44)
doc4.txt  keyword=africa  matches=0  locations=not found
doc5.txt  keyword=africa  matches=4  locations=(1,62) (3,88) (23,121) (38,27)
doc6.txt  keyword=africa  matches=0  locations=not found
doc7.txt  keyword=africa  matches=0  locations=not found
```

`evidence/` holds the logs of the local and cluster runs, the result files and screenshots of the YARN and HDFS web pages.

`hadoop-config/` is a complete single node configuration used through `HADOOP_CONF_DIR`. `yarn-site.xml` raises the disk health threshold to 99% because the development laptop's disk was over 90% full, which otherwise makes YARN mark the node unhealthy.
