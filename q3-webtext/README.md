# Question 3: WebText API proof of concept

`q3_webtext_mining.Rmd` takes one web link as input, checks robots.txt, scrapes the article into an R list (metadata plus a paragraph data frame), cleans it and summarises it with word frequency scoring, TextRank and LexRank. It ends with `webtext_api(url)`, a single function that runs the whole pipeline for any link.

Try another page:

```r
rmarkdown::render("q3_webtext_mining.Rmd", params = list(url = "https://en.wikipedia.org/wiki/Drought"))
```
