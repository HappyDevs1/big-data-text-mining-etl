# ITBDA4-14 Project 2: MapReduce, Text Mining and ETL

Big data analytics project covering four tasks:

1. A Java Hadoop MapReduce job that finds the row and column of a keyword in every document
2. LDA topic modelling of South African news articles in R
3. A proof of concept WebText API that scrapes, cleans and summarises a web page from a link
4. An ETL pipeline that scrapes a book catalogue, cleans it in R and analyses it in PostgreSQL

The written answers to every question are in [docs/ITBDA4-14_Project2_Report.docx](docs/ITBDA4-14_Project2_Report.docx).

## Repository layout

```
data/
  raw/          original zips from the brief, Wikipedia snapshot, cached book pages, raw scrape
  extracted/    unzipped documents (map_reduce_test_docs, sa_news)
  processed/    cleaned outputs of questions 2, 3 and 4
q1-mapreduce/   Java Maven project, notebook (.Rmd and .html), Hadoop config, run scripts and evidence
q2-lda/         LDA notebook (.Rmd), rendered notebook (.html) and figures
q3-webtext/     web text mining notebook, rendered notebook and figures
q4-etl/         ETL notebook, rendered notebook, figures and SQL scripts
docs/           report, report template and build script, Q1 diagram
```

## Requirements

* Java 11 or newer, Maven, Apache Hadoop 3.x (tested with 3.5.0)
* R 4.x with: dplyr, tidyr, stringr, readr, purrr, ggplot2, tidytext, tm, textstem, topicmodels, wordcloud, rvest, httr, xml2, robotstxt, textrank, lexRankr, igraph, ggraph, DBI, RPostgres, rmarkdown
* PostgreSQL 14 or newer running locally
* pandoc (to render notebooks and build the report)

## How to run

**Question 1**

```bash
cd q1-mapreduce
mvn package                 # builds the jar and runs the unit tests
./run_local.sh              # local mode, asks for the keyword
./run_cluster.sh africa     # HDFS and YARN on one machine
./stop_cluster.sh
```

**Questions 2, 3 and 4**

```bash
cd q2-lda     && Rscript -e 'rmarkdown::render("q2_lda_topic_modelling.Rmd")'
cd q3-webtext && Rscript -e 'rmarkdown::render("q3_webtext_mining.Rmd")'
cd q4-etl     && Rscript -e 'rmarkdown::render("q4_etl_books.Rmd")'
```

Question 3 and 4 reuse the cached pages in `data/raw/` and only go online if the cache is missing or `refresh = TRUE` is passed as a parameter. Question 4 creates a local database called `itbda_etl`.

**Report**

```bash
cd docs && python3 build_report.py
```

## Data sources

* `map_reduce_test_docs.zip` and `news.zip` were provided with the brief.
* Question 3 uses the Wikipedia article "Climate change in Africa" (CC BY-SA 4.0).
* Question 4 uses books.toscrape.com, a public site made for scraping practice.
