# Question 2: LDA topic modelling

`q2_lda_topic_modelling.Rmd` loads the ten articles in `news.zip` into a data frame (`file_name`, `content`), cleans them, fits a 4 topic LDA model and visualises the top words per topic with a bar chart, word clouds and heat maps. Open `q2_lda_topic_modelling.html` to see the code with its output.

Change the number of topics or words without editing the code:

```r
rmarkdown::render("q2_lda_topic_modelling.Rmd", params = list(n_topics = 5, top_k = 8))
```
