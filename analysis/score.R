library(tidyverse)
library(arrow)
options(arrow.skip_nul = TRUE)
library(quanteda)
# devtools::install_github("kbenoit/quanteda.dictionaries") 
library(quanteda.dictionaries)
library(lubridate)

# load dictionary
liwc_2015 <- dictionary(file = "C:/Users/I567766/Documents/ccs/LIWC2015_English.dic",
                        format = "LIWC")

# variables to be centered
vars <- c("weeks_since_last_comment", "weeks_since_video_publication", "weeks_since_last_video", "positivity", "pos_affect", "neg_affect", "word_count")

# load comments and score them
if (!file.exists("C:/Users/I567766/Documents/ccs/scored_comments.parquet.gzip")) {
  map(
    list.files(path = "C:/Users/I567766/Documents/ccs/ccs-data", full.names = TRUE) %>% 
      str_subset("meta", negate = TRUE),
    \(file) read_parquet(file) %>%
      corpus(docid_field = "comment_id", text_field = "comment") %>%
      liwcalike(dictionary = liwc_2015) %>%
      mutate(
        comment_id = docname,
        first_person_singular_pronouns = `function (function words).pronoun (pronouns).ppron (personal pronouns).i (i)`,
        present_focus = `timeorient (time orientation).focuspresent (present focus)`,
        discrepancies = `cogproc (cognitive processes).discrep (discrepancies)`,
        more_than_six_letters = Sixltr,
        articles = `function (function words).article (articles)`,
        verbal_immediacy = (
          first_person_singular_pronouns + present_focus + discrepancies - more_than_six_letters - articles
        ) / 5,
        pos_affect = `affect (affect).posemo (positive emotions)`,
        neg_affect = `affect (affect).negemo (negative emotions)`,
        positivity = pos_affect - neg_affect,
        positivity_cat = case_when(
          positivity < 0 ~ "negative",
          positivity == 0 ~ "neutral",
          positivity > 0 ~ "positive"
        ),
        .keep = "none"
      ) %>%
      left_join(read_parquet(file), ., by = join_by(comment_id)) %>%
      mutate(
        word_count = str_count(comment, '\\w+'),
        character_count = str_length(comment)
      ) %>%
      select(-index) %>%
      mutate(channel_name = file) %>% 
      mutate(
        published_at = as_datetime(published_at),
        date = lubridate::date(published_at)
      ) %>% 
      left_join(
        map(
          list.files("C:/Users/I567766/Documents/ccs/ccs-data/meta", full.names = TRUE),
          \(meta_file) read_parquet(meta_file) %>% 
            mutate(
              video_published_at = as_datetime(published_at),
              video_date = lubridate::date(video_published_at),
              video_comment_count = comment_count,
              .keep = "unused"
            ) %>% 
            select(-index)
        ) %>% list_rbind(),
        by = join_by(video_id)
      ) %>% 
      group_by(channel_name, author_channel_id) %>%
      arrange(published_at) %>% 
      mutate(
        weeks_since_first_comment = difftime(date, min(date), units = "weeks") %>% as.numeric(),
        weeks_since_last_comment = difftime(date, lag(date), units = "weeks") %>% as.numeric(),
        weeks_since_video_publication = difftime(date, video_date, units = "weeks") %>% as.numeric(),
        comment_count = row_number()
      ) %>% 
      ungroup() %>% 
      group_by(channel_name) %>% 
      arrange(video_published_at) %>%
      mutate(
        weeks_since_first_video = difftime(video_date, min(video_date), units = "weeks") %>% as.numeric(),
        weeks_since_last_video = difftime(video_date, lag(video_date), units = "weeks") %>% as.numeric(),
        video_count = row_number()
      ) %>% 
      ungroup() %>% 
      mutate(
        across(all_of(vars),
               ~ .x - mean(.x, na.rm = TRUE), .names = "{col}_grand_mean_centered")
      ) %>% 
      group_by(author_channel_id) %>%
      mutate(
        across(all_of(vars),
               ~ mean(.x, na.rm = TRUE), .names = "{col}_person_mean"),
        across(all_of(vars),
               ~ .x - get(glue::glue("{cur_column()}_person_mean")), .names = "{col}_centered_within_person")
      ) %>%
      ungroup() %>%
      mutate(
        across(all_of(str_c(vars, "_person_mean")),
               ~ .x - mean(.x, na.rm = TRUE), .names = "{col}_centered")
      ) %>% 
      group_by(channel_name) %>%
      mutate(
        across(all_of(vars),
               ~ mean(.x, na.rm = TRUE), .names = "{col}_channel_mean"),
        across(all_of(vars),
               ~ .x - get(glue::glue("{cur_column()}_channel_mean")), .names = "{col}_centered_within_channel")
      ) %>%
      ungroup() %>%
      mutate(
        across(all_of(str_c(vars, "_channel_mean")),
               ~ .x - mean(.x, na.rm = TRUE), .names = "{col}_centered")
      ),
    .progress = TRUE
  ) %>%
    list_rbind() %>%
    write_parquet("C:/Users/I567766/Documents/ccs/scored_comments.parquet.gzip")
}
