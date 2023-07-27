library(tidyverse)
library(arrow)
options(arrow.skip_nul = TRUE)
library(lme4)
library(lmerTest)

d <- read_parquet("C:/Users/I567766/Documents/ccs/scored_comments.parquet.gzip")
View(d)

# modeling --------------------------------------------------------------------
m0_vi <- lmer(verbal_immediacy ~ 1 + (1 | author_channel_id) + (1 | channel_name), d)
summary(m0_vi)
performance::icc(m0_vi)
psych::describe(d$verbal_immediacy)


# **Within-Person Level:**
#   
# H1: The within-person magnitude of (a) verbal immediacy and (b) comment valence **intensity** will increase over time as indicated by a greater time difference between comment at t0 and first comment.
# 
# H2: The within-person magnitude of (a) verbal immediacy and (b) comment valence **intensity** will increase with higher interaction frequency as indicated by (1.) shorter time difference between comment at t0 and previous comment and (2.) shorter time difference between video at t0 and previous video.

growthmodel_vi <- 
  lmer(verbal_immediacy ~
         weeks_since_first_comment + 
         weeks_since_last_comment_centered_within_person + 
         weeks_since_last_comment_person_mean_centered + 
         #weeks_since_last_video_centered_within_person +
         #weeks_since_last_video_person_mean_centered +
         (1 | author_channel_id) +
         (1 | channel_name), data = d)

summary(growthmodel_vi)


################################################################################

m0_pa <- lmer(pos_affect ~ 1 + (1 | author_channel_id) + (1 | channel_name), d)
summary(m0_pa)
performance::icc(m0_pa)
psych::describe(d$pos_affect)

growthmodel_pa <- 
  lmer(pos_affect ~
         weeks_since_first_comment + 
         weeks_since_last_comment_centered_within_person + 
         weeks_since_last_comment_person_mean_centered + 
#         weeks_since_last_video_centered_within_person +
#         weeks_since_last_video_person_mean_centered +
         (1 | author_channel_id) +
         (1 | channel_name), data = d)

summary(growthmodel_pa)

################################################################################

m0_na <- lmer(neg_affect ~ 1 + (1 | author_channel_id) + (1 | channel_name), d)
summary(m0_na)
performance::icc(m0_na)
psych::describe(d$neg_affect)

growthmodel_na <- 
  lmer(neg_affect ~
         weeks_since_first_comment + 
         weeks_since_last_comment_centered_within_person + 
         weeks_since_last_comment_person_mean_centered + 
         # weeks_since_last_video_centered_within_person +
         # weeks_since_last_video_person_mean_centered +
         (1 | author_channel_id) +
         (1 | channel_name), data = d)

summary(growthmodel_na)

################################################################################

library(coefplot2)
coefplot2(growthmodel_na,
          frame.plot=TRUE, 
          main="Koeffizienten für negativen Affekt",
          varnames=c("W. seit ersten Kommentar", "W. seit letzem Kommentar (Within)","W. seit letztem Kommentar (Between)")
)
