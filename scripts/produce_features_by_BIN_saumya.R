# loading in packages
library(plyr)
library(fuzzyjoin)
library(dplyr)
library(XML)
library(arrow)
library(tidyverse)
library(lubridate)
library(kableExtra)
library(dplyr)
library(DescTools)
library(tidyverse)
library(dplyr)
library(tidytext)
library(ggplot2)
library(forcats)
library(sentimentr)
library(datawizard)
library(ROCR)
library(rvest)
library(MLmetrics)
library(rpart)
library(rpart.plot)
library(Metrics)
library(class)
library(FNN)
library(rusps)
library(caret)
library(class)
library(dplyr)
library(e1071)
library(FNN) 
library(gmodels) 
library(psych)
# install.packages("naivebayes")
library(naivebayes)

# setting the seed
set.seed(1234)

# loading in the data
dispatch_2021_data <- read_parquet("./data/year=2021/part-0.parquet")
fitness_data <- read_csv("./data/fitness.csv")
summary_data <- read_csv("./data/summary.csv")
alarm_box_locations_data <- read_csv("./data/alarm_box_locations.csv")
inspections_data <- read_csv("./data/inspections.csv")
# not used
rbis_data <- read_csv("./data/rbis.csv")
violation_data <- read_csv("./data/violation orders.csv")

# exploring the data
dispatch_2021_data %>%
  select(ALARM_BOX_BOROUGH, ALARM_BOX_NUMBER, ALARM_BOX_LOCATION) %>%
  arrange(ALARM_BOX_NUMBER) %>%
  distinct()
# 17,313 unique alarm boxes

inspections_data %>%
  distinct(BIN) %>%
  nrow()
# 115,655 unique BINS

head(inspections_data %>% distinct(PREM_ADDR)) %>% arrange(PREM_ADDR)
# 139,176 unique addressess

all_bins_addresses <- inspections_data %>%
  select(BIN, POSTCODE, BOROUGH, Number, Street, LATITUDE, LONGITUDE) %>%
  distinct() %>%
  filter(BIN != 0)

# get count of recent fire related fitness certificates for BINs
idx <- str_detect(unique(fitness_data$COF_TYPE), "FIRE")
fire_related_inspections <- unique(fitness_data$COF_TYPE)[idx]

fitness_certs <- fitness_data %>%
  filter(COF_TYPE %in% fire_related_inspections) %>%
  na.omit() %>%
  select(BIN, COF_TYPE, EXPIRES_ON) %>%
  mutate(
    year = year(mdy(EXPIRES_ON)),
    # NOTE: change this if using dispatch data from 2020 and before
    # recent = in the last three years (2019 - 2021 fitness certs)
    recent = year >= 2019 & year <= 2021
  ) %>%
  filter(recent) %>%
  select(-recent) %>%
  count(BIN) %>%
  rename(
    num_recent_fire_fitness_certs = n
  )

# number of sprinklers and types, and violations for BINs
colnames(summary_data)
sprinkler_summary <- summary_data %>%
  select(
    BIN,
    NUM_SIAM_SPRINKLER,
    SPRINKLER_TYPE,
    NUM_SIAM_STANDPIPE,
    STANDPIPE_TYPE
  ) %>%
  rowwise() %>%
  mutate(
    NUM_SIAM_SPRINKLER = ifelse(!is.na(NUM_SIAM_SPRINKLER),
                                sum(as.numeric(str_split(NUM_SIAM_SPRINKLER, ";", simplify = T)), na.rm = T),
                                NA
    ),
    NUM_SIAM_STANDPIPE = ifelse(!is.na(NUM_SIAM_STANDPIPE),
                                sum(as.numeric(str_split(NUM_SIAM_STANDPIPE, ";", simplify = T)), na.rm = T),
                                NA
    )
  )

# violations are OPEN violations -> let's assume all buildings without violations have none
open_violations <- summary_data %>% select(
  BIN, NUM_OF_VIOLATION_NOTICES, NUM_OF_VIOLATION_ORDER
)

table(inspections_data$LAST_INSP_STAT)

hazard_level_of_BINs <- inspections_data %>%
  filter(BIN != 0) %>%
  mutate(
    approved = ifelse(LAST_INSP_STAT == "APPROVAL", T, F)
  ) %>%
  select(BIN, ALPHA) %>%
  rename(
    hazardous_level = ALPHA
  ) %>%
  group_by(BIN) %>%
  count(hazardous_level) %>%
  pivot_wider(
    names_from = hazardous_level,
    values_from = n,
    names_prefix = "hazard_",
    values_fill = 0
  )

# TODO: remove the hazard levels with almost nothing
colMeans(hazard_level_of_BINs)

# proportion approved inspections by BIN
approved_inspections <- inspections_data %>%
  filter(BIN != 0) %>%
  mutate(
    approved = ifelse(LAST_INSP_STAT == "APPROVAL", T, F)
  ) %>%
  select(BIN, LAST_FULL_INSP_DT, approved) %>%
  group_by(BIN) %>%
  count(approved) %>%
  pivot_wider(
    names_from = approved,
    values_from = n,
    names_prefix = "approved_",
    values_fill = 0
  ) %>%
  select(-approved_NA) %>%
  mutate(
    proportion_approved = approved_TRUE / sum(approved_TRUE, approved_FALSE)
  )

all_features_by_BIN <- hazard_level_of_BINs %>%
  left_join(sprinkler_summary) %>%
  left_join(approved_inspections) %>%
  left_join(fitness_certs) %>%
  left_join(open_violations) %>%
  mutate(
    num_recent_fire_fitness_certs = ifelse(is.na(num_recent_fire_fitness_certs), 0, num_recent_fire_fitness_certs),
    NUM_OF_VIOLATION_NOTICES = ifelse(is.na(NUM_OF_VIOLATION_NOTICES), 0, NUM_OF_VIOLATION_NOTICES),
    NUM_OF_VIOLATION_ORDER = ifelse(is.na(NUM_OF_VIOLATION_ORDER), 0, NUM_OF_VIOLATION_ORDER)
  )

# TODO: Link alarm box number to BIN?
all_bins_addresses

data2021 <-
  dispatch_2021_data %>%
  mutate(
    INCIDENT_CLASSIFICATION = as.factor(INCIDENT_CLASSIFICATION)
  ) %>%
  mutate(
    false_alarm = ifelse(
      INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary", 1, 0),
    INCIDENT_DATETIME = as.POSIXct(INCIDENT_DATETIME, format = "%m/%d/%Y %I:%M:%S %p"),
    incident_month = month(INCIDENT_DATETIME),
    incident_hour = hour(INCIDENT_DATETIME),
    incident_weekday = weekdays(INCIDENT_DATETIME),
    ALARM_BOX_NUMBER = case_when(
      nchar(ALARM_BOX_NUMBER) == 4 ~ paste0(ALARM_BOX_NUMBER),
      nchar(ALARM_BOX_NUMBER) == 3 ~ paste0("0", ALARM_BOX_NUMBER),
      nchar(ALARM_BOX_NUMBER) == 2 ~ paste0("00", ALARM_BOX_NUMBER),
      nchar(ALARM_BOX_NUMBER) == 1 ~ paste0("000", ALARM_BOX_NUMBER),
    ),
    INCIDENT_BOROUGH = as.factor(INCIDENT_BOROUGH),
    ZIPCODE = as.factor(ZIPCODE),
  )

plot(table(data2021$incident_hour))

fit1 <- glm(formula = false_alarm ~ 1, data = data2021, family = "binomial")
summary(fit1)
exp(coef(fit1))

# don't do glm on alarm box number
fit2 <- glm(formula = false_alarm ~ INCIDENT_BOROUGH, data = data2021, family = "binomial")
summary(fit2)
exp(coef(fit2))

# find bin number from alarm box number nyc

# analysis

# STEP 1

# creating a dataset which has all the features we wish to consider as predictors 
# along with the associated zipcodes
false_alarm_by_zip_code <- data2021 %>% group_by(ZIPCODE) %>% summarise(number_of_false_alarms = sum(false_alarm))
inspections_data$LAST_FULL_INSP_DT <- mdy(inspections_data$LAST_FULL_INSP_DT)
inspection_by_zip_code <- inspections_data %>% group_by(POSTCODE) %>% summarise(last_inspection_status_vio = sum(LAST_INSP_STAT == 'NOT APPROVAL(W/REASON)', na.rm = T) + sum(LAST_INSP_STAT == 
                                                                                                                       'NOT APPROVAL(VIO)', na.rm = T),
                                                               median_last_full_inspection_date = median(LAST_FULL_INSP_DT))
inspection_by_zip_code <- inspection_by_zip_code %>% rename('ZIPCODE' = 'POSTCODE')
inspection_by_zip_code$ZIPCODE <- as.numeric((as.character(inspection_by_zip_code$ZIPCODE)))
false_alarm_by_zip_code$ZIPCODE <- as.numeric((as.character(false_alarm_by_zip_code$ZIPCODE)))
temp <- left_join(false_alarm_by_zip_code, inspection_by_zip_code, by = 'ZIPCODE')
fitness_data$EXPIRES_ON <- mdy(fitness_data$EXPIRES_ON)
expiry_by_zip_code <- fitness_data %>% group_by(POSTCODE) %>% summarise(median_cert_expiry_date = median(EXPIRES_ON, na.rm = T)) 
expiry_by_zip_code <- expiry_by_zip_code %>% rename('ZIPCODE' = 'POSTCODE')
temp <- left_join(temp, expiry_by_zip_code, by = 'ZIPCODE')
violation_data$VIO_DATE <- mdy(violation_data$VIO_DATE)
violation_by_zip_code <- violation_data %>% rename('ZIPCODE' = 'POSTCODE') %>% group_by(ZIPCODE) %>% 
  summarise(median_violation_date = median(VIO_DATE, na.rm = T))
temp <- left_join(temp, violation_by_zip_code, by = 'ZIPCODE')
temp$days_from_median_cert_expiry_date <- temp$median_cert_expiry_date - median(temp$median_cert_expiry_date, na.rm = T)
temp$days_from_median_last_full_inspection_date <- temp$median_last_full_inspection_date - median(temp$median_last_full_inspection_date, na.rm = T)
temp$days_from_median_violation_date <- temp$median_violation_date - median(temp$median_violation_date, na.rm = T)
temp <- temp %>% select(-median_cert_expiry_date, -median_last_full_inspection_date, -median_violation_date)
temp$ZIPCODE <- as.factor(temp$ZIPCODE)
temp <- na.omit(temp)
names(temp)
temp$days_from_median_cert_expiry_date <- as.numeric(temp$days_from_median_cert_expiry_date)
temp$days_from_median_last_full_inspection_date <- as.numeric(temp$days_from_median_last_full_inspection_date)
temp$days_from_median_violation_date <- as.numeric(temp$days_from_median_violation_date)
temp$false_alarms_above_below_median <- ifelse(temp$number_of_false_alarms < median(temp$number_of_false_alarms), 0, 1)

# temp is our data with features by zipcode

# we wish to find which zipcodes are most likely to have false alarms above the median number 
# in the city in a year

# dividing into train, validation and test sets
train_indices <- sample(nrow(temp), 109)
validation_indices <- sample(setdiff(1:nrow(temp), train_indices), 55)
test_indices <- sample(setdiff(1:nrow(temp), c(train_indices, validation_indices)), 55)
train <- temp[train_indices, ]
validate <- temp[validation_indices, ]
test <- temp[test_indices, ]

nrow(train)
nrow(validate)
nrow(test)

threshold <- 0.7

# Logistic Regression
fit_glm <- glm(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
             days_from_median_last_full_inspection_date +
     days_from_median_violation_date, data = train, family = 'binomial')
predicted_prob_glm <- as.numeric(predict(fit_glm, newdata = validate, type = "response"))
validate$predicted_prob_glm <- predicted_prob_glm
validate$prediction_glm <- ifelse(validate$predicted_prob_glm > threshold, 1, 0)
# confusion matrix
table(validate$prediction_glm, validate$false_alarms_above_below_median)
# accuracy
accuracy_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] + 
     table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_glm, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_glm, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_glm)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarms_above_below_median == 1)
  actual_positives <- sum(validate$false_alarms_above_below_median == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - Logistic Regression")
# Calibration Plot???

# KNN Regression
k <- 5
fit_knn <- knn.reg(train = train %>% select(-ZIPCODE), 
                   test = validate %>% select(-ZIPCODE, -predicted_prob_glm, -prediction_glm), 
                   y = train$false_alarms_above_below_median, k = k)
validate$predicted_prob_knn <- as.numeric(fit_knn$pred)
validate$prediction_knn <- ifelse(validate$predicted_prob_knn > threshold, 1, 0)
# confusion matrix
table(validate$prediction_knn, validate$false_alarms_above_below_median)
# accuracy
accuracy_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] + 
     table(validate$prediction_knn
           , validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_knn, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_knn <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_knn, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_knn)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarms_above_below_median == 1)
  actual_positives <- sum(validate$false_alarms_above_below_median == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - KNN")
# Calibration Plot???

# Naive Baye's
train$false_alarms_above_below_median <- as.factor(as.character(train$false_alarms_above_below_median))
validate$false_alarms_above_below_median <- as.factor(as.character(validate$false_alarms_above_below_median))
fit_nb <- naive_bayes(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
                        days_from_median_last_full_inspection_date +
                        days_from_median_violation_date, data = train, usekernel = T)
predicted_prob_nb <- predict(fit_nb, newdata = validate, type = "prob")
validate$predicted_prob_nb <- predicted_prob_nb[, 2]
validate$prediction_nb <- ifelse(validate$predicted_prob_nb > threshold, 1, 0)
# confusion matrix
table(validate$prediction_nb, validate$false_alarms_above_below_median)
# accuracy
accuracy_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] + 
     table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_nb, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_nb <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_nb, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_nb)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarms_above_below_median == 1)
  actual_positives <- sum(validate$false_alarms_above_below_median == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - Naive Baye's")
# Calibration Plot???

# Random Forest
library(ranger)
fit_rf <- ranger(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
                   days_from_median_last_full_inspection_date +
                   days_from_median_violation_date, data = train, num.trees = 100, probability = T)
predicted_prob_rf <- predict(fit_rf, data = validate, type = "response")
validate$predicted_prob_rf <- predicted_prob_rf$predictions[, 2]
validate$prediction_rf <- ifelse(validate$predicted_prob_rf > threshold, 1, 0)
# confusion matrix
table(validate$prediction_rf, validate$false_alarms_above_below_median)
# accuracy
accuracy_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] + 
     table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_rf, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_rf <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_rf, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_rf)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarms_above_below_median == 1)
  actual_positives <- sum(validate$false_alarms_above_below_median == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - Random Forest")
# Calibration Plot???

accuracy_glm
accuracy_knn
accuracy_nb
accuracy_rf

precision_glm
precision_knn
precision_nb
precision_rf

recall_glm
recall_knn
recall_nb
recall_rf

auc_glm
auc_knn
auc_nb
auc_rf

# best performance is using Random Forest model: AUC = 0.8915344

# refitting model on Train and Validate using Naive Baye's
fit_rf2 <- ranger(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
                   days_from_median_last_full_inspection_date +
                   days_from_median_violation_date, data = rbind(train, validate %>% select(-contains('pred'))), num.trees = 100, probability = T)
predicted_prob_rf2 <- predict(fit_rf2, data = test, type = "response")
test$predicted_prob_rf2 <- predicted_prob_rf2$predictions[, 2]
test$prediction_rf2 <- ifelse(test$predicted_prob_rf2 > threshold, 1, 0)
# confusion matrix
table(test$prediction_rf2, validate$false_alarms_above_below_median)
# AUC
pred <- prediction(test$prediction_rf2, test$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_rf2 <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_rf2, "\n")

# we now use this model to predict which zipcodes are the most likely to report false fire alarms more than the
# median number for the whole city
temp$predictions <- (predict(fit_rf2, data = temp, type = 'response'))$predictions[, 2]
zipcodes_in_focus <- as.numeric(as.character((temp %>% filter(predictions == 1))$ZIPCODE))
zipcodes_in_focus

# STEP 2

# let us focus on the zipcodes that are most likely to report false fire alarms more than the
# median number for the whole city and switch to data2021 which has a feature called ALARM_BOX_NUMBERS.
# We shall focus on the ALARM_BOX_NUMBERS associated with the zipcodes_in_focus
most_false_alarms_by_zipcode <- data.frame(data2021 %>% filter(any(ZIPCODE %in% zipcodes_in_focus)))

# we are now interested in predicting which alarm box numbers (in these zipcodes which are the most likely to 
# report false fire alarms more than the median number for the whole city) are most likely to report 
# false fire alarms

most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(-STARFIRE_INCIDENT_ID, -INCIDENT_DATETIME, -ALARM_BOX_BOROUGH, -ALARM_BOX_LOCATION,
                                -INCIDENT_BOROUGH, -ZIPCODE, -POLICEPRECINCT, -CITYCOUNCILDISTRICT, -COMMUNITYDISTRICT,
                                -COMMUNITYSCHOOLDISTRICT, -CONGRESSIONALDISTRICT, -ALARM_SOURCE_DESCRIPTION_TX, -INCIDENT_CLASSIFICATION,
                                -INCIDENT_CLASSIFICATION_GROUP, -DISPATCH_RESPONSE_SECONDS_QY, -FIRST_ASSIGNMENT_DATETIME, -FIRST_ACTIVATION_DATETIME,
                                -FIRST_ON_SCENE_DATETIME, -INCIDENT_CLOSE_DATETIME, VALID_DISPATCH_RSPNS_TIME_INDC, -VALID_INCIDENT_RSPNS_TIME_INDC,
                                -INCIDENT_RESPONSE_SECONDS_QY, -INCIDENT_TRAVEL_TM_SECONDS_QY, -ENGINES_ASSIGNED_QUANTITY, -LADDERS_ASSIGNED_QUANTITY,
                                -OTHER_UNITS_ASSIGNED_QUANTITY)
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(-ALARM_LEVEL_INDEX_DESCRIPTION)
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(-HIGHEST_ALARM_LEVEL, -VALID_DISPATCH_RSPNS_TIME_INDC)

most_false_alarms_by_zipcode$incident_month <- month.name[most_false_alarms_by_zipcode$incident_month]
most_false_alarms_by_zipcode$incident_month <- as.factor(most_false_alarms_by_zipcode$incident_month)
most_false_alarms_by_zipcode$incident_hour <- as.factor(most_false_alarms_by_zipcode$incident_hour)
most_false_alarms_by_zipcode$incident_weekday <- as.factor(most_false_alarms_by_zipcode$incident_weekday)

most_false_alarms_by_zipcode$false_alarm <- as.factor(most_false_alarms_by_zipcode$false_alarm)
most_false_alarms_by_zipcode <- na.omit(most_false_alarms_by_zipcode)
train_indices <- sample(nrow(most_false_alarms_by_zipcode), 173702)
validation_indices <- sample(setdiff(1:nrow(most_false_alarms_by_zipcode), train_indices), 86852)
test_indices <- sample(setdiff(1:nrow(most_false_alarms_by_zipcode), c(train_indices, validation_indices)), 86851)
train <- most_false_alarms_by_zipcode[train_indices, ]
validate <- most_false_alarms_by_zipcode[validation_indices, ]
test <- most_false_alarms_by_zipcode[test_indices, ]

nrow(train)
nrow(validate)
nrow(test)

threshold <- 0.7

# Logistic Regression
weights <- ifelse(train$false_alarm == 0, 1, 171306/2413)
fit_glm <- glm(false_alarm ~ incident_weekday + incident_hour + 
                 incident_month, data = train, family = 'binomial', weights = weights)
predicted_prob_glm <- as.numeric(predict(fit_glm, newdata = validate, type = "response"))
validate$predicted_prob_glm <- predicted_prob_glm
validate$prediction_glm <- ifelse(validate$predicted_prob_glm > threshold, 1, 0)
# confusion matrix
table(validate$prediction_glm, validate$false_alarm)
# accuracy
accuracy_glm <- (table(validate$prediction_glm, validate$false_alarm)[1, 1] + table(validate$prediction_glm, validate$false_alarm)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarm)[1, 1] + table(validate$prediction_glm, validate$false_alarm)[2, 2] + 
     table(validate$prediction_glm, validate$false_alarm)[2, 1] + table(validate$prediction_glm, validate$false_alarm)[1, 2])
# precision
precision_glm <- (table(validate$prediction_glm, validate$false_alarm)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarm)[2, 2] + table(validate$prediction_glm, validate$false_alarm)[2, 1])
# recall
recall_glm <- (table(validate$prediction_glm, validate$false_alarm)[2, 2])/
  (table(validate$prediction_glm, validate$false_alarm)[2, 2] + table(validate$prediction_glm, validate$false_alarm)[1, 2])
# AUC
pred <- prediction(validate$prediction_glm, validate$false_alarm)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_glm, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_glm)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarm == 1)
  actual_positives <- sum(validate$false_alarm == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - Logistic Regression")
# Calibration Plot???

# KNN, Naive Baye's do not allow for weights to be applied to the outcomes. Thus, we will forgo it from our
# consideration

# Random Forest
library(ranger)
fit_rf <- ranger(false_alarm ~ incident_weekday + incident_hour + 
                   incident_month, data = train, num.trees = 100, probability = T, 
                 sample.fraction = c(.5, .5), keep.inbag = TRUE, class.weights = c(1, 10))
predicted_prob_rf <- predict(fit_rf, data = validate, type = "response")
validate$predicted_prob_rf <- predicted_prob_rf$predictions[, 2]
validate$prediction_rf <- ifelse(validate$predicted_prob_rf > threshold, 1, 0)
# confusion matrix
table(validate$prediction_rf, validate$false_alarm)
# accuracy
accuracy_rf <- (table(validate$prediction_rf, validate$false_alarm)[1, 1] + table(validate$prediction_rf, validate$false_alarm)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarm)[1, 1] + table(validate$prediction_rf, validate$false_alarm)[2, 2] + 
     table(validate$prediction_rf, validate$false_alarm)[2, 1] + table(validate$prediction_rf, validate$false_alarm)[1, 2])
# precision
precision_rf <- (table(validate$prediction_rf, validate$false_alarm)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarm)[2, 2] + table(validate$prediction_rf, validate$false_alarm)[2, 1])
# recall
recall_rf <- (table(validate$prediction_rf, validate$false_alarm)[2, 2])/
  (table(validate$prediction_rf, validate$false_alarm)[2, 2] + table(validate$prediction_rf, validate$false_alarm)[1, 2])
# AUC
pred <- prediction(validate$prediction_rf, validate$false_alarm)
perf <- performance(pred, "auc")
auc_rf <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_rf, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
recall <- sapply(k_values, function(k) {
  top_k <- head(validate %>% arrange(desc(predicted_prob_rf)), round(nrow(validate) * k))
  true_positives <- sum(top_k$false_alarm == 1)
  actual_positives <- sum(validate$false_alarm == 1)
  recall <- true_positives / actual_positives
  recall
})
data <- data.frame(k_values, recall)
ggplot(data, aes(x = k_values, y = recall)) +
  geom_line() +
  geom_point() +
  xlab("Percentage of Predictions (k)") +
  ylab("Recall") +
  ggtitle("Recall at k% Plot - Random Forest")
# Calibration Plot???

accuracy_glm
accuracy_rf

precision_glm
precision_rf

recall_glm
recall_rf

auc_glm
auc_rf

# best performance is using Logistic Regression Model: AUC = 0.5894677

# even though we adjusted for the class weights (there were many more true fire alarms v/s false),
# we still were not able to find a model that predicts whether alarm boxes (in zipcodes most likely to have false 
# fire alarms) are going to have a false fire alarm depending on the day/month/time of day

# refitting model on Train and Validate using logistic regression
weights <- ifelse((rbind(train, validate %>% select(-contains('pred'))))$false_alarm == 0, 1, 256977/3577)
fit_glm2 <- glm(false_alarm ~ incident_weekday + incident_hour + 
                 incident_month, data = rbind(train, validate %>% select(-contains('pred'))), 
               family = 'binomial', weights = weights)
predicted_prob_glm <- as.numeric(predict(fit_glm2, newdata = test, type = "response"))
test$predicted_prob_glm <- predicted_prob_glm
test$prediction_glm <- ifelse(test$predicted_prob_glm > threshold, 1, 0)
# AUC
pred <- prediction(test$prediction_glm, test$false_alarm)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_glm, "\n")

# we now use this model to predict which alarm boxes are the most likely to report false fire alarms 
# depending on the month/day/time of day
most_false_alarms_by_zipcode$predictions <- predict(fit_glm2, most_false_alarms_by_zipcode)
most_false_alarms_by_zipcode %>% arrange(desc(predictions))
