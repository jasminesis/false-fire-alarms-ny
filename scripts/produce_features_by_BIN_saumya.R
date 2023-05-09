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

# install.packages("remotes")
# remotes::install_github("hansthompson/rusps")
library(rusps)

dispatch_2021_data <- read_parquet("./data/year=2021/part-0.parquet")
fitness_data <- read_csv("./data/fitness.csv")
summary_data <- read_csv("./data/summary.csv")
alarm_box_locations_data <- read_csv("./data/alarm_box_locations.csv")
inspections_data <- read_csv("./data/inspections.csv")
# Not used
rbis_data <- read_csv("./data/rbis.csv")
violation_data <- read_csv("./data/violation orders.csv")

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

# Number of sprinklers and types, and violations for BINs
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
# Violations are OPEN violations -> let's assume all buildings without violations have none
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

# find bin number from alarm box number nyc????????
# 7182813917

names(data2021)

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

sampled <- sample(temp)
0.5*233
train <- sampled[1:116,]
233*.25
validate <- sampled[117:175,]
test <- sampled[176:233,]

nrow(train)
nrow(validate)
nrow(test)

threshold <- 0.8

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
                   y = temp$false_alarms_above_below_median, k = k)
validate$fit_knn <- as.numeric(fit_knn$pred)
validate$prediction_knn <- ifelse(validate$fit_knn > threshold, 1, 0)
# confusion matrix
table(validate$prediction_knn, validate$false_alarms_above_below_median)
# accuracy
accuracy_knn <- (36 + 1)/(36 + 1 + 17 + 2)
# precision
precision_knn <- (1)/(1 + 2)
# recall
recall_knn <- (1)/(1 + 17)
# AUC
pred <- prediction(validate$prediction_knn, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_knn <- perf@y.values[[1]]
cat('The AUC score on the validate set is', auc_knn, "\n")
# Recall at k% plot
k_values <- seq(0.1, 1, by = 0.1)
validate <- validate %>% rename('predicted_prob_knn' = 'fit_knn')
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

# Naive Baye's
# install.packages("naivebayes")
library(naivebayes)
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
accuracy_nb <- (36 + 14)/(36 + 14 + 4 + 2)
# precision
precision_nb <- (14)/(14 + 2)
# recall
recall_nb <- (14)/(14 + 4)
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



library(rms)

# Assuming you have the following columns in your validate dataset:
# predicted_prob_nb: Predicted probabilities
# true_label: True labels (1 for positive, 0 for negative)

# Step 1: Prepare the data
calibration_data <- data.frame(predicted_prob_nb = validate$predicted_prob_nb,
                               true_label = validate$true_label)

# Step 2: Create the calibration plot
calibration_plot <- calibration(predicted_prob_nb ~ true_label, data = calibration_data, curve = TRUE)

# Step 3: Visualize the calibration plot
plot(calibration_plot, ylim = c(0, 1), main = "Calibration Plot")






# random forest regression
library(ranger)
fit_rf <- ranger(number_of_false_alarms ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
                   days_from_median_last_full_inspection_date +
                   days_from_median_violation_date, data = train, num.trees = 100, probability = T)
predict(fit_rf, validate, response = 'prob')$predictions



dec_tree <- rpart(number_of_false_alarms ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
                    days_from_median_last_full_inspection_date +
                    days_from_median_violation_date, data = train)
rpart.plot(dec_tree)
dec_tree$variable.importance/max(dec_tree$variable.importance)
rmse()
library(tree)
tree(number_of_false_alarms ~ last_inspection_status_vio + days_from_median_cert_expiry_date + 
       days_from_median_last_full_inspection_date +
       days_from_median_violation_date, data = train)

# looking at the 5 zipcodes with the most amount of fire alarms
temp7 %>% arrange(desc(number_of_false_alarms))

zipcodes_with_max_false_alarms <- c(11201, 10016, 10036, 10003, 10019)

data2021_11201 <- data2021 %>% filter(ZIPCODE == 11201) %>% mutate(na.omit(temp7[temp7$ZIPCODE == 11201,]$days_from_median_cert_expiry_date), 
                                                 na.omit(temp7[temp7$ZIPCODE == 11201,]$days_from_median_violation_date), 
                                                 na.omit(temp7[temp7$ZIPCODE == 11201,]$days_from_median_last_full_inspection_date)) %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary")
data2021_10016 <- data2021 %>% filter(ZIPCODE == 10016) %>% mutate(na.omit(temp7[temp7$ZIPCODE == 10016,]$days_from_median_cert_expiry_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10016,]$days_from_median_violation_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10016,]$days_from_median_last_full_inspection_date)) %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary")
data2021_10036 <- data2021 %>% filter(ZIPCODE == 10036) %>% mutate(na.omit(temp7[temp7$ZIPCODE == 10036,]$days_from_median_cert_expiry_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10036,]$days_from_median_violation_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10036,]$days_from_median_last_full_inspection_date)) %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary")
data2021_10003 <- data2021 %>% filter(ZIPCODE == 10003) %>% mutate(na.omit(temp7[temp7$ZIPCODE == 10003,]$days_from_median_cert_expiry_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10003,]$days_from_median_violation_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10003,]$days_from_median_last_full_inspection_date)) %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary")
data2021_10019 <- data2021 %>% filter(ZIPCODE == 10019) %>% mutate(na.omit(temp7[temp7$ZIPCODE == 10019,]$days_from_median_cert_expiry_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10019,]$days_from_median_violation_date), 
                                                                   na.omit(temp7[temp7$ZIPCODE == 10019,]$days_from_median_last_full_inspection_date)) %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary")

# let us predict which alarm box numbers within across these zipcodes are most likely to report false alarms
combined <- rbind(data2021_11201, data2021_10016, data2021_10036, data2021_10003, data2021_10019)
combined <- combined %>% select(-STARFIRE_INCIDENT_ID, -INCIDENT_DATETIME, -ALARM_BOX_BOROUGH, -ALARM_BOX_LOCATION,
                                -INCIDENT_BOROUGH, -ZIPCODE, -POLICEPRECINCT, -CITYCOUNCILDISTRICT, -COMMUNITYDISTRICT,
                                -COMMUNITYSCHOOLDISTRICT, -CONGRESSIONALDISTRICT, -ALARM_SOURCE_DESCRIPTION_TX, -INCIDENT_CLASSIFICATION,
                                -INCIDENT_CLASSIFICATION_GROUP, -DISPATCH_RESPONSE_SECONDS_QY, -FIRST_ASSIGNMENT_DATETIME, -FIRST_ACTIVATION_DATETIME,
                                -FIRST_ON_SCENE_DATETIME, -INCIDENT_CLOSE_DATETIME, VALID_DISPATCH_RSPNS_TIME_INDC, -VALID_INCIDENT_RSPNS_TIME_INDC,
                                -INCIDENT_RESPONSE_SECONDS_QY, -INCIDENT_TRAVEL_TM_SECONDS_QY, -ENGINES_ASSIGNED_QUANTITY, -LADDERS_ASSIGNED_QUANTITY,
                                -OTHER_UNITS_ASSIGNED_QUANTITY)
combined <- combined %>% select(-ALARM_LEVEL_INDEX_DESCRIPTION)
combined <- combined %>% select(-HIGHEST_ALARM_LEVEL, -VALID_DISPATCH_RSPNS_TIME_INDC, -false_alarm)
num_of_fake_alarms_by_box_num <- data2021 %>% filter(INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary") %>% 
  group_by(ALARM_BOX_NUMBER) %>% count()
combined <- left_join(combined, num_of_fake_alarms_by_box_num, by = 'ALARM_BOX_NUMBER')
combined <- combined %>% rename(number_false_alarms = n)
combined$incident_month <- month.name[combined$incident_month]
combined$incident_month <- as.factor(combined$incident_month)
combined$incident_hour <- as.factor(combined$incident_hour)
combined$incident_weekday <- as.factor(combined$incident_weekday)
lm(number_false_alarms ~ incident_month + incident_hour + incident_weekday, data = combined)
