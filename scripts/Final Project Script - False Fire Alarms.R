# loading in packages
library(dplyr)
library(FNN)
library(lubridate)
library(tidyverse)
library(arrow)
library(caret)
library(naivebayes)
library(ranger)
library(ROCR)

# setting the seed
set.seed(1234)

# loading in the data
dispatch_2021_data <- read_parquet("./data/year=2021/part-0.parquet")
fitness_data <- read_csv("./data/fitness.csv")
inspections_data <- read_csv("./data/inspections.csv")
violation_data <- read_csv("./data/violation orders.csv")

all_bins_addresses <- inspections_data %>%
  select(BIN, POSTCODE, BOROUGH, Number, Street, LATITUDE, LONGITUDE) %>%
  distinct() %>%
  filter(BIN != 0)

# subsetting to include only fire-related fitness certificates
fitness_data <- fitness_data[which(str_detect(fitness_data$COF_TYPE, "FIRE")), ]

# proportion approved inspections by ZIPCODE
approved_inspections <- inspections_data %>%
  filter(BIN != 0) %>%
  mutate(
    approved = ifelse(LAST_INSP_STAT == "APPROVAL", T, F)
  ) %>%
  select(LAST_FULL_INSP_DT, approved, POSTCODE) %>%
  group_by(POSTCODE) %>%
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

# cleaning the main dataset
data2021 <-
  dispatch_2021_data %>%
  mutate(
    INCIDENT_CLASSIFICATION = as.factor(INCIDENT_CLASSIFICATION)
  ) %>%
  mutate(
    false_alarm = ifelse(
      INCIDENT_CLASSIFICATION == "Alarm System - Defective" | INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary", 1, 0
    ),
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

# plotting the number of false alarms by borough
ggplot(data2021, aes(x = INCIDENT_BOROUGH, y = false_alarm)) +
  geom_bar(stat = "identity", col = "red") +
  xlab("Borough") +
  ylab("Number of false fire alarms") +
  labs(title = "Number of false fire alarms by borough") +
  theme(
    plot.title = element_text(size = 30, face = "bold.italic"),
    axis.title.x = element_text(size = 20, face = "bold"),
    axis.title.y = element_text(size = 20, face = "bold"),
    axis.text = element_text(size = 15)
  )

# STEP 1

# creating a dataset which has all the features we wish to consider as predictors
# along with the associated zipcodes
false_alarm_by_zip_code <- data2021 %>%
  group_by(ZIPCODE) %>%
  summarise(number_of_false_alarms = sum(false_alarm))
inspections_data$LAST_FULL_INSP_DT <- mdy(inspections_data$LAST_FULL_INSP_DT)
inspection_by_zip_code <- inspections_data %>%
  group_by(POSTCODE) %>%
  summarise(
    last_inspection_status_vio = sum(LAST_INSP_STAT == "NOT APPROVAL(W/REASON)", na.rm = T) + sum(LAST_INSP_STAT ==
      "NOT APPROVAL(VIO)", na.rm = T),
    median_last_full_inspection_date = median(LAST_FULL_INSP_DT)
  )
inspection_by_zip_code <- inspection_by_zip_code %>% rename("ZIPCODE" = "POSTCODE")
inspection_by_zip_code$ZIPCODE <- as.numeric((as.character(inspection_by_zip_code$ZIPCODE)))
false_alarm_by_zip_code$ZIPCODE <- as.numeric((as.character(false_alarm_by_zip_code$ZIPCODE)))
step1data <- left_join(false_alarm_by_zip_code, inspection_by_zip_code, by = "ZIPCODE")
fitness_data$EXPIRES_ON <- mdy(fitness_data$EXPIRES_ON)
expiry_by_zip_code <- fitness_data %>%
  group_by(POSTCODE) %>%
  summarise(median_cert_expiry_date = median(EXPIRES_ON, na.rm = T))
expiry_by_zip_code <- expiry_by_zip_code %>% rename("ZIPCODE" = "POSTCODE")
step1data <- left_join(step1data, expiry_by_zip_code, by = "ZIPCODE")
violation_data$VIO_DATE <- mdy(violation_data$VIO_DATE)
violation_by_zip_code <- violation_data %>%
  rename("ZIPCODE" = "POSTCODE") %>%
  group_by(ZIPCODE) %>%
  summarise(median_violation_date = median(VIO_DATE, na.rm = T))
step1data <- left_join(step1data, violation_by_zip_code, by = "ZIPCODE")
step1data$days_from_median_cert_expiry_date <- step1data$median_cert_expiry_date - median(step1data$median_cert_expiry_date, na.rm = T)
step1data$days_from_median_last_full_inspection_date <- step1data$median_last_full_inspection_date - median(step1data$median_last_full_inspection_date, na.rm = T)
step1data$days_from_median_violation_date <- step1data$median_violation_date - median(step1data$median_violation_date, na.rm = T)
step1data <- step1data %>% select(-median_cert_expiry_date, -median_last_full_inspection_date, -median_violation_date)
step1data$ZIPCODE <- as.factor(step1data$ZIPCODE)
step1data <- na.omit(step1data)
names(step1data)
step1data$days_from_median_cert_expiry_date <- as.numeric(step1data$days_from_median_cert_expiry_date)
step1data$days_from_median_last_full_inspection_date <- as.numeric(step1data$days_from_median_last_full_inspection_date)
step1data$days_from_median_violation_date <- as.numeric(step1data$days_from_median_violation_date)
step1data$false_alarms_above_below_median <- ifelse(step1data$number_of_false_alarms < median(step1data$number_of_false_alarms), 0, 1)
approved_inspections <- approved_inspections %>%
  rename("ZIPCODE" = "POSTCODE") %>%
  select(proportion_approved)
approved_inspections$ZIPCODE <- as.factor(approved_inspections$ZIPCODE)
step1data <- left_join(step1data, approved_inspections, by = "ZIPCODE")

# step1data is our data with features by zipcode

# we wish to find which zipcodes are most likely to have false alarms above the median number
# in the city in a year

# dividing into train, validation and test sets
train_indices <- sample(nrow(step1data), 109)
validation_indices <- sample(setdiff(1:nrow(step1data), train_indices), 55)
test_indices <- sample(setdiff(1:nrow(step1data), c(train_indices, validation_indices)), 53)
train <- step1data[train_indices, ]
validate <- step1data[validation_indices, ]
test <- step1data[test_indices, ]

nrow(train)
nrow(validate)
nrow(test)

threshold <- 0.7

# Logistic Regression
fit_glm <- glm(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date +
  days_from_median_last_full_inspection_date +
  days_from_median_violation_date + proportion_approved, data = train, family = "binomial")
predicted_prob_glm <- as.numeric(predict(fit_glm, newdata = validate, type = "response"))
validate$predicted_prob_glm <- predicted_prob_glm
validate$prediction_glm <- ifelse(validate$predicted_prob_glm > threshold, 1, 0)
# confusion matrix
table(validate$prediction_glm, validate$false_alarms_above_below_median)
# accuracy
accuracy_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] +
    table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_glm <- (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_glm, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_glm, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_glm, "\n")
# calibration plot
validate$prediction_glm <- as.factor(validate$prediction_glm)
calPlotData <- calibration(validate$prediction_glm ~ validate$false_alarms_above_below_median)
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - Logistic Regression", xlab = "Prediction: False alarms below (0) or above (100) city median",
  ylab = "Observed: Proportion of false alarms below city median", cex.lab = 3
)

# KNN
k <- 5
fit_knn <- knn(
  train = train %>% select(-ZIPCODE, -false_alarms_above_below_median), test = validate %>%
    select(-contains("pred"), -ZIPCODE, -false_alarms_above_below_median),
  cl = train$false_alarms_above_below_median, k = k, prob = F
)
validate$predicted_prob_knn <- as.numeric(as.character(fit_knn))
validate$prediction_knn <- ifelse(validate$predicted_prob_knn > threshold, 1, 0)
# confusion matrix
table(validate$prediction_knn, validate$false_alarms_above_below_median)
# accuracy
accuracy_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] +
    table(
      validate$prediction_knn,
      validate$false_alarms_above_below_median
    )[2, 1] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_knn <- (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_knn, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_knn, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_knn, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_knn <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_knn, "\n")
# calibration plot
validate$prediction_knn <- as.factor(validate$prediction_knn)
calPlotData <- calibration(validate$prediction_knn ~ validate$false_alarms_above_below_median)
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - KNN", xlab = "Prediction: False alarms below (0) or above (100) city median",
  ylab = "Observed: Proportion of false alarms below city median", cex.lab = 3
)

# Naive Baye's
train$false_alarms_above_below_median <- as.factor(as.character(train$false_alarms_above_below_median))
validate$false_alarms_above_below_median <- as.factor(as.character(validate$false_alarms_above_below_median))
fit_nb <- naive_bayes(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date +
  days_from_median_last_full_inspection_date +
  days_from_median_violation_date + proportion_approved, data = train, usekernel = T)
predicted_prob_nb <- predict(fit_nb, newdata = validate, type = "prob")
validate$predicted_prob_nb <- predicted_prob_nb[, 2]
validate$prediction_nb <- ifelse(validate$predicted_prob_nb > threshold, 1, 0)
# confusion matrix
table(validate$prediction_nb, validate$false_alarms_above_below_median)
# accuracy
accuracy_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] +
    table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_nb <- (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_nb, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_nb, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_nb, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_nb <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_nb, "\n")
# calibration plot
validate$prediction_nb <- as.factor(validate$prediction_nb)
calPlotData <- calibration(validate$prediction_nb ~ as.numeric(as.character(validate$false_alarms_above_below_median)))
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - Naive Bayes", xlab = "Prediction: False alarms below (0) or above (100) city median",
  ylab = "Observed: Proportion of false alarms below city median", cex.lab = 3
)

# Random Forest
fit_rf <- ranger(false_alarms_above_below_median ~ last_inspection_status_vio + days_from_median_cert_expiry_date +
  days_from_median_last_full_inspection_date +
  days_from_median_violation_date + proportion_approved, data = train, num.trees = 100, probability = T)
predicted_prob_rf <- predict(fit_rf, data = validate, type = "response")
validate$predicted_prob_rf <- predicted_prob_rf$predictions[, 2]
validate$prediction_rf <- ifelse(validate$predicted_prob_rf > threshold, 1, 0)
# confusion matrix
table(validate$prediction_rf, validate$false_alarms_above_below_median)
# accuracy
accuracy_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] +
    table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 1] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 2])
# precision
precision_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 1])
# recall
recall_rf <- (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarms_above_below_median)[2, 2] + table(validate$prediction_rf, validate$false_alarms_above_below_median)[1, 2])
# AUC
pred <- prediction(validate$prediction_rf, validate$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_rf <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_rf, "\n")
# calibration plot
validate$prediction_rf <- as.factor(validate$prediction_rf)
calPlotData <- calibration(validate$prediction_rf ~ as.numeric(as.character(validate$false_alarms_above_below_median)))
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - Random Forest", xlab = "Prediction: False alarms below (0) or above (100) city median",
  ylab = "Observed: Proportion of false alarms below city median", cex.lab = 3
)

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

# best performance is using KNN model: AUC = 0.9265873

# refitting model on Train and Validate using KNN
k <- 5
t <- rbind(train, validate %>% select(-contains("pred")))
t$false_alarms_above_below_median <- as.numeric(as.character(t$false_alarms_above_below_median))
fit_knn2 <- knn(
  train = t %>% select(-ZIPCODE), test = test %>% select(-ZIPCODE),
  cl = t$false_alarms_above_below_median, k = k, prob = F
)
test$predicted_prob_knn2 <- as.numeric(as.character(fit_knn2))
test$prediction_knn2 <- ifelse(test$predicted_prob_knn2 > threshold, 1, 0)
# confusion matrix
table(test$prediction_knn2, test$false_alarms_above_below_median)
# AUC
pred <- prediction(test$prediction_knn2, test$false_alarms_above_below_median)
perf <- performance(pred, "auc")
auc_knn2 <- perf@y.values[[1]]
cat("The AUC score on the test set is", auc_knn2, "\n")

# we now use this model to predict which zipcodes are the most likely to report false fire alarms more than the
# median number for the whole city
step1data <- (as.data.frame(sapply(step1data, as.character)))
step1data <- (as.data.frame(sapply(step1data, as.numeric)))
t <- (as.data.frame(sapply(t, as.numeric)))
fit_knn3 <- knn(train = t, test = step1data, cl = t$false_alarms_above_below_median, k = k, prob = F)
step1data$predictions <- as.numeric(as.character((fit_knn3)))
zipcodes_in_focus <- (step1data %>% filter(predictions == 1))$ZIPCODE
zipcodes_in_focus

# STEP 2

# let us focus on the zipcodes that are most likely to report false fire alarms more than the
# median number for the whole city and switch to data2021 which has a feature called ALARM_BOX_NUMBERS.
# We shall focus on the ALARM_BOX_NUMBERS associated with the zipcodes_in_focus
most_false_alarms_by_zipcode <- data.frame(data2021 %>% filter(any(ZIPCODE %in% zipcodes_in_focus)))

# filtering to potentially fire related incidents
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% filter(INCIDENT_CLASSIFICATION_GROUP == "Structural Fires" |
  INCIDENT_CLASSIFICATION_GROUP == "NonStructural Fires" | INCIDENT_CLASSIFICATION_GROUP == "NonMedical Emergencies" |
  INCIDENT_CLASSIFICATION_GROUP == "NonMedical MFAs")

# we are now interested in predicting which alarm box numbers (in these zipcodes which are the most likely to
# report false fire alarms more than the median number for the whole city) are most likely to report
# false fire alarms
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(
  -STARFIRE_INCIDENT_ID, -INCIDENT_DATETIME, -ALARM_BOX_BOROUGH, -ALARM_BOX_LOCATION,
  -INCIDENT_BOROUGH, -POLICEPRECINCT, -CITYCOUNCILDISTRICT, -COMMUNITYDISTRICT,
  -COMMUNITYSCHOOLDISTRICT, -CONGRESSIONALDISTRICT, -ALARM_SOURCE_DESCRIPTION_TX, -INCIDENT_CLASSIFICATION,
  -INCIDENT_CLASSIFICATION_GROUP, -DISPATCH_RESPONSE_SECONDS_QY, -FIRST_ASSIGNMENT_DATETIME, -FIRST_ACTIVATION_DATETIME,
  -FIRST_ON_SCENE_DATETIME, -INCIDENT_CLOSE_DATETIME, VALID_DISPATCH_RSPNS_TIME_INDC, -VALID_INCIDENT_RSPNS_TIME_INDC,
  -INCIDENT_RESPONSE_SECONDS_QY, -INCIDENT_TRAVEL_TM_SECONDS_QY, -ENGINES_ASSIGNED_QUANTITY, -LADDERS_ASSIGNED_QUANTITY,
  -OTHER_UNITS_ASSIGNED_QUANTITY
)
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(-ALARM_LEVEL_INDEX_DESCRIPTION)
most_false_alarms_by_zipcode <- most_false_alarms_by_zipcode %>% select(-HIGHEST_ALARM_LEVEL, -VALID_DISPATCH_RSPNS_TIME_INDC)

most_false_alarms_by_zipcode$incident_month <- month.name[most_false_alarms_by_zipcode$incident_month]
most_false_alarms_by_zipcode$incident_month <- as.factor(most_false_alarms_by_zipcode$incident_month)
most_false_alarms_by_zipcode$incident_hour <- as.factor(most_false_alarms_by_zipcode$incident_hour)
most_false_alarms_by_zipcode$incident_weekday <- as.factor(most_false_alarms_by_zipcode$incident_weekday)

most_false_alarms_by_zipcode$false_alarm <- as.factor(most_false_alarms_by_zipcode$false_alarm)
most_false_alarms_by_zipcode <- na.omit(most_false_alarms_by_zipcode)

most_false_alarms_by_zipcode$ZIPCODE <- as.numeric(as.character(most_false_alarms_by_zipcode$ZIPCODE))
most_false_alarms_by_zipcode <- left_join(most_false_alarms_by_zipcode, step1data, by = "ZIPCODE")

# dividing data into training, validation, and testing sets
train_indices <- sample(nrow(most_false_alarms_by_zipcode), 71170)
validation_indices <- sample(setdiff(1:nrow(most_false_alarms_by_zipcode), train_indices), 35585)
test_indices <- sample(setdiff(1:nrow(most_false_alarms_by_zipcode), c(train_indices, validation_indices)), 35584)
train <- most_false_alarms_by_zipcode[train_indices, ]
validate <- most_false_alarms_by_zipcode[validation_indices, ]
test <- most_false_alarms_by_zipcode[test_indices, ]

nrow(train)
nrow(validate)
nrow(test)

threshold <- 0.7

# Logistic Regression
weights <- ifelse(train$false_alarm == 0, 1, 137598 / 4741)
fit_glm <- glm(false_alarm ~ incident_weekday + incident_hour +
  incident_month + last_inspection_status_vio +
  days_from_median_cert_expiry_date + days_from_median_last_full_inspection_date +
  days_from_median_violation_date + proportion_approved, data = train, family = "binomial", weights = weights)
predicted_prob_glm <- as.numeric(predict(fit_glm, newdata = validate, type = "response"))
validate$predicted_prob_glm <- predicted_prob_glm
validate$prediction_glm <- ifelse(validate$predicted_prob_glm > threshold, 1, 0)
# confusion matrix
table(validate$prediction_glm, validate$false_alarm)
# accuracy
accuracy_glm <- (table(validate$prediction_glm, validate$false_alarm)[1, 1] + table(validate$prediction_glm, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarm)[1, 1] + table(validate$prediction_glm, validate$false_alarm)[2, 2] +
    table(validate$prediction_glm, validate$false_alarm)[2, 1] + table(validate$prediction_glm, validate$false_alarm)[1, 2])
# precision
precision_glm <- (table(validate$prediction_glm, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarm)[2, 2] + table(validate$prediction_glm, validate$false_alarm)[2, 1])
# recall
recall_glm <- (table(validate$prediction_glm, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_glm, validate$false_alarm)[2, 2] + table(validate$prediction_glm, validate$false_alarm)[1, 2])
# AUC
validate <- na.omit(validate)
pred <- prediction(validate$prediction_glm, validate$false_alarm)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_glm, "\n")
# calibration plot
validate$prediction_glm <- as.factor(validate$prediction_glm)
validate$false_alarm <- as.numeric(as.character(validate$false_alarm))
calPlotData <- calibration(validate$prediction_glm ~ validate$false_alarm)
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - Logistic Regression", xlab = "False fire alarm (no = 0 / yes = 100)",
  ylab = "Proportion of true fire alarms"
)

# KNN, Naive Baye's do not allow for weights to be applied to the outcomes. Thus, we will forgo it from our
# consideration

# Random Forest
train <- na.omit(train)
fit_rf <- ranger(
  false_alarm ~ incident_weekday + incident_hour +
    incident_month + last_inspection_status_vio +
    days_from_median_cert_expiry_date + days_from_median_last_full_inspection_date +
    days_from_median_violation_date + proportion_approved,
  data = train, num.trees = 100, probability = T,
  sample.fraction = c(.5, .5), keep.inbag = TRUE, class.weights = c(1, 10)
)
predicted_prob_rf <- predict(fit_rf, data = validate, type = "response")
validate$predicted_prob_rf <- predicted_prob_rf$predictions[, 2]
validate$prediction_rf <- ifelse(validate$predicted_prob_rf > threshold, 1, 0)

# confusion matrix
table(validate$prediction_rf, validate$false_alarm)

# accuracy
accuracy_rf <- (table(validate$prediction_rf, validate$false_alarm)[1, 1] + table(validate$prediction_rf, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarm)[1, 1] + table(validate$prediction_rf, validate$false_alarm)[2, 2] +
    table(validate$prediction_rf, validate$false_alarm)[2, 1] + table(validate$prediction_rf, validate$false_alarm)[1, 2])

# precision
precision_rf <- (table(validate$prediction_rf, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarm)[2, 2] + table(validate$prediction_rf, validate$false_alarm)[2, 1])

# recall
recall_rf <- (table(validate$prediction_rf, validate$false_alarm)[2, 2]) /
  (table(validate$prediction_rf, validate$false_alarm)[2, 2] + table(validate$prediction_rf, validate$false_alarm)[1, 2])

# AUC
pred <- prediction(validate$prediction_rf, validate$false_alarm)
perf <- performance(pred, "auc")
auc_rf <- perf@y.values[[1]]
cat("The AUC score on the validate set is", auc_rf, "\n")

# calibration plot
validate$prediction_rf <- as.factor(validate$prediction_rf)
calPlotData <- calibration(validate$prediction_rf ~ as.numeric(as.character(validate$false_alarm)))
xyplot(calPlotData,
  auto.key = list(columns = 2), main = "Calibration plot - Random Forest", xlab = "False fire alarm (no = 0 / yes = 100)",
  ylab = "Proportion of true fire alarms"
)

accuracy_glm
accuracy_rf

precision_glm
precision_rf

recall_glm
recall_rf

auc_glm
auc_rf

# best performance is using Logistic Regression Model: AUC = 0.646

# refitting model on Train and Validate using logistic regression
weights <- ifelse((rbind(train %>% select(-predictions), validate %>% select(-contains("pred"))))$false_alarm == 0, 1, 113544 / 3515)
fit_glm2 <- glm(
  false_alarm ~ incident_weekday + incident_hour +
    incident_month + last_inspection_status_vio +
    days_from_median_cert_expiry_date + days_from_median_last_full_inspection_date +
    days_from_median_violation_date + proportion_approved,
  data = rbind(train %>% select(-predictions), validate %>% select(-contains("pred"))),
  family = "binomial", weights = weights
)
predicted_prob_glm <- as.numeric(predict(fit_glm2, newdata = test, type = "response"))
test$predicted_prob_glm <- predicted_prob_glm
test$prediction_glm <- ifelse(test$predicted_prob_glm > threshold, 1, 0)
# AUC
test <- na.omit(test)
pred <- prediction(test$prediction_glm, test$false_alarm)
perf <- performance(pred, "auc")
auc_glm <- perf@y.values[[1]]
cat("The AUC score on the test set is", auc_glm, "\n")

# we now use this model to predict which alarm boxes are the most likely to report false fire alarms
most_false_alarms_by_zipcode$predictions <- predict(fit_glm2, most_false_alarms_by_zipcode)
most_false_alarms_by_alarm_box <- most_false_alarms_by_zipcode %>%
  arrange(desc(predictions)) %>%
  top_n(10)
most_false_alarms_by_alarm_box <- data.frame(zip = most_false_alarms_by_alarm_box$ZIPCODE, alarm_box = most_false_alarms_by_alarm_box$ALARM_BOX_NUMBER)
