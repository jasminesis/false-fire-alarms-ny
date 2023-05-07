library(plyr)
library(fuzzyjoin)
library(dplyr)
library(XML)
library(arrow)
library(tidyverse)
library(lubridate)
library(kableExtra)

# install.packages("remotes")
# remotes::install_github("hansthompson/rusps")
# library(rusps)

dispatch_2021_data <- read_parquet("../data/year=2021/part-0.parquet")
fitness_data <- read_csv("../data/fitness.csv")
summary_data <- read_csv("../data/summary.csv")
alarm_box_locations_data <- read_csv("../data/alarm_box_locations.csv")
inspections_data <- read_csv("../data/inspections.csv")
# Not used
rbis_data <- read_csv("../data/rbis.csv")
violation_data <- read_csv("../data/violation orders.csv")

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
    false_alarm = case_when(
      INCIDENT_CLASSIFICATION == "Alarm System - Defective" ~ 1,
      INCIDENT_CLASSIFICATION == "Alarm System - Unnecessary" ~ 1,
      .default = 0
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

plot(table(data2021$incident_hour))

fit1 <- glm(formula = false_alarm ~ 1, data = data2021, family = "binomial")
summary(fit1)
exp(coef(fit1))

# don't do glm on alarm box number
fit2 <- glm(formula = false_alarm ~ INCIDENT_BOROUGH, data = data2021, family = "binomial")
summary(fit2)
exp(coef(fit2))
