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
library(rusps)


# we want to find the probability of false fire alarms by ZIPCODE 
# (we tried to use BIN number but the dataset which has information about the 
# incidents for false fire alarms does not have inforamtion more exact than 
# zipcodes apart from alarm box numbers but we decided not to use alarm box numbers as 
# all the datasets that we wish to merge with data2021 do not have alarm box number information.
# We also could not find a way to effectively find the alarm box information from BIN numbers; which is
# present in the datasets that we wish to merge with data2021)

# reading in fitness, summary, alarm_box_locations, inspections, rbis, violation, data2021
data <- read_csv_arrow("dispatch.csv")
data <- data %>% mutate(
  year = year(mdy_hms(INCIDENT_DATETIME))
)
data %>% group_by(year) %>% write_dataset(path ="data/")
data2021 <- read_parquet("./data/year=2021/part-0.parquet")

fitness <- read.csv("fitness.csv")
summary <- read.csv("summary.csv")
alarm_box_locations <- read.csv("alarm_box_locations.csv")
inspections <- read.csv("inspections.csv")
rbis <- read.csv("rbis.csv")
violation <- read.csv("violation orders.csv")

data2021_temp <- data2021
fitness_temp <- fitness
summary_temp <- summary
alarm_box_locations_temp <- alarm_box_locations
inspections_temp <- inspections
rbis_temp <- rbis
violation_temp <- violation

# removing unnecessary columns from all datasets
fitness_temp <- fitness_temp %>% select(-COF_ID, -COF_NUM,
                                        -COF_TYPE, -HOLDER_NAME,
                                        -BIN, -COMMUNITY.BOARD,
                                        -COUNCIL.DISTRICT, -BBL,
                                        -LATITUDE, -LONGITUDE,
                                        -NUMBER, -STREET, 
                                        -CENSUS.TRACT, -NTA)
summary_temp <- summary_temp %>% select(-SUMMARY_ID, -SPRINKLER_TYPE,
                              -STANDPIPE_TYPE, -BIN, 
                              -BBL, 
                              -BLOCK, -LOT, -COMMUNITY.BOARD, 
                              -COUNCIL.DISTRICT)
alarm_box_locations_temp <- alarm_box_locations_temp %>% select(-BOX_TYPE,
                                                                -COMMUNITYDISTICT,
                                                                -CITYCOUNCIL,
                                                                -LATITUDE, -LONGITUDE, -Location.Point)

inspections_temp <- inspections_temp %>% select(-ACCT_ID, -ALPHA, -ACCT_NUM,
                                                -OWNER_NAME, -BIN, -COMMUNITY.BOARD, 
                                                -COUNCIL.DISTRICT, -BBL, 
                                                -LATITUDE, -LONGITUDE, -Number,
                                                -Street, -Census.Tract, -NTA)
rbis_temp <- rbis_temp %>% select(-INSPTN_OPS_DETAIL, -INSPECTING_UNIT_CODE,
                                  -BLDG_CURRENT_BIN_FK, -COMMUNITYDISTRICT, 
                                  -CITYCOUNCILDISTRICT, -BBL, 
                                  -Location.1)
violation_temp <- violation_temp %>% select(-VIO_ID, -ACCT_NUM, -ACCT_OWNER,
                                            -VIOLATION_NUM, -BIN, -COMMUNITY.BOARD,
                                            -COUNCIL.DISTRICT, -BBL,
                                            -LATITUDE, -LONGITUDE, -NUMBER,
                                            -STREET, -CENSUS.TRACT, -NTA)

# cleaning the 'borough' column of 'fitness'
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "10 GRAND ARMY PLZ"),]$BOROUGH <- "Brooklyn"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "1 WORLDS FAIR MARINA"),]$BOROUGH <- "Queens"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "21 WEST END AVE"),]$BOROUGH <- "Manhattan"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "30-30 THOMPSON AVE"),]$BOROUGH <- "Queens"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "254-14 UNION TNPK"),]$BOROUGH <- "Queens"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "17-19 HAZEN ST"),]$BOROUGH <- "Queens"
fitness_temp[(fitness_temp$BOROUGH == "") & (fitness_temp$PREM_ADDR == "186-16 UNION TNPK"),]$BOROUGH <- "Queens"

# the first address does not seem to be correct
# we remove this row
fitness_temp <- fitness_temp[-1,]

# recoding the boroughs
fitness_temp <- fitness_temp %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))

alarm_box_locations_temp <- alarm_box_locations_temp %>% rename('ZIPCODE' = 'ZIP')
alarm_box_locations_temp <- alarm_box_locations_temp %>% rename('ALARM_BOX_LOCATION' = 'LOCATION')
alarm_box_locations_temp$BOROBOX <- sub('.', '', alarm_box_locations_temp$BOROBOX)
alarm_box_locations_temp <- alarm_box_locations_temp %>% rename('ALARM_BOX_NUMBER' = 'BOROBOX')

data2021_temp <- data2021_temp %>% rename('ALARM_BOX_NUMBER' = 'ALARM_BOX_NUMBER.x')
data2021_temp <- data2021_temp %>% rename('ZIPCODE' = 'ZIPCODE.x') %>% select(-ALARM_BOX_NUMBER.y)

data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 3)] <- 
  paste0("0", data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 3)])

data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 2)] <- 
  paste0("00", data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 2)])

data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 1)] <- 
  paste0("000", data2021_temp$ALARM_BOX_NUMBER[which(nchar(data2021_temp$ALARM_BOX_NUMBER) == 1)])

left_join_keep_first_only <- function(x, y, by) {
  ## Our strategy is to use summarize/first on y for each non-match category,
  ## then join that to x.
  . <- NULL                           # silence notes on package check
  ll <- by
  names(ll) <- NULL
  y %>%
    dplyr::group_by_at(tidyselect::all_of(ll)) %>%
    dplyr::summarize_at(dplyr::vars(-tidyselect::any_of(ll)), first) %>%
    ungroup() %>%
    left_join(x, ., by=by)
}

# joining alarm_box_locations
data2021_temp$ALARM_BOX_NUMBER <- as.integer(data2021_temp$ALARM_BOX_NUMBER)
alarm_box_locations_temp$ALARM_BOX_NUMBER <- as.integer(alarm_box_locations_temp$ALARM_BOX_NUMBER)
data2021_temp <- left_join_keep_first_only(data2021_temp, alarm_box_locations_temp, by = 'ALARM_BOX_NUMBER')

############################ not sure why zipcodes which were NA
# in data2021 before were not replaced data in alarm_box_locations_temp
# ZIPCODES UNSUCCESSFULLY ADDED FROM alarm_box_locations (join by ALARM_BOX_NUMBER)

# joining fitness
############################ would be helpful if you could populate the zipcode
# missing data in the fitness dataset using this API 'https://tools.usps.com/zip-code-lookup.htm?byaddress'
# (we have all teh adresses in fitness but some associated zipcodes are missing)
fitness_temp <- fitness_temp %>% rename('ZIPCODE' = 'POSTCODE')
fitness_temp$EXPIRES_ON <- mdy(fitness_temp$EXPIRES_ON)
fitness_temp <- fitness_temp[which(year(fitness_temp$EXPIRES_ON) > 2020),]
date <- as.POSIXct(data2021_temp$INCIDENT_DATETIME, format = "%m/%d/%Y %I:%M:%S %p")
data2021_temp$INCIDENT_DATETIME <- format(date, "%m/%d/%Y")
data2021_temp$INCIDENT_DATETIME <- mdy(data2021_temp$INCIDENT_DATETIME)
data2021_temp <- data2021_temp %>% select(-ALARM_BOX_LOCATION.y, 
                                          -ZIPCODE.y) %>% rename('ZIPCODE' = 'ZIPCODE.x',
                                                                 'ALARM_BOX_LOCATION' = 'ALARM_BOX_LOCATION.x')
data2021_temp <- left_join_keep_first_only(data2021_temp, 
                          fitness_temp, by = 'ZIPCODE')
data2021_temp <- data2021_temp %>% select(-BOROUGH.y) %>% rename('BOROUGH' = 'BOROUGH.x')
# EXPIRES_ON ADDED SUCCESSFULLY FROM fitness (join by ZIPCODE)

# joining inspections
# renaming boroughs
inspections_temp <- inspections_temp %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))
inspections_temp <- inspections_temp %>% rename('ZIPCODE' = 'POSTCODE')
data2021_temp <- left_join_keep_first_only(data2021_temp, 
                                           inspections_temp, by = 'ZIPCODE')
data2021_temp <- data2021_temp %>% select(-BOROUGH.y, -PREM_ADDR.y) %>% 
  rename('BOROUGH' = 'BOROUGH.x', 'PREM_ADDR' = 'PREM_ADDR.x')
# LAST_VISIT_DT, LAST_FULL_INSP_DT, LAST_INSP_STAT ADDED SUCCESSFULLY FROM inspections
# (join by ZIPCODE)

# joining summary
# recoding the boroughs for 'summary'
summary_temp <- summary_temp %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))
names(summary_temp)
############################ not sure how to add this to data2021 since
# there are no zipcodes in this dataset (there are latitudes and langitudes
# but I don't know how to convert them to zipcodes --- seems to be associated
# with Google API but not sure how to use it!)
# NUM_SIAM_SPRINKLER, NUM_SIAM_STANDPIPE, NUM_OF_VIOLATION_NOTICES,
# NUM_OF_VIOLATION_ORDER UNSUCCESSFULLY ADDED FROM summary

# joining rbis
# recoding the boroughs for 'rbis'
rbis <- rbis %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))
names(rbis_temp)
############################ not sure how to add this to data2021 since
# there are no zipcodes in this dataset (there are latitudes and langitudes
# but I don't know how to convert them to zipcodes --- seems to be associated
# with Google API but not sure how to use it!)
# INSPTN_TYP_CD, INSP_INSPECT_DT UNSUCCESSFULLY ADDED FROM rbis (join by zipcode)

# joining violation
violation_temp <- violation_temp %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))
violation_temp <- violation_temp %>% rename('ZIPCODE' = 'POSTCODE')
data2021_temp <- left_join_keep_first_only(data2021_temp, 
                                           violation_temp, by = 'ZIPCODE')
data2021_temp <- data2021_temp %>% select(-BOROUGH.y, -PREM_ADDR.y) %>% 
  rename('BOROUGH' = 'BOROUGH.x', 'PREM_ADDR' = 'PREM_ADDR.x')
# VIO_LAW_NUM, VIO_LAW_DESC, VIO_DATE, ACTION SUCCESSFULLY ADDED FROM violation (join by zipcode)

data2021_temp <- data2021_temp %>% select(-POLICEPRECINCT, -CITYCOUNCILDISTRICT, -COMMUNITYDISTRICT,
                                        -COMMUNITYSCHOOLDISTRICT, -CONGRESSIONALDISTRICT)

# creating column for the number of days between last inspection and incident date
data2021_temp$LAST_FULL_INSP_DT <- mdy(data2021_temp$LAST_FULL_INSP_DT)
data2021_temp <- data2021_temp %>% mutate(days_after_last_inspection = 
                                            data2021_temp$INCIDENT_DATETIME - data2021_temp$LAST_FULL_INSP_DT)
data2021_temp <- data2021_temp %>% select(-STARFIRE_INCIDENT_ID, -ALARM_BOX_BOROUGH, 
                                          -ALARM_BOX_LOCATION, -INCIDENT_BOROUGH)

data2021_temp$INCIDENT_CLASSIFICATION <- as.factor(data2021_temp$INCIDENT_CLASSIFICATION)

data2021_temp2 <- data2021_temp
levels(data2021_temp2$INCIDENT_CLASSIFICATION)[c(2, 4)] <- 1
levels(data2021_temp2$INCIDENT_CLASSIFICATION)[-2] <- 0
names(data2021_temp2)

data2021_temp2$ALARM_LEVEL_INDEX_DESCRIPTION <- as.factor(data2021_temp2$ALARM_LEVEL_INDEX_DESCRIPTION)
data2021_temp2$ALARM_BOX_NUMBER <- as.factor(data2021_temp2$ALARM_BOX_NUMBER)
data2021_temp2$ZIPCODE <- as.factor(data2021_temp2$ZIPCODE)
data2021_temp2$ALARM_SOURCE_DESCRIPTION_TX <- as.factor(data2021_temp2$ALARM_SOURCE_DESCRIPTION_TX)
data2021_temp2$HIGHEST_ALARM_LEVEL <- as.factor(data2021_temp2$HIGHEST_ALARM_LEVEL)

data2021_temp2$INCIDENT_CLASSIFICATION_GROUP

glm(INCIDENT_CLASSIFICATION ~ )


sapply(X = false_alarms, FUN = function(x) sum(is.na(x)))



