fitness <- read.csv("fitness.csv")
summary <- read.csv("summary.csv")
# main dataset
# main <- read.csv("main.csv")

library(plyr)
library(fuzzyjoin)

# 1. point of concern: there are 2 ways in which a false alarm can be 
# sounded - human 'mischief' and defective alarm system. So should we
# consider only the machine error false alarms (as it may not make sense
# to predict human 'mischief')

library(dplyr)
library(rusps)
library(XML)

# cleaning the 'bin' column of 'fitness'
sum(fitness$BIN == "0")
rows <- which(fitness$PREM_ADDR == "10 GRAND ARMY PLZ")
fitness[rows,]$BIN <- "3029665"
unique(fitness[which(fitness$BIN == "0"),]$PREM_ADDR)
rows2 <- which(fitness$PREM_ADDR == "141 W 156 ST")
fitness[rows2,]$BIN <- NA
rows3 <- which(fitness$PREM_ADDR == "30-20 THOMPSON AVE")
fitness[rows3,]$BIN <- "4003517"
rows4 <- which(fitness$PREM_ADDR == "1 WORLDS FAIR MARINA")
fitness[rows4,]$BIN <- "4541458"
rows5 <- which(fitness$PREM_ADDR == "21 WEST END AVE")
fitness[rows5,]$BIN <- "1088870"
rows6 <- which(fitness$PREM_ADDR == "30-30 THOMPSON AVE")
fitness[rows6,]$BIN <- "4003533"
rows7 <- which(fitness$PREM_ADDR == "254-14 UNION TNPK")
fitness[rows7,]$BIN <- "4539986"
rows8 <- which(fitness$PREM_ADDR == "17-19 HAZEN ST")
fitness[rows8,]$BIN <- "2118476"
rows9 <- which(fitness$PREM_ADDR == "186-16 UNION TNPK")
fitness[rows9,]$BIN <- "4156125"
rows10 <- which(fitness$PREM_ADDR == "188-10 UNION TNPK")
fitness[rows10,]$BIN <- "4156312"
rows11 <- which(fitness$PREM_ADDR == "82 W 225 ST")
fitness[rows11,]$BIN <- "1064698"
# right now, is 1 NA in 'bin'

# cleaning the 'borough' column of 'fitness'
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "10 GRAND ARMY PLZ"),]$BOROUGH <- "Brooklyn"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "30-20 THOMPSON AVE"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "1 WORLDS FAIR MARINA"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "21 WEST END AVE"),]$BOROUGH <- "Manhattan"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "30-30 THOMPSON AVE"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "254-14 UNION TNPK"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "17-19 HAZEN ST"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "186-16 UNION TNPK"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "188-10 UNION TNPK"),]$BOROUGH <- "Queens"
fitness[(fitness$BOROUGH == "") & (fitness$PREM_ADDR == "82 W 225 ST"),]$BOROUGH <- "Bronx"

# aim - to merge using borough, postcodes and BIN
# cannot use latitude and longitude as the coordinates would be too close to each other - difficult to parse as well

# removing unnecessary columns
fitness <- fitness %>% select(-COMMUNITY.BOARD, -COUNCIL.DISTRICT, -BBL, 
                              -LATITUDE, -LONGITUDE, -CENSUS.TRACT, -NTA, -COF_ID, -COF_NUM)
# since there are many NA values in the 'postcodes' column,
# we join fitness and summary using bin number only - we will
# add postcodes with the help of other datasets that we join
fitness <- fitness %>% select(-POSTCODE)
# the first address does not seem to be correct
# we remove this row
fitness <- fitness[-1,]

# recoding the boroughs
fitness <- fitness %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))

# recoding the boroughs for 'summary'
summary <- summary %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))

# removing unnecessary columns
summary <- summary %>% select(-BLOCK, -LOT, -COMMUNITY.BOARD, -COUNCIL.DISTRICT,
                              -BBL, 
                              -LATITUDE, -LONGITUDE)

# joining the 2 datasets
fitness$BIN <- as.numeric(fitness$BIN)
joined <- rbind.fill(fitness, summary)

# predictors that we can use: expires on, borough, prem address (?), number of violation notices, number of violation orders (from 'joined' dataset)
# predictors that we can use: violation dates (from 'violations' dataset)

# working with 'violation' dataset
violation <- read.csv("violation orders.csv")
rows1 <- which(violation$PREM_ADDR == "1 SO FERRY STATION")
violation[rows1,]$BIN <- "1000027"
rows2 <- which(violation$PREM_ADDR == "21 WEST END AVE")
violation[rows2,]$BIN <- "1088870"
rows3 <- which(violation$PREM_ADDR == "343 COURT ST")
violation[rows3,]$BIN <- "3007004"
rows3 <- which(violation$PREM_ADDR == "17-19 HAZEN ST")
violation[rows3,]$BIN <- "2118476"
violation[which(violation$PREM_ADDR == "301 SHORE RD"), ]$PREM_ADDR <- "30-1 SHORE RD"
row4 <- which(violation$PREM_ADDR == "30-1 SHORE RD")
violation[rows4,]$BIN <- "4168098"
rows5 <- which(violation$PREM_ADDR == "2323 ADAM CLAYTON BLVD")
violation[rows5,]$BIN <- "1058309"
rows6 <- which(violation$PREM_ADDR == "1 BEACH 116 ST STAT")
violation[rows6,]$BIN <- "4303939"
rows7 <- which(violation$PREM_ADDR == "1103 E GUNHILL RD")
violation[rows7,]$BIN <- "2059733"
rows8 <- which(violation$PREM_ADDR == "1610 TILDEN AVE")
violation[rows8,]$BIN <- NA
rows9 <- which(violation$PREM_ADDR == "2013 ADAM CLAYTON BLVD")
violation[rows9,]$BIN <- "1057660"
rows10 <- which(violation$PREM_ADDR == "1213-43 E 15 ST")
violation[rows10,]$BIN <- "3180674"
rows11 <- which(violation$PREM_ADDR == "30-30 THOMPSON AVE")
violation[rows11,]$BIN <- "4003533"
rows12 <- which(violation$PREM_ADDR == "30 PERIMETER RD")
violation[rows12,]$BIN <- NA
rows13 <- which(violation$PREM_ADDR == "1 CHELSEA PIERS")
violation[rows13,]$BIN <- "1000000"
rows14 <- which(violation$PREM_ADDR == "3 CANAL ST STATION")
violation[rows14,]$BIN <- NA
rows15 <- which(violation$PREM_ADDR == "188-02 UNION TNPK")
violation[rows15,]$BIN <- "4156312"
rows16 <- which(violation$PREM_ADDR == "1 PIER 36 EMS BLDG")
violation[rows16,]$BIN <- "1079601"
rows17 <- which(violation$PREM_ADDR == "1 BEACH 67 ST STATION")
violation[rows17,]$BIN <- "4818796" # 211-213 beach 67 st
rows18 <- which(violation$PREM_ADDR == "118-23 GUY BREWER BLVD")
violation[rows18,]$BIN <- "4529903" # 116-30 guy brewer blvd
rows19 <- which(violation$PREM_ADDR == "1 BEACH 60 ST STATION")
violation[rows19,]$BIN <- "4818388" # 217 beach 60 st
rows20 <- which(violation$PREM_ADDR == "98 W 225 ST")
violation[rows20,]$BIN <- "1064698"
rows21 <- which(violation$PREM_ADDR == "94-45 GUY BREWER BLVD")
violation[rows21,]$BIN <- "4529903" # 116-30 guy brewer blvd
rows22 <- which(violation$PREM_ADDR == "129 ODELL CLARK PL")
violation[rows22,]$BIN <- "1060023"
rows23 <- which(violation$PREM_ADDR == "31-40 THOMPSON AVE")
violation[rows23,]$BIN <- "4003535"
rows24 <- which(violation$PREM_ADDR == "52-10 CENTER BLVD")
violation[rows24,]$BIN <- "4542932"
rows25 <- which(violation$PREM_ADDR == "338 STORY RD")
violation[rows25,]$BIN <- NA

# renaming boroughs for 'violation'
violation <- violation %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))


# removing unnecessary columns
violation <- violation %>% select(-VIO_ID, -ACCT_NUM, -ACCT_OWNER, -VIO_LAW_NUM, -COMMUNITY.BOARD, 
                                  -COUNCIL.DISTRICT, -BBL, -LATITUDE, -LONGITUDE, -POSTCODE, -CENSUS.TRACT, -NTA,
                                  -VIOLATION_NUM)

# reading in 'causes' dataset
causes <- read.csv("causes.csv")

# reading in 'inspections' dataset
inspections <- read.csv("inspections.csv")
# renaming boroughs
inspections <- inspections %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))

causes <- causes %>% rename('BOROUGH' = 'Borough')

joined <- rbind.fill(joined, inspections)
joined <- rbind.fill(joined, causes)


joined <- joined %>% select(-COF_TYPE, -SUMMARY_ID, -ACCT_ID, -ACCT_NUM, -ALPHA, -COMMUNITY.BOARD, 
                            -COUNCIL.DISTRICT, -BBL, -LONGITUDE,
                            -LATITUDE, -POSTCODE, -Census.Tract, -NTA, -COF_TYPE, -Battalion, -Community_District,
                            -Precinct)
joined <- joined[-which(duplicated(joined)),]

library(lubridate)

joined2 <- joined
joined2$LAST_FULL_INSP_DT <- mdy(joined2$LAST_FULL_INSP_DT)
joined2$LAST_VISIT_DT <- mdy(joined2$LAST_VISIT_DT)
joined2$Incident_DateTime <- mdy_hms(joined2$Incident_DateTime)
joined2$Incident_DateTime <- format(joined2$Incident_DateTime, "%m/%d/%Y")

joined2 <- joined2 %>% select(-Number, -NUMBER, -Street, -STREET, -OWNER_NAME, -HOLDER_NAME, -Case_Year)

View(joined2)







name(joined2)
sum(joined2$LAST_FULL_INSP_DT > joined2$Incident_DateTime, na.rm = T)
sum(is.na(joined2$LAST_INSP_STAT))
table(joined2$LAST_INSP_STAT)

View(joined2[which(joined2$LAST_INSP_STAT == ""),])


not_approvals <- joined2 %>% filter(grepl('NOT APPROVAL(VIO)', LAST_INSP_STAT))




range(joined)






rbis <- read.csv("rbis.csv")
rbis <- rbis %>%
  mutate(across(where(is.character), 
                ~ if_else(. == "MN", "Manhattan", 
                          if_else(. == "BK", "Brooklyn",
                                  if_else(. == "SI", "Staten Island",
                                          if_else(. == "BX", "Bronx",
                                                  if_else(. == "QN", "Queens", .)))))))
full_join(joined, rbis)

View(rbis)
