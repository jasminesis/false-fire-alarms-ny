library(arrow)
library(tidyverse)

data <- read_csv_arrow("../data/Fire_Incident_Dispatch_Data.csv")
data <- data %>% mutate(
  year = year(mdy_hms(INCIDENT_DATETIME))
)

data_2021 <- data %>% filter(year == 2021)
data_2020 <- data %>% filter(year == 2020)
data_2019 <- data %>% filter(year == 2019)

data$INCIDENT_DATETIME[589848]
dt <-"01/01/2005 04:05:20 AM"
year(mdy_hms(dt))


