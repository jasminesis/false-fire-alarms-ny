library(arrow)
library(tidyverse)
library(lubridate)
library(kableExtra)

data <- read_csv_arrow("./data/Fire_Incident_Dispatch_Data.csv")
data <- data %>% mutate(
  year = year(mdy_hms(INCIDENT_DATETIME))
)
data %>%
  group_by(year) %>%
  write_dataset(path = "../data/")

data2021 <- read_parquet("../data/year=2021/part-0.parquet")
t(head(data2021)) %>%
  kbl() %>%
  kable_minimal() %>%
  save_kable("../figures/head.png")
