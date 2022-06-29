library(data.table)
library(rio)
library(lubridate)
#setwd("Z:/Test")

df <- file.info(list.files("Z:/Speedwell_ERA5_FTP_Daily_Import/Daily Updates", full.names = T))
windfile <- rownames(df)[which.max(df$mtime)]

#windspeed.load <- read.csv("Z:/Speedwell_ERA5_FTP_Daily_Import/Daily Updates/Australia_HOURLY_100M_WIND_2021-04-15.csv", header=TRUE)

windspeed.load <- read.csv(windfile, header=TRUE)
windspeed.load <- as.data.table(windspeed.load)
windspeed.load <- windspeed.load[,.(SRC_ID,DATE,HOUR,HOURLY_WIND100M_SPEED)]
WS <- dcast(windspeed.load, DATE+HOUR ~ SRC_ID)
WS <- as.data.table(WS)
old <- c("DATE","HOUR","GR100_-37.250x143.750","GR100_-37.750x142.750",
         "GR100_-38.250x142.000","GR101_-38.000x142.250")
new <- c("Date","Hour","Loc1","Loc2","Loc3","Loc4")
WS <- setnames(WS,old,new)
WS$Loc1 <- as.numeric(WS$Loc1)
WS$Loc2 <- as.numeric(WS$Loc2)
WS$Loc3 <- as.numeric(WS$Loc3)
WS$Loc4 <- as.numeric(WS$Loc4)
WS[, AverageWS := (Loc1+Loc2+Loc3+Loc4)/4]
WS[, Date := ymd(WS[["Date"]])]

write.csv(WS,"D:/Users/jason.west/Clean Energy Transfer Fund Pty Ltd/Rob Ashdown - Revenue Gx Model/Active Strategy/SpeedwellLatest.csv")

