#setwd('D:/Users/jason.west/Clean Energy Transfer Fund Pty Ltd/Rob Ashdown - Revenue Gx Model/Active Strategy/Dashboard')

##############################################################################
##############################################################################
# 
# tryCatch(
#   library(odbc),
#   error = function(e) {
#     install.packages("odbc", repos = "http://cran.rstudio.com/")
#     library(odbc)
#   }
# )
# 
# tryCatch(
#   library(dplyr),
#   error = function(e) {
#     install.packages("dplyr", repos = "http://cran.rstudio.com/")
#     library(dplyr)
#   }
# )
# 
# tryCatch(
#   library(dbplyr),
#   error = function(e) {
#     install.packages("dbplyr", repos = "http://cran.rstudio.com/")
#     library(dbplyr)
#   }
# )
# 
# tryCatch(
#   library(sqldf),
#   error = function(e) {
#     install.packages("sqldf", repos = "http://cran.rstudio.com/")
#     library(sqldf)
#   }
# )
# 
# tryCatch(
#   library(lubridate),
#   error = function(e) {
#     install.packages("lubridate", repos = "http://cran.rstudio.com/")
#     library(lubridate)
#   }
# )
# 
# tryCatch(
#   library(tidyverse),
#   error = function(e) {
#     install.packages("tidyverse", repos = "http://cran.rstudio.com/")
#     library(tidyverse)
#   }
# )
# 
# tryCatch(
#   library(rio),
#   error = function(e) {
#     install.packages("rio", repos = "http://cran.rstudio.com/")
#     library(rio)
#   }
# )
# 
# tryCatch(
#   library(reshape2),
#   error = function(e) {
#     install.packages("reshape2", repos = "http://cran.rstudio.com/")
#     library(reshape2)
#   }
# )
# 
# tryCatch(
#   library(data.table),
#   error = function(e) {
#     install.packages("data.table", repos = "http://cran.rstudio.com/")
#     library(data.table)
#   }
# )

############################################################
############################################################
## Packages listed in case already loaded
library(odbc)
library(dplyr)
library(dbplyr)
library(sqldf)
library(tidyverse)
library(rio)
#library(reshape2)
library(lubridate)
library(data.table)
##############################################################################
##############################################################################

# set this as the date from which MMS data is used to compute PnL
last.settle<-'2022-04-01 10:00:00'

# Connection code....enter password if prompted
dwConnect <- function() {
  con <- DBI::dbConnect(
    odbc::odbc(),
    Driver   = "SQL Server",
    Server   = "172.17.133.92",
    Database = "aemosql",
    UID      = "aemosqluser",
    PWD      =  "LVtsFxCup17Ckk7",
    #rstudioapi::askForPassword("aemosqluser"), #pw = LVtsFxCup17Ckk7
    Port     = 1433
  )
  return(con)
}


##############################################################################
##############################################################################

windsp <- function() {
  
  df <- file.info(list.files("//172.17.135.233/share/Speedwell_ERA5_FTP_Daily_Import/Daily Updates", full.names = T))
  windfile <- rownames(df)[which.max(df$mtime)]
  
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
  WS$newdate<-as.POSIXct(WS$Date, tz="UTC", format = "%Y-%m-%d %H:%M:%S")
  #WS$hr<-as.POSIXct(WS$Hour, origin = "1960-01-01", format = "%Y-%m-%d", tz="UTC")
  WS$newdate<-WS$newdate-3600*10 # reset times
  WS$newdate1 <- WS$newdate+WS$Hour*3600+14*3600
  WS.trimmed<-WS[,.(newdate1,AverageWS)]
  WS.trimmed <- setnames(WS.trimmed, c("date_time","WindSpeed"))
  WS.trimmed<-na.omit(WS.trimmed)
  
  windspeed.hr<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/windspeed.csv')
  windspeed.hr <- as.data.table(windspeed.hr)
  windspeed.hr[,(1):=NULL] # delete first column
  windspeed.hr[, datetime := ymd_hms(windspeed.hr[["date_time"]])]#,tz = "Australia/Brisbane")] # set date format
  windspeed.hr[, date_time := NULL]
  windspeed.hr<-windspeed.hr[!duplicated(windspeed.hr), ]
  old <- c("WindSpeed","datetime")
  new <- c("WindSpeed","date_time")
  windspeed.hr <- setnames(windspeed.hr,old,new)
  setcolorder(windspeed.hr, c("date_time", "WindSpeed"))
  WS.full<-merge.data.table(windspeed.hr,WS.trimmed, all=TRUE)
  WS.full<-WS.full[!duplicated(WS.full), ]
  WS.final<-unique(WS.full, by = "date_time")
  write.csv(WS.final,'//172.17.135.233/share/Dashboards/PowerBI/windspeed.csv')
  return(WS.final)
}


###################################################################
###################################################################



##############################################################################
##############################################################################


CETFVicWind <- function() {
  connDB<-dwConnect()
  WF <-
    dbGetQuery(
      connDB,
      "SELECT * FROM dbo.DISPATCH_UNIT_SCADA WHERE (SETTLEMENTDATE > '2021-03-31 23:30:00')
                        AND (DUID='CHALLHWF' OR DUID='CROWLWF1' OR DUID='MACARTH1'
                        OR DUID='OAKLAND1' OR DUID='PORTWF' OR DUID='WAUBRAWF' OR DUID='YAMBUKWF'
                        OR DUID='YSWF1') "
    )
  WF <- as.data.table(WF)
  WF[, date_time := ymd_hms(WF[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
  WF[, SETTLEMENTDATE := NULL]
  setcolorder(WF, c("date_time", "DUID", "SCADAVALUE"))
  WF_temp <- dcast(WF, date_time ~ DUID)
  DT <- WF_temp[order(WF_temp$date_time), ]
  
  ######################################################################
  ### Compute half-hour aggregations for SCADA data
  
  cuts <- seq(round(min(DT$date_time), "hours"), max(DT$date_time)+30*60, "30 min")

  # # CHALLHWF CROWLWF1 MACARTH1 OAKLAND1 PORTWF WAUBRAWF YAMBUKWF YSWF1
  CROWLWF1.hh <- aggregate(DT$CROWLWF1, list(cut(DT$date_time, cuts)), mean)
  CHALLHWF.hh <- aggregate(DT$CHALLHWF, list(cut(DT$date_time, cuts)), mean)
  MACARTH1.hh <- aggregate(DT$MACARTH1, list(cut(DT$date_time, cuts)), mean)
  OAKLAND1.hh <- aggregate(DT$OAKLAND1, list(cut(DT$date_time, cuts)), mean)
  PORTWF.hh <- aggregate(DT$PORTWF, list(cut(DT$date_time, cuts)), mean)
  WAUBRAWF.hh <- aggregate(DT$WAUBRAWF, list(cut(DT$date_time, cuts)), mean)
  YAMBUKWF.hh <- aggregate(DT$YAMBUKWF, list(cut(DT$date_time, cuts)), mean)
  YSWF1.hh <- aggregate(DT$YSWF1, list(cut(DT$date_time, cuts)), mean)
  
  CROWLWF1.hh <- as.data.table(CROWLWF1.hh)
  CHALLHWF.hh <- as.data.table(CHALLHWF.hh)
  MACARTH1.hh <- as.data.table(MACARTH1.hh)
  OAKLAND1.hh <- as.data.table(OAKLAND1.hh)
  PORTWF.hh <- as.data.table(PORTWF.hh)
  WAUBRAWF.hh <- as.data.table(WAUBRAWF.hh)
  YAMBUKWF.hh <- as.data.table(YAMBUKWF.hh)
  YSWF1.hh <- as.data.table(YSWF1.hh)
  
  CROWLWF1.hh[, date_time := ymd_hms(CROWLWF1.hh[["Group.1"]])]
  CHALLHWF.hh[, date_time := ymd_hms(CHALLHWF.hh[["Group.1"]])]
  MACARTH1.hh[, date_time := ymd_hms(MACARTH1.hh[["Group.1"]])]
  OAKLAND1.hh[, date_time := ymd_hms(OAKLAND1.hh[["Group.1"]])]
  PORTWF.hh[, date_time := ymd_hms(PORTWF.hh[["Group.1"]])]
  WAUBRAWF.hh[, date_time := ymd_hms(WAUBRAWF.hh[["Group.1"]])]
  YAMBUKWF.hh[, date_time := ymd_hms(YAMBUKWF.hh[["Group.1"]])]
  YSWF1.hh[, date_time := ymd_hms(YSWF1.hh[["Group.1"]])]
  
  DTT <- cbind(CROWLWF1.hh$date_time,CROWLWF1.hh[,2], CHALLHWF.hh[,2], MACARTH1.hh[,2],
               OAKLAND1.hh[,2], PORTWF.hh[,2], WAUBRAWF.hh[,2], YAMBUKWF.hh[,2], YSWF1.hh[,2])
  DTT <- as.data.table(DTT)
  DTT <- setnames(DTT, c("date_time","CROWLWF1","CHALLHWF","MACARTH1","OAKLAND1","PORTWF","WAUBRAWF","YAMBUKWF","YSWF1"))
  
  return(DTT)
}

##############################################################################
##############################################################################

VicPrices <- function() {
  connDB<-dwConnect()
  # Regional reference price
  pricedat_raw <-
    dbGetQuery(
      connDB,
      "SELECT SETTLEMENTDATE, RRP FROM dbo.TRADINGPRICE WHERE (SETTLEMENTDATE > '2021-03-31 23:30:00')
                           AND (REGIONID='VIC1')"
    )
  pricedat_raw <- as.data.table(pricedat_raw)
  pricedat_raw[, date_time := ymd_hms(pricedat_raw[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
  pricedat_raw[, SETTLEMENTDATE := NULL]
  setcolorder(pricedat_raw, c("date_time", "RRP"))
  pricedat_raw <-
    setNames(pricedat_raw, c("date_time", "Regions_VIC_Price"))
  pricedat <- pricedat_raw[order(pricedat_raw$date_time),]
  return(pricedat)
}


#############################################################################
#############################################################################

PriceVol <- function() {
  # Get WF data from CETF assets
  DTT <- CETFVicWind()
  # Vic prices
  pricedat<-VicPrices()
  DT.all <- merge(pricedat, DTT, by = "date_time")
  
  DT.all$CROWLWF1 <- nafill(DT.all$CROWLWF1, "const", fill = 0)
  DT.all$CHALLHWF <- nafill(DT.all$CHALLHWF, "const", fill = 0)
  DT.all$MACARTH1 <- nafill(DT.all$MACARTH1, "const", fill = 0)
  DT.all$OAKLAND1 <- nafill(DT.all$OAKLAND1, "const", fill = 0)
  DT.all$PORTWF <- nafill(DT.all$PORTWF, "const", fill = 0)
  DT.all$WAUBRAWF <- nafill(DT.all$WAUBRAWF, "const", fill = 0)
  DT.all$YAMBUKWF <- nafill(DT.all$YAMBUKWF, "const", fill = 0)
  DT.all$YSWF1 <- nafill(DT.all$YSWF1, "const", fill = 0)
  
  #MLF	
  #Oakland	1.0063
  #Macarthur	0.9757
  #Challicum	0.9711
  #Crowlands	0.9026
  #Portland	0.991
  #Waubra	0.9228
  #Yambuk	1.0079
  #Yaloak	0.9711
  
  # CETF Portfolio
  DT.all[, Act_Oakland := 1.0063 * 0.881 * OAKLAND1/2]
  DT.all[, Act_Macarthur := 0.9757 * 0.119 * MACARTH1/2]
  DT.all[, Act_Challicum := 0.9711 * 0.29 * CHALLHWF/2]
  DT.all[, Act_Crowlands := 0.9026 * 0.30 * CROWLWF1/2]
  DT.all[, Act_Portland := 0.991 * 0.11 * PORTWF/2]
  DT.all[, Act_Yambuk := 1.0079 * 0.83 * YAMBUKWF/2]
  DT.all[, Act_Yaloak := 0.9711 * 0.40 * YSWF1/2]
  DT.all[, Act_Waubra := 0.9228 * 0.26 * WAUBRAWF/2]
  DT.all[, Act_AGL := Act_Oakland + Act_Macarthur]
  DT.all[, Act_Acciona := Act_Waubra]
  DT.all[, Act_PacHydro := Act_Challicum + Act_Crowlands + Act_Portland +
           Act_Yambuk + Act_Yaloak]
  DT.all[, Act_CETF := Act_Oakland+Act_Macarthur+Act_Challicum+Act_Crowlands+Act_Portland+
           Act_Yambuk+Act_Yaloak+Act_Waubra]
  
  # Start calculations
  DT.all[,PPA_Mac:=(Regions_VIC_Price-46.3)*Act_Macarthur]
  DT.all[,PPA_Oak:=(Regions_VIC_Price-46.3)*Act_Oakland]
  DT.all[,PPA_Chall:=(Regions_VIC_Price-43.75)*Act_Challicum]
  DT.all[,PPA_Crow:=(Regions_VIC_Price-43.75)*Act_Crowlands]
  DT.all[,PPA_Port:=(Regions_VIC_Price-43.75)*Act_Portland]
  DT.all[,PPA_Yamb:=(Regions_VIC_Price-43.75)*Act_Yambuk]
  DT.all[,PPA_Yal:=(Regions_VIC_Price-43.75)*Act_Yaloak]
  # seperate columns for Waubra calcs
  DT.all[,Waub_Collar:=ifelse(Regions_VIC_Price*0.898>=405,405,
                              ifelse(Regions_VIC_Price*0.898<=37.5,37.5,Regions_VIC_Price*0.898))]
  DT.all[,Waub_RawP:=(44.5+Waub_Collar)/2]
  DT.all[,PPA_Waub:=(ifelse(Regions_VIC_Price<0,0,Regions_VIC_Price)-Waub_RawP)*Act_Waubra]
  #PPA by counterpart
  DT.all[,PPA_AGL_Act:=PPA_Mac+PPA_Oak]
  DT.all[,AGL_Inflow:=Regions_VIC_Price*Act_AGL]
  DT.all[,AGL_Outflow:=46.32*Act_AGL]
  DT.all[,PPA_Acciona_Act:=PPA_Waub]
  DT.all[,Acciona_Inflow:=Regions_VIC_Price*Act_Waubra]
  DT.all[,Acciona_Outflow:=44.5*Act_Waubra]
  DT.all[,PPA_PH_Act:=PPA_Chall+PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal]
  DT.all[,PH_Inflow:=Regions_VIC_Price*Act_PacHydro]
  DT.all[,PH_Outflow:=43.75*Act_PacHydro]
  DT.all[,CETF_250_Inflow:=AGL_Inflow+Acciona_Inflow+PH_Inflow]
  DT.all[,CETF_250_Outflow:=AGL_Outflow+Acciona_Outflow+PH_Outflow]
  
  DT.all$grouped_time = lubridate::ceiling_date(DT.all$date_time, unit = "hour")
  
  DT = DT.all %>%
    group_by(grouped_time) %>%
    summarise(Regions_VIC_Price = mean(Regions_VIC_Price),
              PPA_Chall = sum(PPA_Chall),
              PPA_Crow = sum(PPA_Crow),
              PPA_Mac = sum(PPA_Mac),
              PPA_Oak = sum(PPA_Oak),
              PPA_Port = sum(PPA_Port),
              PPA_Waub = sum(PPA_Waub),
              PPA_Yamb = sum(PPA_Yamb),
              PPA_Yal = sum(PPA_Yal),
              Act_Challicum = sum(Act_Challicum),
              Act_Crowlands = sum(Act_Crowlands),
              Act_Macarthur = sum(Act_Macarthur),
              Act_Oakland = sum(Act_Oakland),
              Act_Portland = sum(Act_Portland),
              Act_Waubra = sum(Act_Waubra),
              Act_Yambuk = sum(Act_Yambuk),
              Act_Yaloak = sum(Act_Yaloak),
              Act_AGL = sum(Act_AGL),
              Act_Acciona = sum(Act_Acciona),
              Act_PacHydro = sum(Act_PacHydro),
              Act_CETF = sum(Act_CETF),
              AGL_Inflow = sum(AGL_Inflow),
              AGL_Outflow = sum(AGL_Outflow),
              Acciona_Inflow = sum(Acciona_Inflow),
              PH_Inflow = sum(PH_Inflow),
              PH_Outflow = sum(PH_Outflow),
              CETF_250_Inflow = sum(CETF_250_Inflow),
              CETF_250_Outflow = sum(CETF_250_Outflow))
  
  # Bring windspeed data into the data table
  DT<-as.data.table(DT)
  DT[, date_time := ymd_hms(DT[["grouped_time"]])]#,tz = "Australia/Brisbane")] # set date format
  DT[, grouped_time := NULL]
  WindData<-windsp()
  DT.full <- merge(DT, WindData, by = "date_time", all = TRUE)
  # compute assumed windspeeds for missing data
  DT.full[,WindSpeed:=ifelse(is.na(WindSpeed),(Act_CETF+90)/25.5,WindSpeed)]
  
  # assign time periods to data
  DT.full[, Day := day(date_time)]
  DT.full[, Mth := month(date_time)]
  DT.full[, Year := year(date_time)]
  DT.full[, Hour := hour(date_time)]
  DT.full[, Week := week(date_time)]
  #DT.full[, Quart := quarter(date_time,with_year=TRUE,fiscal_start = 1)]
  
  
  # Blunt Hedge payoffs
  layer1 <- 100
  layer2 <- 1200
  layer3 <- 1600
  layer4 <- 2100
  layer5 <- 2400
  layer6 <- 3000
  layer7 <- 3600
  layer8 <- 800
  
  # sharp and blunt hedge triggers
  bluntp.cap <- 30
  sharp.low <- 30
  sharp.hi <- 45
  
  #max payouts
  bluntcap <- 6000000
  sharpcap <- 6000000
  
  # Price below blunt hedge price cap?
  DT.full[, bprice := ifelse(Regions_VIC_Price<bluntp.cap,1,0)]
  
  DT.full[, ws6 := ifelse(WindSpeed<6,1,0)]
  DT.full[, ws6.7 := ifelse(WindSpeed>6 & WindSpeed<7,1,0)*layer1*bprice]
  DT.full[, ws7.8 := ifelse(WindSpeed>7 & WindSpeed<8,1,0)*layer2*bprice]
  DT.full[, ws8.9 := ifelse(WindSpeed>8 & WindSpeed<9,1,0)*layer3*bprice]
  DT.full[, ws9.10 := ifelse(WindSpeed>9 & WindSpeed<10,1,0)*layer4*bprice]
  DT.full[, ws10.11 := ifelse(WindSpeed>10 & WindSpeed<11,1,0)*layer5*bprice]
  DT.full[, ws11.12 := ifelse(WindSpeed>11 & WindSpeed<12,1,0)*layer6*bprice]
  DT.full[, ws12.13 := ifelse(WindSpeed>12 & WindSpeed<13,1,0)*layer7*bprice]
  DT.full[, ws13.14 := ifelse(WindSpeed>13 & WindSpeed<14,1,0)*layer8*bprice]
  DT.full[, ws14 := ifelse(WindSpeed>14,1,0)]
  DT.full[date_time<'2021-07-01 10:00:00', blunt := (ws6.7+ws7.8+ws8.9+ws9.10+ws10.11+ws11.12+ws12.13+ws13.14)/2] #half price <Jul21
  DT.full[date_time>='2021-07-01 10:00:00', blunt := ws6.7+ws7.8+ws8.9+ws9.10+ws10.11+ws11.12+ws12.13+ws13.14]
  DT.full[, bluntcum := cumsum(blunt),.(rleid(Year))]
  # Now cap the blunt hedge payments if bluntcum > payout cap
  DT.full[, bluntpay := ifelse(bluntcum<bluntcap,blunt,0)] # this is the blunt insurance payout
  
  # Sharp hedge (don't forget the premium)
  DT.full[, sharp := 0]
  DT.full[date_time>='2021-07-01 10:00:00', sharp := ifelse((Regions_VIC_Price<sharp.hi & Regions_VIC_Price>sharp.low & WindSpeed>6.9 & WindSpeed<13.2),(-90+25.5*WindSpeed)*45,0)]
  DT.full[date_time>='2021-07-01 10:00:00', sharpflag := ifelse((Regions_VIC_Price<sharp.hi & Regions_VIC_Price>sharp.low & WindSpeed>6.9 & WindSpeed<13.2),1,0)]
  DT.full[, sharpcum := cumsum(sharp),.(rleid(Year))]
  # Now cap the blunt hedge payments if bluntcum > payout cap
  DT.full[, sharppay := ifelse(sharpcum<sharpcap,sharp,0)] # this is the sharp insurance payout
  
  blunthedgecost <- 2370000
  sharphedgecost <- 660000*4
  
  DT.full[, blunthedgeprem := blunthedgecost/(24*365)]
  DT.full[, sharphedgeprem := sharphedgecost/(24*365)]
  
  # construct P_L and cumulative PL
  DT.full[, P_L:=PPA_Mac+PPA_Oak+blunt]
  DT.full[date_time>='2021-07-01' & date_time<'2021-07-04', 
          P_L:=PPA_Mac+PPA_Oak+PPA_Chall+PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal+blunt+sharp]
  DT.full[date_time>='2021-07-04', P_L:=PPA_Mac+PPA_Oak+PPA_Chall+
            PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal+PPA_Waub+blunt+sharp]
  DT.full[,Cum_PL := cumsum(P_L)]
  
  return(DT.full)
}


ReconPL <- function() {
  # Get WF data from CETF assets
  DTT <- CETFVicWind()
  # Vic prices
  pricedat<-VicPrices()
  DT.all <- merge(pricedat, DTT, by = "date_time")
  
  DT.all$CROWLWF1 <- nafill(DT.all$CROWLWF1, "const", fill = 0)
  DT.all$CHALLHWF <- nafill(DT.all$CHALLHWF, "const", fill = 0)
  DT.all$MACARTH1 <- nafill(DT.all$MACARTH1, "const", fill = 0)
  DT.all$OAKLAND1 <- nafill(DT.all$OAKLAND1, "const", fill = 0)
  DT.all$PORTWF <- nafill(DT.all$PORTWF, "const", fill = 0)
  DT.all$WAUBRAWF <- nafill(DT.all$WAUBRAWF, "const", fill = 0)
  DT.all$YAMBUKWF <- nafill(DT.all$YAMBUKWF, "const", fill = 0)
  DT.all$YSWF1 <- nafill(DT.all$YSWF1, "const", fill = 0)
  
  #MLF	
  #Oakland	1.0063
  #Macarthur	0.9757
  #Challicum	0.9711
  #Crowlands	0.9026
  #Portland	0.991
  #Waubra	0.9228
  #Yambuk	1.0079
  #Yaloak	0.9711
  
  # CETF Portfolio
  DT.all[, Act_Oakland := 1.0063 * 0.881 * OAKLAND1/2]
  DT.all[, Act_Macarthur := 0.9757 * 0.119 * MACARTH1/2]
  DT.all[, Act_Challicum := 0.9711 * 0.29 * CHALLHWF/2]
  DT.all[, Act_Crowlands := 0.9026 * 0.30 * CROWLWF1/2]
  DT.all[, Act_Portland := 0.991 * 0.11 * PORTWF/2]
  DT.all[, Act_Yambuk := 1.0079 * 0.83 * YAMBUKWF/2]
  DT.all[, Act_Yaloak := 0.9711 * 0.40 * YSWF1/2]
  DT.all[, Act_Waubra := 0.9228 * 0.26 * WAUBRAWF/2]
  DT.all[, Act_AGL := Act_Oakland + Act_Macarthur]
  DT.all[, Act_Acciona := Act_Waubra]
  DT.all[, Act_PacHydro := Act_Challicum + Act_Crowlands + Act_Portland +
           Act_Yambuk + Act_Yaloak]
  DT.all[, Act_CETF := Act_Oakland+Act_Macarthur+Act_Challicum+Act_Crowlands+Act_Portland+
           Act_Yambuk+Act_Yaloak+Act_Waubra]
  
  # Start calculations
  DT.all[,PPA_Mac:=(Regions_VIC_Price-46.3)*Act_Macarthur]
  DT.all[,PPA_Oak:=(Regions_VIC_Price-46.3)*Act_Oakland]
  DT.all[,PPA_Chall:=(Regions_VIC_Price-43.75)*Act_Challicum]
  DT.all[,PPA_Crow:=(Regions_VIC_Price-43.75)*Act_Crowlands]
  DT.all[,PPA_Port:=(Regions_VIC_Price-43.75)*Act_Portland]
  DT.all[,PPA_Yamb:=(Regions_VIC_Price-43.75)*Act_Yambuk]
  DT.all[,PPA_Yal:=(Regions_VIC_Price-43.75)*Act_Yaloak]
  # seperate columns for Waubra calcs
  DT.all[,Waub_Collar:=ifelse(Regions_VIC_Price*0.898>=405,405,
                              ifelse(Regions_VIC_Price*0.898<=37.5,37.5,Regions_VIC_Price*0.898))]
  DT.all[,Waub_RawP:=(44.5+Waub_Collar)/2]
  DT.all[,PPA_Waub:=(ifelse(Regions_VIC_Price<0,0,Regions_VIC_Price)-Waub_RawP)*Act_Waubra]
  #PPA by counterpart
  DT.all[,PPA_AGL_Act:=PPA_Mac+PPA_Oak]
  DT.all[,AGL_Inflow:=Regions_VIC_Price*Act_AGL]
  DT.all[,AGL_Outflow:=46.32*Act_AGL]
  DT.all[,PPA_Acciona_Act:=PPA_Waub]
  DT.all[,Acciona_Inflow:=Regions_VIC_Price*Act_Waubra]
  DT.all[,Acciona_Outflow:=44.5*Act_Waubra]
  DT.all[,PPA_PH_Act:=PPA_Chall+PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal]
  DT.all[,PH_Inflow:=Regions_VIC_Price*Act_PacHydro]
  DT.all[,PH_Outflow:=43.75*Act_PacHydro]
  DT.all[,CETF_250_Inflow:=AGL_Inflow+Acciona_Inflow+PH_Inflow]
  DT.all[,CETF_250_Outflow:=AGL_Outflow+Acciona_Outflow+PH_Outflow]
  
  DT.all$grouped_time = lubridate::ceiling_date(DT.all$date_time, unit = "hour")
  #detach("package:reshape2", unload = TRUE)
  DT = DT.all %>%
    group_by(grouped_time) %>%
    summarise(Regions_VIC_Price = mean(Regions_VIC_Price),
              PPA_Chall = sum(PPA_Chall),
              PPA_Crow = sum(PPA_Crow),
              PPA_Mac = sum(PPA_Mac),
              PPA_Oak = sum(PPA_Oak),
              PPA_Port = sum(PPA_Port),
              PPA_Waub = sum(PPA_Waub),
              PPA_Yamb = sum(PPA_Yamb),
              PPA_Yal = sum(PPA_Yal),
              Act_Challicum = sum(Act_Challicum),
              Act_Crowlands = sum(Act_Crowlands),
              Act_Macarthur = sum(Act_Macarthur),
              Act_Oakland = sum(Act_Oakland),
              Act_Portland = sum(Act_Portland),
              Act_Waubra = sum(Act_Waubra),
              Act_Yambuk = sum(Act_Yambuk),
              Act_Yaloak = sum(Act_Yaloak),
              Act_AGL = sum(Act_AGL),
              Act_Acciona = sum(Act_Acciona),
              Act_PacHydro = sum(Act_PacHydro),
              Act_CETF = sum(Act_CETF),
              AGL_Inflow = sum(AGL_Inflow),
              AGL_Outflow = sum(AGL_Outflow),
              Acciona_Inflow = sum(Acciona_Inflow),
              PH_Inflow = sum(PH_Inflow),
              PH_Outflow = sum(PH_Outflow),
              CETF_250_Inflow = sum(CETF_250_Inflow),
              CETF_250_Outflow = sum(CETF_250_Outflow))
  
  # Bring windspeed data into the data table
  DT<-as.data.table(DT)
  DT[, date_time := ymd_hms(DT[["grouped_time"]])]#,tz = "Australia/Brisbane")] # set date format
  DT[, grouped_time := NULL]
  WindData<-windsp()
  DT.full <- merge(DT, WindData, by = "date_time", all = TRUE)
  # compute assumed windspeeds for missing data
  DT.full[,WindSpeed:=ifelse(is.na(WindSpeed),(Act_CETF+90)/25.5,WindSpeed)]
  
  # assign time periods to data
  DT.full[, Day := day(date_time)]
  DT.full[, Mth := month(date_time)]
  DT.full[, Year := year(date_time)]
  DT.full[, Hour := hour(date_time)]
  DT.full[, Week := week(date_time)]
  #DT.full[, Quart := quarter(date_time,with_year=TRUE,fiscal_start = 1)]
  
  
  # Blunt Hedge payoffs
  layer1 <- 100
  layer2 <- 1200
  layer3 <- 1600
  layer4 <- 2100
  layer5 <- 2400
  layer6 <- 3000
  layer7 <- 3600
  layer8 <- 800
  
  # sharp and blunt hedge triggers
  bluntp.cap <- 30
  sharp.low <- 30
  sharp.hi <- 45
  
  #max payouts
  bluntcap <- 6000000
  sharpcap <- 6000000
  
  # Price below blunt hedge price cap?
  DT.full[, bprice := ifelse(Regions_VIC_Price<bluntp.cap,1,0)]
  
  DT.full[, ws6 := ifelse(WindSpeed<6,1,0)]
  DT.full[, ws6.7 := ifelse(WindSpeed>6 & WindSpeed<7,1,0)*layer1*bprice]
  DT.full[, ws7.8 := ifelse(WindSpeed>7 & WindSpeed<8,1,0)*layer2*bprice]
  DT.full[, ws8.9 := ifelse(WindSpeed>8 & WindSpeed<9,1,0)*layer3*bprice]
  DT.full[, ws9.10 := ifelse(WindSpeed>9 & WindSpeed<10,1,0)*layer4*bprice]
  DT.full[, ws10.11 := ifelse(WindSpeed>10 & WindSpeed<11,1,0)*layer5*bprice]
  DT.full[, ws11.12 := ifelse(WindSpeed>11 & WindSpeed<12,1,0)*layer6*bprice]
  DT.full[, ws12.13 := ifelse(WindSpeed>12 & WindSpeed<13,1,0)*layer7*bprice]
  DT.full[, ws13.14 := ifelse(WindSpeed>13 & WindSpeed<14,1,0)*layer8*bprice]
  DT.full[, ws14 := ifelse(WindSpeed>14,1,0)]
  DT.full[date_time<'2021-07-01 10:00:00', blunt := (ws6.7+ws7.8+ws8.9+ws9.10+ws10.11+ws11.12+ws12.13+ws13.14)/2] #half price <Jul21
  DT.full[date_time>='2021-07-01 10:00:00', blunt := ws6.7+ws7.8+ws8.9+ws9.10+ws10.11+ws11.12+ws12.13+ws13.14]
  DT.full[, bluntcum := cumsum(blunt),.(rleid(Year))]
  # Now cap the blunt hedge payments if bluntcum > payout cap
  DT.full[, bluntpay := ifelse(bluntcum<bluntcap,blunt,0)] # this is the blunt insurance payout
  
  # Sharp hedge (don't forget the premium)
  DT.full[, sharp := 0]
  DT.full[date_time>='2021-07-01 10:00:00', sharp := ifelse((Regions_VIC_Price<sharp.hi & Regions_VIC_Price>sharp.low & WindSpeed>6.9 & WindSpeed<13.2),(-90+25.5*WindSpeed)*45,0)]
  DT.full[date_time>='2021-07-01 10:00:00', sharpflag := ifelse((Regions_VIC_Price<sharp.hi & Regions_VIC_Price>sharp.low & WindSpeed>6.9 & WindSpeed<13.2),1,0)]
  DT.full[, sharpcum := cumsum(sharp),.(rleid(Year))]
  # Now cap the blunt hedge payments if bluntcum > payout cap
  DT.full[, sharppay := ifelse(sharpcum<sharpcap,sharp,0)] # this is the sharp insurance payout
  
  blunthedgecost <- 2370000
  sharphedgecost <- 660000*4
  
  DT.full[, blunthedgeprem := blunthedgecost/(24*365)]
  DT.full[, sharphedgeprem := sharphedgecost/(24*365)]
  
  # construct P_L and cumulative PL
  DT.full[, P_L:=PPA_Mac+PPA_Oak+blunt]
  DT.full[date_time>='2021-07-01' & date_time<'2021-07-04', 
          P_L:=PPA_Mac+PPA_Oak+PPA_Chall+PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal+blunt+sharp]
  DT.full[date_time>='2021-07-04', P_L:=PPA_Mac+PPA_Oak+PPA_Chall+
            PPA_Crow+PPA_Port+PPA_Yamb+PPA_Yal+PPA_Waub+blunt+sharp]
  DT.full[,Cum_PL := cumsum(P_L)]
  
  # replace already settled payments
  settles<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Settlements.csv')
  settles<-as.data.table(settles)
  colnames(settles)<-c("date_time","PPA","Blunt","Sharp")
  settles$date_time<-mdy(settles$date_time)
  settles$date_time<-as.POSIXct(settles$date_time)
  # ignore data prior to last settlement date
  DT.last <- DT.full[!(date_time<last.settle),]
  DT.last[,PPA:=PPA_Chall+PPA_Crow+PPA_Mac+PPA_Oak+PPA_Port+PPA_Waub+PPA_Yal+PPA_Yamb]
  DT.last <- DT.last[,.(date_time,PPA,blunt,sharp)]
  colnames(DT.last)<-c("date_time","PPA","Blunt","Sharp")
  DT.settled <- merge(DT.last, settles, by = "date_time", all = TRUE)
  DT.settled[,PPA:=as.numeric(ifelse(is.na(PPA.x),PPA.y,PPA.x))]
  DT.settled[,Blunt:=as.numeric(ifelse(is.na(Blunt.x),Blunt.y,Blunt.x))]
  DT.settled[,Sharp:=as.numeric(ifelse(is.na(Sharp.x),Sharp.y,Sharp.x))]
  DT.settled[,PPA.x:=NULL]
  DT.settled[,PPA.y:=NULL]
  DT.settled[,Blunt.x:=NULL]
  DT.settled[,Blunt.y:=NULL]
  DT.settled[,Sharp.x:=NULL]
  DT.settled[,Sharp.y:=NULL]
  DT.settled[,PnL:=PPA+Blunt+Sharp]
  
  return(DT.settled)
}



## Run above functions to view data

EStatesPrices <- function() {
  connDB <- dwConnect()
  
  pricedat_raw <-
    dbGetQuery(
      connDB,
      "SELECT SETTLEMENTDATE, RRP, REGIONID FROM dbo.TRADINGPRICE WHERE (SETTLEMENTDATE > '2014-12-31 23:30:00')
                           AND (REGIONID='VIC1' OR REGIONID='NSW1' OR REGIONID='QLD1' OR REGIONID='SA1')"
    )
  pricedat_raw <- as.data.table(pricedat_raw)
  pricedat_raw[, date_time := ymd_hms(pricedat_raw[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
  pricedat_raw[, SETTLEMENTDATE := NULL]
  setcolorder(pricedat_raw, c("date_time", "REGIONID", "RRP"))
  price_temp <- dcast(pricedat_raw, date_time ~ REGIONID)
  pricedat <- price_temp[order(price_temp$date_time), ]
  pricedat <- as.data.table(pricedat)
  
  pricedat[, Day := day(date_time)]
  pricedat[, Mth := month(date_time)]
  pricedat[, Year := year(date_time)]
  pricedat[, Hour := hour(date_time)]
  pricedat[, Week := week(date_time)]
  #pricedat[, Quart := quarter(date_time, with_year = TRUE, fiscal_start = 1)]
  #pricedat[, days := ymd_hms(pricedat_raw[["date_time"]])]
  nsw <- pricedat[, .(mean(NSW1)), by = .(Day, Mth, Year)]
  qld <- pricedat[, .(mean(QLD1)), by = .(Day, Mth, Year)]
  sa <- pricedat[, .(mean(SA1)), by = .(Day, Mth, Year)]
  return(pricedat)
}

VicWindDWAP <- function() {
  connDB <- dwConnect()
  WF <- dbGetQuery(
    connDB,
    "SELECT * FROM dbo.DISPATCH_UNIT_SCADA WHERE (SETTLEMENTDATE > '2021-03-31 23:30:00')
                        AND (DUID='ARWF1' OR DUID='BALDHWF1' OR DUID='BULGANA1' OR DUID='CHALLHWF'
                        OR DUID='CHYTWF1' OR DUID='CROWLWF1' OR DUID='DUNDWF1' OR DUID='DUNDWF2'
                        OR DUID='DUNDWF3' OR DUID='ELAINWF1' OR DUID='KIATAWF1' OR DUID='MACARTH1'
                        OR DUID='MERCER01' OR DUID='MLWF1' OR DUID='MTGELWF1' OR DUID='MUWAWF1' OR
                        DUID='OAKLAND1' OR DUID='PORTWF' OR DUID='SALTCRK1' OR DUID='WAUBRAWF'
                        OR DUID='YAMBUKWF' OR DUID='YENDWF1' OR DUID='YSWF1')"
  )
  WF <- as.data.table(WF)
  WF[, date_time := ymd_hms(WF[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
  WF[, SETTLEMENTDATE := NULL]
  setcolorder(WF, c("date_time", "DUID", "SCADAVALUE"))
  WF_temp <- dcast(WF, date_time ~ DUID)
  DT <- WF_temp[order(WF_temp$date_time), ]
  
  ################################################################################
  ### Compute half-hour aggregations for SCADA data
  
  cuts <-
    seq(round(min(DT$date_time), "hours"), max(DT$date_time) + 30 * 60, "30 min")
  #
  # # CHALLHWF CROWLWF1 MACARTH1 OAKLAND1 PORTWF WAUBRAWF YAMBUKWF YSWF1
  ARWF1.hh <- aggregate(DT$ARWF1, list(cut(DT$date_time, cuts)), mean)
  BALDHWF1.hh <-
    aggregate(DT$BALDHWF1, list(cut(DT$date_time, cuts)), mean)
  BULGANA1.hh <-
    aggregate(DT$BULGANA1, list(cut(DT$date_time, cuts)), mean)
  CHALLHWF.hh <-
    aggregate(DT$CHALLHWF, list(cut(DT$date_time, cuts)), mean)
  CHYTWF1.hh <-
    aggregate(DT$CHYTWF1, list(cut(DT$date_time, cuts)), mean)
  CROWLWF1.hh <-
    aggregate(DT$CROWLWF1, list(cut(DT$date_time, cuts)), mean)
  DUNDWF1.hh <-
    aggregate(DT$DUNDWF1, list(cut(DT$date_time, cuts)), mean)
  DUNDWF2.hh <-
    aggregate(DT$DUNDWF2, list(cut(DT$date_time, cuts)), mean)
  DUNDWF3.hh <-
    aggregate(DT$DUNDWF3, list(cut(DT$date_time, cuts)), mean)
  ELAINWF1.hh <-
    aggregate(DT$ELAINWF1, list(cut(DT$date_time, cuts)), mean)
  KIATAWF1.hh <-
    aggregate(DT$KIATAWF1, list(cut(DT$date_time, cuts)), mean)
  MACARTH1.hh <-
    aggregate(DT$MACARTH1, list(cut(DT$date_time, cuts)), mean)
  MERCER01.hh <-
    aggregate(DT$MERCER01, list(cut(DT$date_time, cuts)), mean)
  MLWF1.hh <- aggregate(DT$MLWF1, list(cut(DT$date_time, cuts)), mean)
  MTGELWF1.hh <-
    aggregate(DT$MTGELWF1, list(cut(DT$date_time, cuts)), mean)
  MUWAWF1.hh <-
    aggregate(DT$MUWAWF1, list(cut(DT$date_time, cuts)), mean)
  OAKLAND1.hh <-
    aggregate(DT$OAKLAND1, list(cut(DT$date_time, cuts)), mean)
  PORTWF.hh <-
    aggregate(DT$PORTWF, list(cut(DT$date_time, cuts)), mean)
  SALTCRK1.hh <-
    aggregate(DT$SALTCRK1, list(cut(DT$date_time, cuts)), mean)
  WAUBRAWF.hh <-
    aggregate(DT$WAUBRAWF, list(cut(DT$date_time, cuts)), mean)
  YAMBUKWF.hh <-
    aggregate(DT$YAMBUKWF, list(cut(DT$date_time, cuts)), mean)
  YENDWF1.hh <-
    aggregate(DT$YENDWF1, list(cut(DT$date_time, cuts)), mean)
  YSWF1.hh <- aggregate(DT$YSWF1, list(cut(DT$date_time, cuts)), mean)
  
  ARWF1.hh <- as.data.table(ARWF1.hh)
  BALDHWF1.hh <- as.data.table(BALDHWF1.hh)
  BULGANA1.hh <- as.data.table(BULGANA1.hh)
  CHALLHWF.hh <- as.data.table(CHALLHWF.hh)
  CHYTWF1.hh <- as.data.table(CHYTWF1.hh)
  CROWLWF1.hh <- as.data.table(CROWLWF1.hh)
  DUNDWF1.hh <- as.data.table(DUNDWF1.hh)
  DUNDWF2.hh <- as.data.table(DUNDWF2.hh)
  DUNDWF3.hh <- as.data.table(DUNDWF3.hh)
  ELAINWF1.hh <- as.data.table(ELAINWF1.hh)
  KIATAWF1.hh <- as.data.table(KIATAWF1.hh)
  MACARTH1.hh <- as.data.table(MACARTH1.hh)
  MERCER01.hh <- as.data.table(MERCER01.hh)
  MLWF1.hh <- as.data.table(MLWF1.hh)
  MTGELWF1.hh <- as.data.table(MTGELWF1.hh)
  MUWAWF1.hh <- as.data.table(MUWAWF1.hh)
  OAKLAND1.hh <- as.data.table(OAKLAND1.hh)
  PORTWF.hh <- as.data.table(PORTWF.hh)
  SALTCRK1.hh <- as.data.table(SALTCRK1.hh)
  WAUBRAWF.hh <- as.data.table(WAUBRAWF.hh)
  YAMBUKWF.hh <- as.data.table(YAMBUKWF.hh)
  YENDWF1.hh <- as.data.table(YENDWF1.hh)
  YSWF1.hh <- as.data.table(YSWF1.hh)
  
  ARWF1.hh[, date_time := ymd_hms(ARWF1.hh[["Group.1"]])]
  BALDHWF1.hh[, date_time := ymd_hms(BALDHWF1.hh[["Group.1"]])]
  BULGANA1.hh[, date_time := ymd_hms(BULGANA1.hh[["Group.1"]])]
  CHALLHWF.hh[, date_time := ymd_hms(CHALLHWF.hh[["Group.1"]])]
  CHYTWF1.hh[, date_time := ymd_hms(CHYTWF1.hh[["Group.1"]])]
  CROWLWF1.hh[, date_time := ymd_hms(CROWLWF1.hh[["Group.1"]])]
  DUNDWF1.hh[, date_time := ymd_hms(DUNDWF1.hh[["Group.1"]])]
  DUNDWF2.hh[, date_time := ymd_hms(DUNDWF2.hh[["Group.1"]])]
  DUNDWF3.hh[, date_time := ymd_hms(DUNDWF3.hh[["Group.1"]])]
  ELAINWF1.hh[, date_time := ymd_hms(ELAINWF1.hh[["Group.1"]])]
  KIATAWF1.hh[, date_time := ymd_hms(KIATAWF1.hh[["Group.1"]])]
  MACARTH1.hh[, date_time := ymd_hms(MACARTH1.hh[["Group.1"]])]
  MERCER01.hh[, date_time := ymd_hms(MERCER01.hh[["Group.1"]])]
  MLWF1.hh[, date_time := ymd_hms(MLWF1.hh[["Group.1"]])]
  MTGELWF1.hh[, date_time := ymd_hms(MTGELWF1.hh[["Group.1"]])]
  MUWAWF1.hh[, date_time := ymd_hms(MUWAWF1.hh[["Group.1"]])]
  OAKLAND1.hh[, date_time := ymd_hms(OAKLAND1.hh[["Group.1"]])]
  PORTWF.hh[, date_time := ymd_hms(PORTWF.hh[["Group.1"]])]
  SALTCRK1.hh[, date_time := ymd_hms(SALTCRK1.hh[["Group.1"]])]
  WAUBRAWF.hh[, date_time := ymd_hms(WAUBRAWF.hh[["Group.1"]])]
  YAMBUKWF.hh[, date_time := ymd_hms(YAMBUKWF.hh[["Group.1"]])]
  YENDWF1.hh[, date_time := ymd_hms(YENDWF1.hh[["Group.1"]])]
  YSWF1.hh[, date_time := ymd_hms(YSWF1.hh[["Group.1"]])]
  
  DTT <-
    cbind(
      ARWF1.hh$date_time,
      ARWF1.hh[, 2],
      BALDHWF1.hh[, 2],
      BULGANA1.hh[, 2],
      CHALLHWF.hh[, 2],
      CHYTWF1.hh[, 2],
      CROWLWF1.hh[, 2],
      DUNDWF1.hh[, 2],
      DUNDWF2.hh[, 2],
      DUNDWF3.hh[, 2],
      ELAINWF1.hh[, 2],
      KIATAWF1.hh[, 2],
      MACARTH1.hh[, 2],
      MERCER01.hh[, 2],
      MLWF1.hh[, 2],
      MTGELWF1.hh[, 2],
      MUWAWF1.hh[, 2],
      OAKLAND1.hh[, 2],
      PORTWF.hh[, 2],
      SALTCRK1.hh[, 2],
      WAUBRAWF.hh[, 2],
      YAMBUKWF.hh[, 2],
      YENDWF1.hh[, 2],
      YSWF1.hh[, 2]
    )
  
  DTT <- as.data.table(DTT)
  
  # 23 wind farms in Victoria
  #ARWF1 BALDHWF1 BULGANA1 CHALLHWF CHYTWF1 CROWLWF1 DUNDWF1 DUNDWF2 DUNDWF3
  #ELAINWF1 KIATAWF1 MACARTH1 MERCER01 MLWF1 MTGELWF1 MUWAWF1 OAKLAND1
  #PORTWF SALTCRK1 WAUBRAWF YAMBUKWF YENDWF1 YSWF1
  
  DTT <-
    setnames(
      DTT,
      c(
        "date_time",
        "ARWF1",
        "BALDHWF1",
        "BULGANA1",
        "CHALLHWF",
        "CHYTWF1",
        "CROWLWF1",
        "DUNDWF1",
        "DUNDWF2",
        "DUNDWF3",
        "ELAINWF1",
        "KIATAWF1",
        "MACARTH1",
        "MERCER01",
        "MLWF1",
        "MTGELWF1",
        "MUWAWF1",
        "OAKLAND1",
        "PORTWF",
        "SALTCRK1",
        "WAUBRAWF",
        "YAMBUKWF",
        "YENDWF1",
        "YSWF1"
      )
    )
  
  ################################################################################################
  # Regional reference prices
  pricedat<-EStatesPrices()
  
  #############################################################################################
  # Merge price and volumes
  DT.all <- merge(pricedat, DTT, by = "date_time")
  DT.all <- as.data.table(DT.all)
  
  #ARWF1 BALDHWF1 BULGANA1 CHALLHWF CHYTWF1 CROWLWF1 DUNDWF1 DUNDWF2 DUNDWF3
  #ELAINWF1 KIATAWF1 MACARTH1 MERCER01 MLWF1 MTGELWF1 MUWAWF1 OAKLAND1
  #PORTWF SALTCRK1 WAUBRAWF YAMBUKWF YENDWF1 YSWF1
  
  DT.all$ARWF1 <- nafill(DT.all$ARWF1, "const", fill = 0)
  DT.all$BALDHWF1 <- nafill(DT.all$BALDHWF1, "const", fill = 0)
  DT.all$BULGANA1 <- nafill(DT.all$BULGANA1, "const", fill = 0)
  DT.all$CHALLHWF <- nafill(DT.all$CHALLHWF, "const", fill = 0)
  DT.all$CHYTWF1 <- nafill(DT.all$CHYTWF1, "const", fill = 0)
  DT.all$CROWLWF1 <- nafill(DT.all$CROWLWF1, "const", fill = 0)
  DT.all$DUNDWF1 <- nafill(DT.all$DUNDWF1, "const", fill = 0)
  DT.all$DUNDWF2 <- nafill(DT.all$DUNDWF2, "const", fill = 0)
  DT.all$DUNDWF3 <- nafill(DT.all$DUNDWF3, "const", fill = 0)
  DT.all$ELAINWF1 <- nafill(DT.all$ELAINWF1, "const", fill = 0)
  DT.all$KIATAWF1 <- nafill(DT.all$KIATAWF1, "const", fill = 0)
  DT.all$MACARTH1 <- nafill(DT.all$MACARTH1, "const", fill = 0)
  DT.all$MERCER01 <- nafill(DT.all$MERCER01, "const", fill = 0)
  DT.all$MLWF1 <- nafill(DT.all$MLWF1, "const", fill = 0)
  DT.all$MTGELWF1 <- nafill(DT.all$MTGELWF1, "const", fill = 0)
  DT.all$MUWAWF1 <- nafill(DT.all$MUWAWF1, "const", fill = 0)
  DT.all$OAKLAND1 <- nafill(DT.all$OAKLAND1, "const", fill = 0)
  DT.all$PORTWF <- nafill(DT.all$PORTWF, "const", fill = 0)
  DT.all$SALTCRK1 <- nafill(DT.all$SALTCRK1, "const", fill = 0)
  DT.all$WAUBRAWF <- nafill(DT.all$WAUBRAWF, "const", fill = 0)
  DT.all$YAMBUKWF <- nafill(DT.all$YAMBUKWF, "const", fill = 0)
  DT.all$YENDWF1 <- nafill(DT.all$YENDWF1, "const", fill = 0)
  DT.all$YSWF1 <- nafill(DT.all$YSWF1, "const", fill = 0)
  
  #ARWF1 BALDHWF1 BULGANA1 CHALLHWF CHYTWF1 CROWLWF1 DUNDWF1 DUNDWF2 DUNDWF3
  #ELAINWF1 KIATAWF1 MACARTH1 MERCER01 MLWF1 MTGELWF1 MUWAWF1 OAKLAND1
  #PORTWF SALTCRK1 WAUBRAWF YAMBUKWF YENDWF1 YSWF1
  
  # price x gen for each gen
  DT.all[, ARWF1.genp := ARWF1 * VIC1]
  DT.all[, BALDHWF1.genp := BALDHWF1 * VIC1]
  DT.all[, BULGANA1.genp := BULGANA1 * VIC1]
  DT.all[, CHALLHWF.genp := CHALLHWF * VIC1]
  DT.all[, CHYTWF1.genp := CHYTWF1 * VIC1]
  DT.all[, CROWLWF1.genp := CROWLWF1 * VIC1]
  DT.all[, DUNDWF1.genp := DUNDWF1 * VIC1]
  DT.all[, DUNDWF2.genp := DUNDWF2 * VIC1]
  DT.all[, DUNDWF3.genp := DUNDWF3 * VIC1]
  DT.all[, ELAINWF1.genp := ELAINWF1 * VIC1]
  DT.all[, KIATAWF1.genp := KIATAWF1 * VIC1]
  DT.all[, MACARTH1.genp := MACARTH1 * VIC1]
  DT.all[, MERCER01.genp := MERCER01 * VIC1]
  DT.all[, MLWF1.genp := MLWF1 * VIC1]
  DT.all[, MTGELWF1.genp := MTGELWF1 * VIC1]
  DT.all[, MUWAWF1.genp := MUWAWF1 * VIC1]
  DT.all[, OAKLAND1.genp := OAKLAND1 * VIC1]
  DT.all[, PORTWF.genp := PORTWF * VIC1]
  DT.all[, SALTCRK1.genp := SALTCRK1 * VIC1]
  DT.all[, WAUBRAWF.genp := WAUBRAWF * VIC1]
  DT.all[, YAMBUKWF.genp := YAMBUKWF * VIC1]
  DT.all[, YENDWF1.genp := YENDWF1 * VIC1]
  DT.all[, YSWF1.genp := YSWF1 * VIC1]
  
  # CETF Portfolio
  DT.all[, CETF := 0.29 * CHALLHWF + 0.30 * CROWLWF1 + 0.119 * MACARTH1 +
           0.881 * OAKLAND1 + 0.11 * PORTWF + 0.26 * WAUBRAWF + 0.83 * YAMBUKWF + 0.40 *
           YSWF1]
  DT.all[, AGL := 0.881 * OAKLAND1 + 0.119 * MACARTH1]
  DT.all[, PacHydro := 0.29 * CHALLHWF + 0.30 * CROWLWF1 + 0.11 * PORTWF +
           0.83 * YAMBUKWF + 0.40 * YSWF1]
  DT.all[, Naive := 1 / 23 * (
    ARWF1 + BALDHWF1 + BULGANA1 + CHALLHWF + CHYTWF1 + CROWLWF1 + DUNDWF1 +
      DUNDWF2 + DUNDWF3 + ELAINWF1 + KIATAWF1 + MACARTH1 +
      MERCER01 + MLWF1 + MTGELWF1 +
      MUWAWF1 + OAKLAND1 + PORTWF + SALTCRK1 + WAUBRAWF +
      YAMBUKWF + YENDWF1 + YSWF1
  )]
  DT.all[, CETF.100mw := AGL * VIC1]
  DT.all[, CETF.PacHydro := PacHydro * VIC1]
  DT.all[, CETF.250mw := CETF * VIC1]
  DT.all[, VicWind := Naive * VIC1]
  
  
  DT.all[, Day := day(date_time)]
  DT.all[, Mth := month(date_time)]
  DT.all[, Year := year(date_time)]
  DT.all[, Hour := hour(date_time)]
  DT.all[, Week := week(date_time)]
  #DT.all[, Quart := quarter(date_time, with_year = TRUE, fiscal_start = 1)]
  
  dwap.100 <- DT.all[, .(sum(CETF.100mw) / sum(AGL)), by = .(Mth, Year)]
  dwap.250 <- DT.all[, .(sum(CETF.250mw) / sum(CETF)), by = .(Mth, Year)]
  dwap.PacHydro <-
    DT.all[, .(sum(CETF.PacHydro) / sum(PacHydro)), by = .(Mth, Year)]
  dwap.VicWind <- DT.all[, .(sum(VicWind) / sum(Naive)), by = .(Mth, Year)]
  #dwap.with.blunt <- DT.all[, .(sum(CETF.100mw)+387]
  
  
  dwap.ARWF1 <- DT.all[, .(sum(ARWF1.genp) / sum(ARWF1)), by = .(Mth, Year)]
  dwap.BALDHWF1 <-
    DT.all[, .(sum(BALDHWF1.genp) / sum(BALDHWF1)), by = .(Mth, Year)]
  dwap.BULGANA1 <-
    DT.all[, .(sum(BULGANA1.genp) / sum(BULGANA1)), by = .(Mth, Year)]
  dwap.CHALLHWF <-
    DT.all[, .(sum(CHALLHWF.genp) / sum(CHALLHWF)), by = .(Mth, Year)]
  dwap.CHYTWF1 <-
    DT.all[, .(sum(CHYTWF1.genp) / sum(CHYTWF1)), by = .(Mth, Year)]
  dwap.CROWLWF1 <-
    DT.all[, .(sum(CROWLWF1.genp) / sum(CROWLWF1)), by = .(Mth, Year)]
  dwap.DUNDWF1 <-
    DT.all[, .(sum(DUNDWF1.genp) / sum(DUNDWF1)), by = .(Mth, Year)]
  dwap.DUNDWF2 <-
    DT.all[, .(sum(DUNDWF2.genp) / sum(DUNDWF2)), by = .(Mth, Year)]
  dwap.DUNDWF3 <-
    DT.all[, .(sum(DUNDWF3.genp) / sum(DUNDWF3)), by = .(Mth, Year)]
  dwap.ELAINWF1 <-
    DT.all[, .(sum(ELAINWF1.genp) / sum(ELAINWF1)), by = .(Mth, Year)]
  dwap.KIATAWF1 <-
    DT.all[, .(sum(KIATAWF1.genp) / sum(KIATAWF1)), by = .(Mth, Year)]
  dwap.MACARTH1 <-
    DT.all[, .(sum(MACARTH1.genp) / sum(MACARTH1)), by = .(Mth, Year)]
  dwap.MERCER01 <-
    DT.all[, .(sum(MERCER01.genp) / sum(MERCER01)), by = .(Mth, Year)]
  dwap.MLWF1 <- DT.all[, .(sum(MLWF1.genp) / sum(MLWF1)), by = .(Mth, Year)]
  dwap.MTGELWF1 <-
    DT.all[, .(sum(MTGELWF1.genp) / sum(MTGELWF1)), by = .(Mth, Year)]
  dwap.MUWAWF1 <-
    DT.all[, .(sum(MUWAWF1.genp) / sum(MUWAWF1)), by = .(Mth, Year)]
  dwap.OAKLAND1 <-
    DT.all[, .(sum(OAKLAND1.genp) / sum(OAKLAND1)), by = .(Mth, Year)]
  dwap.PORTWF <- DT.all[, .(sum(PORTWF.genp) / sum(PORTWF)), by = .(Mth, Year)]
  dwap.SALTCRK1 <-
    DT.all[, .(sum(SALTCRK1.genp) / sum(SALTCRK1)), by = .(Mth, Year)]
  dwap.WAUBRAWF <-
    DT.all[, .(sum(WAUBRAWF.genp) / sum(WAUBRAWF)), by = .(Mth, Year)]
  dwap.YAMBUKWF <-
    DT.all[, .(sum(YAMBUKWF.genp) / sum(YAMBUKWF)), by = .(Mth, Year)]
  dwap.YENDWF1 <-
    DT.all[, .(sum(YENDWF1.genp) / sum(YENDWF1)), by = .(Mth, Year)]
  dwap.YSWF1 <- DT.all[, .(sum(YSWF1.genp) / sum(YSWF1)), by = .(Mth, Year)]
  av.spot.VIC1 <- DT.all[, .(mean(VIC1)), by = .(Mth, Year)]
  av.spot.NSW1 <- DT.all[, .(mean(NSW1)), by = .(Mth, Year)]
  av.spot.QLD1 <- DT.all[, .(mean(QLD1)), by = .(Mth, Year)]
  
  dwap.mth <-
    cbind(
      dwap.ARWF1$Mth,
      dwap.ARWF1$Year,
      dwap.ARWF1[, 3],
      dwap.BALDHWF1[, 3],
      dwap.BULGANA1[, 3],
      dwap.CHALLHWF[, 3],
      dwap.CHYTWF1[, 3],
      dwap.CROWLWF1[, 3],
      dwap.DUNDWF1[, 3],
      dwap.DUNDWF2[, 3],
      dwap.DUNDWF3[, 3],
      dwap.ELAINWF1[, 3],
      dwap.KIATAWF1[, 3],
      dwap.MACARTH1[, 3],
      dwap.MERCER01[, 3],
      dwap.MLWF1[, 3],
      dwap.MTGELWF1[, 3],
      dwap.MUWAWF1[, 3],
      dwap.OAKLAND1[, 3],
      dwap.PORTWF[, 3],
      dwap.SALTCRK1[, 3],
      dwap.WAUBRAWF[, 3],
      dwap.YAMBUKWF[, 3],
      dwap.YENDWF1[, 3],
      dwap.YSWF1[, 3],
      dwap.100[, 3],
      dwap.250[, 3],
      dwap.PacHydro[, 3],
      dwap.VicWind[, 3],
      av.spot.VIC1[, 3],
      av.spot.NSW1[, 3],
      av.spot.QLD1[, 3]
    )
  
  dwap.mth <- as.data.table(dwap.mth)
  
  dwap.mth <-
    setnames(
      dwap.mth,
      c(
        "Month",
        "Year",
        "ARWF1",
        "BALDHWF1",
        "BULGANA1",
        "CHALLHWF",
        "CHYTWF1",
        "CROWLWF1",
        "DUNDWF1",
        "DUNDWF2",
        "DUNDWF3",
        "ELAINWF1",
        "KIATAWF1",
        "MACARTH1",
        "MERCER01",
        "MLWF1",
        "MTGELWF1",
        "MUWAWF1",
        "OAKLAND1",
        "PORTWF",
        "SALTCRK1",
        "WAUBRAWF",
        "YAMBUKWF",
        "YENDWF1",
        "YSWF1",
        "CETF100MW",
        "CETF250MW",
        "CETF.PacHydro",
        "VIC.WIND",
        "VIC1.Spot",
        "NSW1.Spot",
        "QLD1.Spot"
      )
    )
  
  dwaps <- melt(dwap.mth, c("Month", "Year"))
  dwaps <- setnames(dwaps, c("Month", "Year", "Generator", "DWAP"))
  
  return(dwaps)
}


######################################################################
######################################################################

VicFutures <- function() {
  # list of ASX files
  df <- list.files(
    "//172.17.135.233/share/ASX_FTP_Daily_Import/ETD_ELEC_AU",
    recursive = TRUE,
    pattern = 'FinalSnapshot',
    full.names = T
  )
  # read list into a data frame
  ldf <- lapply(df, read.csv)
  ldf <- as.data.table(ldf)
  ldf <- select(ldf,-contains("Bid"))
  ldf <- select(ldf,-contains("Ask"))
  ldf <- select(ldf,-contains("Open"))
  ldf <- select(ldf,-contains("Last.Price"))
  ldf <- select(ldf,-contains("Low"))
  ldf <- select(ldf,-contains("High"))
  ldf <- select(ldf,-contains("Implied"))
  ldf <- select(ldf,-contains("Volume"))
  ldf <- select(ldf,-contains("Time"))
  ldf <- select(ldf,-contains("X"))
  
  # add ID for reshaping data table
  ldf$ID <- seq.int(nrow(ldf))
  #ldf<-as.data.table(ldf)
  measure <- c("Code",
               "Last.Trading.Date",
               "Settlement.Price",
               "Settlement.Date")
  
  # ensure to detach reshape2 - the patterns() function in data.table won't work otherwise
  # detach("package:reshape2", unload = TRUE)
  ldf_long <-
    melt(ldf,
         measure.vars = patterns(measure),
         value.name = measure)
  
  rm(ldf)
  
  # read table annotating contract names and long/short ASX contracts
  contracts<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Contract Names.csv', header = TRUE)
  contracts<-as.data.table(contracts)
  
  vic.futures <- as.character(contracts$VIC)
  
  ldf.vic.futures <- ldf_long[ldf_long$Code %in% vic.futures, ]
  rm(ldf_long)
  ldf.vic.futures[, Settle.Date := dmy(ldf.vic.futures[["Settlement.Date"]])]#,tz = "Australia/Brisbane")] # set date format
  ldf.vic.futures[, Qtr.End.Date := dmy(ldf.vic.futures[["Last.Trading.Date"]])]
  ldf.vic.futures[, Settlement.Date := NULL]
  ldf.vic.futures[, Last.Trading.Date := NULL]
  ldf.vic.futures[, ID := NULL]
  ldf.vic.futures[, variable := NULL]
  setcolorder(ldf.vic.futures,
              c("Code", "Qtr.End.Date",
                "Settlement.Price", "Settle.Date"))
  ldf.vic.futures <- ldf.vic.futures[order(-Settle.Date, Qtr.End.Date)]
  # remove duplicates
  vic.futures <-
    ldf.vic.futures[-which(duplicated(ldf.vic.futures)),]
  rm(ldf.vic.futures)
  return(vic.futures)
}

NswFutures <- function() {
  # list of ASX files
  df <- list.files(
    "//172.17.135.233/share/ASX_FTP_Daily_Import/ETD_ELEC_AU",
    recursive = TRUE,
    pattern = 'FinalSnapshot',
    full.names = T
  )
  # read list into a data frame
  ldf <- lapply(df, read.csv)
  ldf <- as.data.table(ldf)
  ldf <- select(ldf,-contains("Bid"))
  ldf <- select(ldf,-contains("Ask"))
  ldf <- select(ldf,-contains("Open"))
  ldf <- select(ldf,-contains("Last.Price"))
  ldf <- select(ldf,-contains("Low"))
  ldf <- select(ldf,-contains("High"))
  ldf <- select(ldf,-contains("Implied"))
  ldf <- select(ldf,-contains("Volume"))
  ldf <- select(ldf,-contains("Time"))
  ldf <- select(ldf,-contains("X"))
  
  # add ID for reshaping data table
  ldf$ID <- seq.int(nrow(ldf))
  measure <- c("Code",
               "Last.Trading.Date",
               "Settlement.Price",
               "Settlement.Date")
  
  # ensure to detach reshape2 - the patterns() function in data.table won't work otherwise
  # detach("package:reshape2", unload = TRUE)
  ldf_long <-
    melt(ldf,
         measure.vars = patterns(measure),
         value.name = measure)
  
  rm(ldf)
  
  # read table annotating contract names and long/short ASX contracts
  contracts<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Contract Names.csv', header = TRUE)
  contracts<-as.data.table(contracts)
  
  nsw.futures <- as.character(contracts$NSW)
  
  ldf.nsw.futures <- ldf_long[ldf_long$Code %in% nsw.futures, ]
  rm(ldf_long)
  ldf.nsw.futures[, Settle.Date := dmy(ldf.nsw.futures[["Settlement.Date"]])]#,tz = "Australia/Brisbane")] # set date format
  ldf.nsw.futures[, Qtr.End.Date := dmy(ldf.nsw.futures[["Last.Trading.Date"]])]
  ldf.nsw.futures[, Settlement.Date := NULL]
  ldf.nsw.futures[, Last.Trading.Date := NULL]
  ldf.nsw.futures[, ID := NULL]
  ldf.nsw.futures[, variable := NULL]
  setcolorder(ldf.nsw.futures,
              c("Code", "Qtr.End.Date",
                "Settlement.Price", "Settle.Date"))
  ldf.nsw.futures <- ldf.nsw.futures[order(-Settle.Date, Qtr.End.Date)]
  # remove duplicates
  nsw.futures <-
    ldf.nsw.futures[-which(duplicated(ldf.nsw.futures)),]
  rm(ldf.nsw.futures)
  return(nsw.futures)
}

QldFutures <- function() {
  # list of ASX files
  df <- list.files(
    "Z:/ASX_FTP_Daily_Import/ETD_ELEC_AU",
    recursive = TRUE,
    pattern = 'FinalSnapshot',
    full.names = T
  )
  # read list into a data frame
  ldf <- lapply(df, read.csv)
  ldf <- as.data.table(ldf)
  ldf <- select(ldf,-contains("Bid"))
  ldf <- select(ldf,-contains("Ask"))
  ldf <- select(ldf,-contains("Open"))
  ldf <- select(ldf,-contains("Last.Price"))
  ldf <- select(ldf,-contains("Low"))
  ldf <- select(ldf,-contains("High"))
  ldf <- select(ldf,-contains("Implied"))
  ldf <- select(ldf,-contains("Volume"))
  ldf <- select(ldf,-contains("Time"))
  ldf <- select(ldf,-contains("X"))
  
  # add ID for reshaping data table
  ldf$ID <- seq.int(nrow(ldf))
  measure <- c("Code",
               "Last.Trading.Date",
               "Settlement.Price",
               "Settlement.Date")
  
  # ensure to detach reshape2 - the patterns() function in data.table won't work otherwise
  # detach("package:reshape2", unload = TRUE)
  ldf_long <-
    melt(ldf,
         measure.vars = patterns(measure),
         value.name = measure)
  
  rm(ldf)
  
  # read table annotating contract names and long/short ASX contracts
  contracts<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Contract Names.csv', header = TRUE)
  contracts<-as.data.table(contracts)
  
  qld.futures <- as.character(contracts$QLD)

  ldf.qld.futures <- ldf_long[ldf_long$Code %in% qld.futures, ]
  rm(ldf_long)
  ldf.qld.futures[, Settle.Date := dmy(ldf.qld.futures[["Settlement.Date"]])]#,tz = "Australia/Brisbane")] # set date format
  ldf.qld.futures[, Qtr.End.Date := dmy(ldf.qld.futures[["Last.Trading.Date"]])]
  ldf.qld.futures[, Settlement.Date := NULL]
  ldf.qld.futures[, Last.Trading.Date := NULL]
  ldf.qld.futures[, ID := NULL]
  ldf.qld.futures[, variable := NULL]
  setcolorder(ldf.qld.futures,
              c("Code", "Qtr.End.Date",
                "Settlement.Price", "Settle.Date"))
  ldf.qld.futures <- ldf.qld.futures[order(-Settle.Date, Qtr.End.Date)]
  # remove duplicates
  qld.futures <-
    ldf.qld.futures[-which(duplicated(ldf.qld.futures)),]
  rm(ldf.qld.futures)
  return(qld.futures)
}

AsxOptions <- function() {
  # list of ASX files
  df <- list.files(
    "Z:/ASX_FTP_Daily_Import/ETD_ELEC_AU",
    recursive = TRUE,
    pattern = 'FinalSnapshot',
    full.names = T
  )
  # read list into a data frame
  ldf <- lapply(df, read.csv)
  ldf <- as.data.table(ldf)
  ldf <- select(ldf,-contains("Bid"))
  ldf <- select(ldf,-contains("Ask"))
  ldf <- select(ldf,-contains("Open"))
  ldf <- select(ldf,-contains("Last.Price"))
  ldf <- select(ldf,-contains("Low"))
  ldf <- select(ldf,-contains("High"))
  ldf <- select(ldf,-contains("Volume"))
  ldf <- select(ldf,-contains("Time"))
  ldf <- select(ldf,-contains("X"))
  
  # add ID for reshaping data table
  ldf$ID <- seq.int(nrow(ldf))
  measure <- c("Code",
               "Last.Trading.Date",
               "Settlement.Price",
               "Implied.Volatility",
               "Settlement.Date")
  
  # ensure to detach reshape2 - the patterns() function in data.table won't work otherwise
  # detach("package:reshape2", unload = TRUE)
  ldf_long <-
    melt(ldf,
         measure.vars = patterns(measure),
         value.name = measure)
  
  rm(ldf)
  
  # read table annotating contract names and long/short ASX contracts
  contracts<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Contract Names.csv', header = TRUE)
  contracts<-as.data.table(contracts)
  
  asx.options <- as.character(contracts$ASX)
  
  ldf.options <- ldf_long[ldf_long$Code %in% asx.options, ]
  rm(ldf_long)
  ldf.options[, Settle.Date := dmy(ldf.options[["Settlement.Date"]])]#,tz = "Australia/Brisbane")] # set date format
  ldf.options[, Qtr.End.Date := dmy(ldf.options[["Last.Trading.Date"]])]
  ldf.options[, Settlement.Date := NULL]
  ldf.options[, Last.Trading.Date := NULL]
  ldf.options[, ID := NULL]
  ldf.options[, variable := NULL]
  setcolorder(ldf.options,
              c("Code", "Qtr.End.Date",
                "Settlement.Price", "Implied.Volatility", "Settle.Date"))
  ldf.options <- ldf.options[order(-Settle.Date, Qtr.End.Date)]
  # remove duplicates
  asx.options <-
    ldf.options[-which(duplicated(ldf.options)),]
  rm(ldf.options)
  return(asx.options)
}

################################################################
################################################################
## CONSTRUCT THE MTM TABLE FOR STRUCTURAL POSITIONS

MTMTable <- function() {
  
  today<-Sys.Date()
  disc.rate<-0.15
  Sh.Fact<-0.73
  AGL.ppa<-46.3
  PH.ppa<-43.75
  Acc.ppa<-44.5
  
  Vfutures<-VicFutures() # run the VIC futures data extraction
  DT<-Vfutures[(1:17),]
  DT <- setnames(DT, c("Code", "Quart", "Fprice", "Sdate"))
  DT<-DT[,Fprice.m1.v:=Vfutures[18:34]$Settlement.Price]
  DT<-DT[,Sdate.m1.v:=Vfutures[18:34]$Settle.Date]
  
  Nfutures<-NswFutures() # run the NSW futures data extraction
  DTn<-Nfutures[(1:17),]
  DTn <- setnames(DTn, c("Code", "Quart", "Fprice", "Sdate"))
  DTn<-DTn[,Fprice.m1.n:=Nfutures[18:34]$Settlement.Price]
  DTn<-DTn[,Sdate.m1.n:=Nfutures[18:34]$Settle.Date]
  
  Qfutures<-QldFutures() # run the NSW futures data extraction
  DTq<-Qfutures[(1:17),]
  DTq <- setnames(DTq, c("Code", "Quart", "Fprice", "Sdate"))
  DTq<-DTq[,Fprice.m1.q:=Qfutures[18:34]$Settlement.Price]
  DTq<-DTq[,Sdate.m1.q:=Qfutures[18:34]$Settle.Date]
  
  DTint1<-merge.data.table(DT,DTn,by="Quart")
  DTint2<-merge.data.table(DTint1,DTq,by="Quart")
  DT<-DTint2
  
  #amend columns
  removals<-c("Code.x","Fprice.x","Sdate.x","Code.y",
              "Fprice.y","Sdate.y","Code","Fprice","Sdate")
  replacements<-c("Code.v","Fprice.v","Sdate.v","Code.n",
                  "Fprice.n","Sdate.n","Code.q","Fprice.q","Sdate.q")
  setnames(DT,removals,replacements)
    
  DT<-DT[,timedur:=(Quart-today)/365]
  DT$timedur<-as.numeric(DT$timedur)
  DT<-DT[,DF:=1/(1+disc.rate)^timedur]
  DT[,Fprice.v.SF:=Fprice.v*Sh.Fact]
  DT[,Fprice.m1.v.SF:=Fprice.m1.v*Sh.Fact]
  DT[,Fdelta.v:=Fprice.v.SF-Fprice.m1.v.SF]
  
  # Load WF deal data
  Wfarms<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/WF_Deals.csv')
  Wfarms <- as.data.table(Wfarms)
  Wfarms[, Quart := mdy(Wfarms[["Quart"]])]#,tz = "Australia/Brisbane")] # set date format
  
  # merge data
  DT.mtm<-merge.data.table(DT,Wfarms,by='Quart')
  
  # compute MTM deltas
  DT.mtm[,PPA_Mac:=Vol_Mac*(Fprice.v.SF-AGL_PPA_COST)*DF]
  DT.mtm[,PPA_Oak:=Vol_Oak*(Fprice.v.SF-AGL_PPA_COST)*DF]
  DT.mtm[,PPA_Chall:=Vol_Chall*(Fprice.v.SF-PH_PPA_COST)*DF]
  DT.mtm[,PPA_Crow:=Vol_Crow*(Fprice.v.SF-PH_PPA_COST)*DF]
  DT.mtm[,PPA_Port:=Vol_Port*(Fprice.v.SF-PH_PPA_COST)*DF]
  DT.mtm[,PPA_Yamb:=Vol_Yamb*(Fprice.v.SF-PH_PPA_COST)*DF]
  DT.mtm[,PPA_Yal:=Vol_Yal*(Fprice.v.SF-PH_PPA_COST)*DF]
  DT.mtm[,PPA_Waub:=Vol_Waub*(Fprice.v.SF-WAUB_PPA_COST)*DF]
  DT.mtm[,CETF_Volume:=Vol_Mac+Vol_Oak+Vol_Chall+Vol_Crow+
           Vol_Port+Vol_Yamb+Vol_Yal+Vol_Waub]
  DT.mtm[,Struct.Swap:=Swaps*24*91*(PH_SWAP_COST-Fprice.v)*DF]
  
  # compute MTM in $$
  
  DT.mtm[,CETF.mtm:=CETF_Volume*Fdelta.v*DF+Struct.Swap]
  DT.mtm[,mtm_Mac:=Vol_Mac*Fdelta.v*DF]
  DT.mtm[,mtm_Oak:=Vol_Oak*Fdelta.v*DF]
  DT.mtm[,mtm_Chall:=Vol_Chall*Fdelta.v*DF]
  DT.mtm[,mtm_Crow:=Vol_Crow*Fdelta.v*DF]
  DT.mtm[,mtm_Port:=Vol_Port*Fdelta.v*DF]
  DT.mtm[,mtm_Yamb:=Vol_Yamb*Fdelta.v*DF]
  DT.mtm[,mtm_Yal:=Vol_Yal*Fdelta.v*DF]
  DT.mtm[,mtm_Waub:=Vol_Waub*Fdelta.v*DF]
  
  rm(Wfarms)
  rm(DT)
  rm(Vfutures)
  rm(Nfutures)
  rm(Qfutures)

  return(DT.mtm)
    
}

###############################################################
## CONSTRUCT THE MTM TABLE FOR TRADING POSITIONS

MTMTableProp <- function() {
  
  today<-Sys.Date()
  disc.rate<-0.15
  Sh.Fact<-0.73
  AGL.ppa<-46.3
  PH.ppa<-43.75
  Acc.ppa<-44.5
  
  Vfutures<-VicFutures() # run the VIC futures data extraction
  Nfutures<-NswFutures() # run the NSW futures data extraction
  Aoptions<-AsxOptions()
  Aoptions<-Aoptions[order(-(Settle.Date), Code)]
  
  DTvic<-Vfutures[(1:17),]
  DTvic <- setnames(DTvic, c("Code", "Quart", "Fprice", "Sdate"))
  DTvic<-DTvic[,Fprice.m1:=Vfutures[18:34]$Settlement.Price]
  DTvic<-DTvic[,Sdate.m1:=Vfutures[18:34]$Settle.Date]
  
  DTnsw<-Nfutures[(1:17),]
  DTnsw <- setnames(DTnsw, c("Code", "Quart", "Fprice", "Sdate"))
  DTnsw<-DTnsw[,Fprice.m1:=Nfutures[18:34]$Settlement.Price]
  DTnsw<-DTnsw[,Sdate.m1:=Nfutures[18:34]$Settle.Date]
  
  # read in contract list to set nrows
  contracts<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/Contract Names.csv', header = TRUE)
  contracts<-as.data.table(contracts)
  contracts<-contracts[,cflag:=ifelse(ASX=="",0,1)]
  nconts<-sum(contracts$cflag)
  
  DToptions<-Aoptions[(1:nconts),]
  DToptions <- setnames(DToptions, c("Code", "Quart", "Oprice","Volatility", "Sdate"))
  DToptions<-DToptions[,Oprice.m1:=Aoptions[(nconts+1):(2*nconts)]$Settlement.Price]
  DToptions<-DToptions[,Volatility.m1:=Aoptions[(nconts+1):(2*nconts)]$Implied.Volatility]
  DToptions<-DToptions[,Sdate.m1:=Aoptions[(nconts+1):(2*nconts)]$Settle.Date]
  
  # DT<-DT[,timedur:=(Quart-today)/365]
  # DT$timedur<-as.numeric(DT$timedur)
  # DT<-DT[,DF:=1/(1+disc.rate)^timedur]
  DTfut<-merge.data.table(DTvic,DTnsw,by="Quart")
  DTfut <- setnames(DTfut, c("Quart", "Code.Vic", "Fprice.Vic", "Sdate.Vic", "Fpricem1.Vic", "Sdatem1.Vic",
                             "Code.Nsw", "Fprice.Nsw", "Sdate.Nsw", "Fpricem1.Nsw", "Sdatem1.Nsw"))
  DTfut[,Fprice.Vic.SF:=Fprice.Vic*Sh.Fact]
  DTfut[,Fpricem1.Vic.SF:=Fpricem1.Vic*Sh.Fact]
  DTfut[,Fprice.Nsw.SF:=Fprice.Nsw*Sh.Fact]
  DTfut[,Fpricem1.Nsw.SF:=Fpricem1.Nsw*Sh.Fact]
  
  DTfut[,Fdelta.Vic:=Fprice.Vic.SF-Fpricem1.Vic.SF]
  DTfut[,Fdelta.Nsw:=Fprice.Nsw.SF-Fpricem1.Nsw.SF]
  
  setcolorder(DToptions,
              c("Quart", "Code", "Oprice", "Volatility", "Sdate",
                "Oprice.m1", "Volatility.m1", "Sdate.m1"))
  
  # Load WF deal data
  AsxPositions<-read.csv('//172.17.135.233/share/Dashboards/PowerBI/ASX_Deals.csv')
  AsxPositions <- as.data.table(AsxPositions)
  
  # merge data
  DTasx<-merge.data.table(DToptions,AsxPositions,by='Code')
  
  # compute MTM deltas
  DTasx[,Daily.delta:=Volume*(Oprice-Oprice.m1)]
  DTasx[,Total.delta:=Volume*(Oprice-SettlePrice)]
  
  return(DTasx)
  
}

##############################################################################
##############################################################################
## RUN FUNCTIONS

# PNL table for viewing in power BI
pl.table<-PriceVol()
View(pl.table)
write.csv(pl.table,'Z:/Dashboards/cp_data.csv')

#Dwaps for power BI
cetf.dwap<-VicWindDWAP()
View(cetf.dwap)
write.csv(cetf.dwap,'Z:/Dashboards/PowerBI/dwap.csv')

# Daily MTM movements of CETF structural portfolio
mtm<-MTMTable()
View(mtm)
write.csv(mtm,'Z:/Dashboards/PowerBI/mtm.csv')

# Daily MTM movements of CETF trading portfolio
mtmtrading<-MTMTableProp()
View(mtmtrading)
write.csv(mtmtrading,'Z:/Dashboards/PowerBI/mtmtrading.csv')

