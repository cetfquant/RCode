######################################################################
### Download 5 minute data and convert to 1/2 hr - by SCADA and by State RRP
######################################################################

library(odbc)
library(dplyr)
library(dbplyr)
library(sqldf)
library(data.table)
library(reshape2)
library(lubridate)
# Connection code....enter password when prompted

con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "SQL Server",
                      Server   = "172.17.133.92",
                      Database = "aemosql",
                      UID      = "aemosqluser",
                      PWD      =  "LVtsFxCup17Ckk7", #rstudioapi::askForPassword("aemosqluser"), #pw = LVtsFxCup17Ckk7
                      Port     = 1433)

#################################################################################################
# WF 5 min in MW  2021-02-28 23:30:00

WF <- dbGetQuery(con,"SELECT * FROM dbo.DISPATCH_UNIT_SCADA WHERE (SETTLEMENTDATE > '2021-02-28 23:30:00')
                        AND (DUID='CHALLHWF' OR DUID='CROWLWF1' OR DUID='MACARTH1' 
                        OR DUID='OAKLAND1' OR DUID='PORTWF' OR DUID='WAUBRAWF' OR DUID='YAMBUKWF'
                        OR DUID='YSWF1') ")
WF <- as.data.table(WF)
WF[, date_time := ymd_hms(WF[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
WF[,SETTLEMENTDATE:=NULL]
setcolorder(WF, c("date_time", "DUID", "SCADAVALUE"))
WF_temp <- dcast(WF, date_time ~ DUID)
DT <- WF_temp[order(WF_temp$date_time),]

################################################################################################
# Regional reference price
pricedat_raw <- dbGetQuery(con,"SELECT SETTLEMENTDATE, RRP FROM dbo.TRADINGPRICE WHERE (SETTLEMENTDATE > '2021-02-28 23:30:00')
                           AND (REGIONID='VIC1')")
pricedat_raw <- as.data.table(pricedat_raw)
pricedat_raw[, date_time := ymd_hms(pricedat_raw[["SETTLEMENTDATE"]])]#,tz = "Australia/Brisbane")] # set date format
pricedat_raw[, SETTLEMENTDATE := NULL]
setcolorder(pricedat_raw,c("date_time","RRP"))
pricedat_raw <- setNames(pricedat_raw, c("date_time", "Regions_VIC_Price"))
pricedat <- pricedat_raw[order(pricedat_raw$date_time),]

#################################################################
######################################################################
### Compute half-hour aggregations for SCADA data

cuts <- seq(round(min(DT$date_time), "hours"), max(DT$date_time)+30*60, "30 min")
#
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

DT.all <- merge(pricedat, DTT, by = "date_time")

DT.all$CROWLWF1 <- nafill(DT.all$CROWLWF1, "const", fill = 0)
DT.all$CHALLHWF <- nafill(DT.all$CHALLHWF, "const", fill = 0)
DT.all$MACARTH1 <- nafill(DT.all$MACARTH1, "const", fill = 0)
DT.all$OAKLAND1 <- nafill(DT.all$OAKLAND1, "const", fill = 0)
DT.all$PORTWF <- nafill(DT.all$PORTWF, "const", fill = 0)
DT.all$WAUBRAWF <- nafill(DT.all$WAUBRAWF, "const", fill = 0)
DT.all$YAMBUKWF <- nafill(DT.all$YAMBUKWF, "const", fill = 0)
DT.all$YSWF1 <- nafill(DT.all$YSWF1, "const", fill = 0)

# CETF Portfolio
DT.all[, total_MW := 0.29*CHALLHWF+0.30*CROWLWF1+0.119*MACARTH1+0.881*OAKLAND1+0.11*PORTWF+0.26*WAUBRAWF+0.83*YAMBUKWF+0.40*YSWF1]
DT.all[, Act_Oakland := 0.881*OAKLAND1]
DT.all[, Act_Macarthur := 0.119*MACARTH1]
DT.all[, Act_Challicum := 0.29*CHALLHWF]
DT.all[, Act_Crowlands := 0.30*CROWLWF1]
DT.all[, Act_Portland := 0.11*PORTWF]
DT.all[, Act_Yambuk := 0.83*YAMBUKWF]
DT.all[, Act_Yaloak := 0.40*YSWF1]
DT.all[, Act_Waubra := 0.26*WAUBRAWF]
DT.all[, Act_AGL := Act_Oakland + Act_Macarthur]
DT.all[, Act_Acciona := Act_Waubra]
DT.all[, Act_PacHydro := Act_Challicum+Act_Crowlands+Act_Portland+Act_Yambuk+Act_Yaloak]

#DT.all[is.na(DT.all)] <- 0

# # CETF Portfolio
# # 29% CHALL 30% CROW 11.9% Macarthur 88.1% oakland  0.11% PORT 0.26% WAUBRA 0.83% YAMBUKWF 0.82% YS
#DT.all[, total_MW := 0.29*CHALLHWF+0.30*CROWLWF1+0.119*MACARTH1+0.881*OAKLAND1+0.11*PORTWF+0.26*WAUBRAWF+0.83*YAMBUKWF+0.40*YSWF1]
# Benchmark 67% CHALL	44%	CROW 8% Macarthur	55% oakland	23%	PORT 18%	WAUB 85%	YAMBUK 40% Yaloak
#DT.all[, benchmark := 0.67*CHALLHWF+0.44*CROWLWF1+0.08*MACARTH1+0.55*OAKLAND1+0.23*PORTWF+0.18*WAUBRAWF+0.85*YAMBUKWF+0.40*YSWF1]

#########################################################################
setwd("Z:/Dashboards/Data")
library("xlsx")
write.xlsx(DT.all,file="hhr_update.xlsx",sheetName = "Data",append = FALSE)

#########################################################################

