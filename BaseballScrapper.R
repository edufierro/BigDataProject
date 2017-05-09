rm(list=ls())
setwd("~")

#################################################
# Eduardo Fierro F.                             #
# Scrapper Baseball Data                        #
# Results Yankees and mets                      #
# Big Data                                      #
# Data Scraped: May the 4th (be with you), 2017 #
#################################################

# Load dependencies: 
require(XML)
require(httr)
require(stringr)
require(plyr)

###############
# Directories #
###############

# Output directory for created data
dir1 <- "/Users/eduardofierro/Google Drive/SegundoSemestre/BigData/Project/Data/"

#############
# Functions #
#############

str_cleaner <- function(x){
	x <- gsub("\n", "", x)
    x <- gsub("\t", "", x)
    x <- str_trim(x, side="both")
    return(x)
}

######################
# Scrapper - Yankees #
######################

data.all <- data.frame()

for(year in 2008:2016){
	print(paste("*** YEAR: ", year, " ***", sep=""))
	url <- paste0("http://www.baseball-reference.com/teams/NYY/", year, ".shtml")
    page.doc <- content(GET(url))
    page.doc <- htmlParse(page.doc)
    data <- xpathApply(page.doc, "//*/li[@class = 'result']", xmlValue)
    data <- do.call(rbind, data)
    data <- str_cleaner(data)
    data <- data.frame(data)
    url_lists <- xpathApply(page.doc, "//*/li[@class = 'result']//*/a", xmlGetAttr, "href")
    url_lists <- do.call(rbind, url_lists)
    data <- cbind(data, url_lists)
    data[] <- lapply(data, as.character)
    data$attendance <- as.character(NA)
    data$venue <- as.character(NA)
    pb <- txtProgressBar(min=1, max=nrow(data), style=3)
    for(i in 1:nrow(data)){
    	url2 <-  paste0("http://www.baseball-reference.com", data[i,2])
    	page.doc2 <- content(GET(url2))
    	page.doc2 <- htmlParse(page.doc2)
    	info <- xpathApply(page.doc2, "//*/div[@class = 'scorebox_meta']/div", xmlValue)
    	info <- do.call(cbind, info)
    	venue <- info[,4]
    	attendance <- info[,3]
    	venue <- str_cleaner(venue)
    	attendance <- str_cleaner(attendance)
    	data$attendance[i] <- attendance
    	data$venue[i] <- venue
    	rm(url2, page.doc2, info, venue, attendance)
    	setTxtProgressBar(pb, i) 
    }
    close(pb)
    data$year <- year
    data.all <- rbind.fill(data.all, data)
    rm(url, page.doc, url_lists, i, pb)
}
rm(year)

yankees <- subset(data.all, data.all$venue=="Venue: Yankee Stadium II" | data.all$venue=="Venue: Yankee Stadium III")
yankees$attendance <- gsub("Attendance: ", "", yankees$attendance)
yankees$attendance <- gsub(",", "", yankees$attendance)
yankees$attendance <- as.numeric(yankees$attendance)
yankees$win <- as.numeric(str_detect(yankees$data, "beat"))
yankees$lost <- as.numeric(str_detect(yankees$data, "lost"))

data <- do.call(rbind, str_split(yankees$data, ","))[,1]
data <-  do.call(rbind, str_split(data, "[.]"))[,2]
data <-  do.call(rbind, str_split(data, " "))
data <- as.data.frame(data)
names(data) <- c("drop", "month", "day")
data$drop <- NULL
data$month <- as.character(data$month)
data$day <- as.numeric(as.character(data$day))

yankees <- cbind(yankees, data)
rm(data)
yankees$team <- "Yankees"


###################
# Scrapper - METS #
###################

# Same code, differente URL 

data.all <- data.frame()

for(year in 2015:2016){
	print(paste("*** YEAR: ", year, " ***", sep=""))
	url <- paste0("http://www.baseball-reference.com/teams/NYM/", year, ".shtml")
    page.doc <- content(GET(url))
    page.doc <- htmlParse(page.doc)
    data <- xpathApply(page.doc, "//*/li[@class = 'result']", xmlValue)
    data <- do.call(rbind, data)
    data <- str_cleaner(data)
    data <- data.frame(data)
    url_lists <- xpathApply(page.doc, "//*/li[@class = 'result']//*/a", xmlGetAttr, "href")
    url_lists <- do.call(rbind, url_lists)
    data <- cbind(data, url_lists)
    data[] <- lapply(data, as.character)
    data$attendance <- as.character(NA)
    data$venue <- as.character(NA)
    pb <- txtProgressBar(min=1, max=nrow(data), style=3)
    for(i in 1:nrow(data)){
    	url2 <-  paste0("http://www.baseball-reference.com", data[i,2])
    	page.doc2 <- content(GET(url2))
    	page.doc2 <- htmlParse(page.doc2)
    	info <- xpathApply(page.doc2, "//*/div[@class = 'scorebox_meta']/div", xmlValue)
    	info <- do.call(cbind, info)
    	venue <- info[,4]
    	attendance <- info[,3]
    	venue <- str_cleaner(venue)
    	attendance <- str_cleaner(attendance)
    	data$attendance[i] <- attendance
    	data$venue[i] <- venue
    	rm(url2, page.doc2, info, venue, attendance)
    	setTxtProgressBar(pb, i) 
    }
    close(pb)
    data$year <- year
    data.all <- rbind.fill(data.all, data)
    rm(url, page.doc, url_lists, i, pb)
}
rm(year)

mets <- subset(data.all, data.all$venue=="Venue: Citi Field")
mets$attendance <- gsub("Attendance: ", "", mets$attendance)
mets$attendance <- gsub(",", "", mets$attendance)
mets$attendance <- as.numeric(mets$attendance)
mets$win <- as.numeric(str_detect(mets$data, "beat"))
mets$lost <- as.numeric(str_detect(mets$data, "lost"))

data <- do.call(rbind, str_split(mets$data, ","))[,1]
data <-  do.call(rbind, str_split(data, "[.]"))[,2]
data <-  do.call(rbind, str_split(data, " "))
data <- as.data.frame(data)
names(data) <- c("drop", "month", "day")
data$drop <- NULL
data$month <- as.character(data$month)
data$day <- as.numeric(as.character(data$day))

mets <- cbind(mets, data)
rm(data)
mets$team <- "Mets"

both <- rbind(mets, yankees)


##############
# Clean date #
##############

both$month_num <- as.numeric(NA)

both$month_num[both$month=="Apr"] <- 4
both$month_num[both$month=="Aug"] <- 8
both$month_num[both$month=="Jul"] <- 7
both$month_num[both$month=="Jun"] <- 6
both$month_num[both$month=="Mar"] <- 3
both$month_num[both$month=="May"] <- 5
both$month_num[both$month=="Oct"] <- 10
both$month_num[both$month=="Sep"] <- 9

both$date <- paste(formatC(both$month_num, width = 2, format = "d", flag = "0"), formatC(both$day, width = 2, format = "d", flag = "0"), as.character(both$year), sep="/")

write.csv(both , paste0(dir1, "BaseballData.csv"), row.names=F)

