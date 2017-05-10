rm(list=ls())
setwd("~")

########################################
# EFF/ RDS / AK                        #
# Maps for Baseball crime data         #
# May 9, 2017                          #
# Big Data project                     # 
########################################

require(stringr)
require(ggmap)
require(ggplot2)
require(rgdal)
require(rgeos)
require(maptools)
require(plyr)

#####################
# Local Directories #
#####################

# Point dir1 to data folder on local machine. 
# Will be concatenated with no separators. 
dir1 <- "/Users/eduardofierro/Google Drive/SegundoSemestre/BigData/Project/Data/"

# Point dir2 to directory to Output Maps
dir2 <- "/Users/eduardofierro/Google Drive/SegundoSemestre/BigData/Project/Maps/"

#############
# Load data #
#############

data_loader <- function(dir, file){
	
	# Function to load data from pyspark
	# Takes @file (name of file)
	#       @dir directory where file is located
	main_data <- readLines(paste0(dir1, file))	
	main_data <- gsub("CARDS, DIC", "CARDS DIC", main_data)
	main_data <- gsub("HOMICIDE-NEGLIGENT,UNCLASSIFIE", "HOMICIDE-NEGLIGENT UNCLASSIFIE", main_data)
	main_data <- str_split(main_data, "[,]")
	main_data <- do.call(rbind, main_data)
	main_data <- data.frame(main_data, stringsAsFactors=F)
	names(main_data) <- c("date", "crime", "lat", "lon", "stadium", "isGameday")
	main_data$date <- gsub("[(]", "", main_data$date )
	main_data$date <- gsub("[']", "", main_data$date )
	main_data$crime <- gsub("[']", "", main_data$crime )
	main_data$crime <- gsub("[(]", "", main_data$crime )
	main_data$lat <- gsub("[']", "", main_data$lat )
	main_data$lon <- gsub("[']", "", main_data$lon )
	main_data$lat <- as.numeric(main_data$lat)
	main_data$lon <- as.numeric(main_data$lon)
	main_data$stadium <- gsub("[']", "", main_data$stadium )
	main_data$isGameday <- gsub("[']", "", main_data$isGameday )
	main_data$isGameday <- gsub("[)]", "", main_data$isGameday )
	main_data$isGameday[main_data$isGameday==" gameday"] <- "Gameday"
	main_data$isGameday[main_data$isGameday==" not_gameday"] <- "Not Gameday"
	dates <- str_split(main_data $date, "[/]")
	dates <- do.call(rbind, dates)
	dates <- data.frame(dates, stringsAsFactors=F)
	names(dates) <- c("month", "day", "year")
	dates$month <- as.numeric(dates$month)
	dates$day <- as.numeric(dates$day)
	dates$year <- as.numeric(dates$year)
	main_data <- cbind(main_data, dates)
	return(main_data)
	
}

subset250 <- data_loader(dir1, "subset250.out")
subset500 <- data_loader(dir1, "subset500.out")
subset1000 <- data_loader(dir1, "subset1000.out")
subset2000 <- data_loader(dir1, "subset2000.out")

data_results <- readLines(paste0(dir1, "BaseballAnalysis.txt"))
# Same problem, the comma on two categories: 
data_results <- gsub("CARDS, DIC", "CARDS DIC", data_results)
data_results <- gsub("HOMICIDE-NEGLIGENT,UNCLASSIFIE", "HOMICIDE-NEGLIGENT UNCLASSIFIE", data_results)
data_results <- str_split(data_results, "[,]")
data_results <- do.call(rbind, data_results)
data_results <- data.frame(data_results, stringsAsFactors=F)
names(data_results) <- c("distance", "crime", "stadium", "intercept", "coefficient", "pValue")
data_results$distance <- as.numeric(data_results$distance)
data_results$intercept <- as.numeric(data_results$intercept)
data_results$coefficient <- as.numeric(data_results$coefficient)
data_results$pValue <- as.numeric(data_results$pValue)

# Filter results invalid: 
data_results$pValue[data_results$pValue > 1 | data_results$pValue < 0] <- NA
data_filtered_results <- subset(data_results, is.na(data_results$pValue)==F)
data_invalid_results <- subset(data_results, is.na(data_results$pValue)==T)

rm(data_results)

data_filtered_results$gameday_average <- data_filtered_results$intercept + data_filtered_results$coefficient
data_filtered_results$difference <- data_filtered_results$gameday_average  - data_filtered_results$intercept
data_filtered_results <- data_filtered_results[order(-data_filtered_results$difference),]

###################
# Prep GGmap base #
###################

ny <-  get_googlemap("New York, NY", zoom=11, maptype="hybrid")

mets <-  get_googlemap("Citi Field, New York", zoom=13, maptype="hybrid") 
yankees <-  get_googlemap("Yankee Stadium, New York", zoom=13, maptype="hybrid") 

# More zoom to yankee stadium: 
yankees2 <-  get_googlemap("Yankee Stadium, New York", zoom=16, maptype="roadmap") 


#########################################
# Map 1 and 2 - All Crimes 2015 2000mts #
#########################################

data_map <- subset(subset2000, subset2000$year==2015)

map  <- ggmap(mets) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Mets Baseball stadiums \n 2000 mts bounding box") 
ggsave(paste(dir2, "1_MetsAll2000mts.png", sep="/"), plot= map,  width = 8, height = 14)


map  <- ggmap(yankees) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Yankees's Baseball stadiums \n 2000 mts bounding box") 
ggsave(paste(dir2, "2_YankeesAll2000mts.png", sep="/"), plot= map,  width = 8, height = 14)

#########################################
# Map 3 and 4 - All Crimes 2015 1000mts #
#########################################

data_map <- subset(subset1000, subset1000$year==2015)

map  <- ggmap(mets) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Mets Baseball stadiums \n 1000 mts bounding box") 
ggsave(paste(dir2, "3_MetsAll1000mts.png", sep="/"), plot= map,  width = 8, height = 14)


map  <- ggmap(yankees) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Yankees's Baseball stadiums \n 1000 mts bounding box") 
ggsave(paste(dir2, "4_YankeesAll1000mts.png", sep="/"), plot= map,  width = 8, height = 14)

########################################
# Map 5 and 6 - All Crimes 2015 500mts #
########################################

data_map <- subset(subset500, subset500$year==2015)

map  <- ggmap(mets) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Mets Baseball stadiums \n 500 mts bounding box") 
ggsave(paste(dir2, "5_MetsAll500mts.png", sep="/"), plot= map,  width = 8, height = 14)


map  <- ggmap(yankees) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Yankees's Baseball stadiums \n 500 mts bounding box") 
ggsave(paste(dir2, "6_YankeesAll500mts.png", sep="/"), plot= map,  width = 8, height = 14)

########################################
# Map 7 and 8 - All Crimes 2015 250mts #
########################################

data_map <- subset(subset250, subset250$year==2015)

map  <- ggmap(mets) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Mets Baseball stadiums \n 250 mts bounding box") 
ggsave(paste(dir2, "7_MetsAll250mts.png", sep="/"), plot= map,  width = 8, height = 14)


map  <- ggmap(yankees) + 
        geom_point(data= data_map, aes(x = lon, y = lat), alpha=0.5, colour= "red") + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("All crimes 2015 around Yankees's Baseball stadiums \n 250 mts bounding box") 
ggsave(paste(dir2, "8_YankeesAll250mts.png", sep="/"), plot= map,  width = 8, height = 14)

#####################################################
# Map 9 - OTHER STATE LAWS NON PENAL LA 2015 250mts #
#####################################################

data_map <- subset(subset250, subset250$crime==" OTHER STATE LAWS NON PENAL LA" | subset250$crime==" OTHER OFFENSES RELATED TO THEF")

map  <- ggmap(yankees2) + 
        geom_point(data= data_map, aes(x = lon, y = lat, color=crime), alpha=0.5, size=4) + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("Selected crimes during 2006 - 2015 \n 250 mts bounding box") +
        theme(legend.position = "bottom")
ggsave(paste(dir2, "9_YankeesOthers250mts.png", sep="/"), plot= map,  width = 8, height = 14)


###########################
# Map 10 - 250mts Yankees #
###########################

data_map <- subset(subset250, subset250$crime %in% attributes(table(subset(data_filtered_results, data_filtered_results$pValue<0.05 & data_filtered_results$distance==250 & data_filtered_results$crime!=' all')$crime))[[2]][[1]])

map  <- ggmap(yankees2) + 
        geom_point(data= data_map, aes(x = lon, y = lat, color=crime), alpha=0.5) + 
        facet_wrap(~isGameday, ncol=1) + 
        theme_nothing(legend= TRUE) + 
        coord_fixed() + 
        ggtitle("'OTHER STATE LAWS NON PENAL LAW' crimes 2006 - 2015 \n 250 mts bounding box") 
ggsave(paste(dir2, "10_YankeesOthers250mts.png", sep="/"), plot= map,  width = 8, height = 14)
