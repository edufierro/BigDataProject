rm(list=ls())
setwd("~")

################################
# Eduardo Fierro F.            #
# Scrapper Baseball Data       #
# Results Yankees and mets     #
# Big Data                     #
# May the 4th (be with you)    #
################################

require(XML)
require(httr)
require(stringr)
require(plyr)
require(rgeos)
require(rgdal)

######################
# Define Geom-Points #
######################

lat <- c(40.829582, 40.757077)
lon <- c(-73.926309, -73.845852)  
stadium <- c("yankees", "mets") 
              
stadiums <- data.frame(stadiums = stadium, lat = lat, lon = lon)
rm(stadium, lat ,lon)
stadiums <- SpatialPointsDataFrame(cbind(stadiums$lon, stadiums$lat), stadiums,  proj4string=CRS("+proj=longlat"))
stadiums$lat <- stadiums$lon <- NULL

stadiums <- spTransform(stadiums, CRS("+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 +x_0=2500000 +y_0=0 +ellps=GRS80 +units=m +no_defs"))

stadiums250m <- gBuffer(stadiums, width=250, byid=T)
stadiums500m <- gBuffer(stadiums, width=500, byid=T)
stadiums1k <- gBuffer(stadiums, width=1000, byid=T)
stadiums2k <- gBuffer(stadiums, width=2000, byid=T)

stadiums250m <- spTransform(stadiums250m, CRS("+proj=longlat"))
stadiums500m <- spTransform(stadiums500m, CRS("+proj=longlat"))
stadiums1k <- spTransform(stadiums1k, CRS("+proj=longlat"))
stadiums2k <- spTransform(stadiums2k, CRS("+proj=longlat"))
stadiums <- spTransform(stadiums, CRS("+proj=longlat"))

######################
# Get Bounding boxes #
######################

# 250 mts #
subset(stadiums250m, stadiums250m@data$stadiums=="yankees")@bbox
subset(stadiums250m, stadiums250m@data$stadiums=="mets")@bbox

# 500 mts #
subset(stadiums500m, stadiums500m@data$stadiums=="yankees")@bbox
subset(stadiums500m, stadiums500m@data$stadiums=="mets")@bbox

# 1 km #
subset(stadiums1k, stadiums1k@data$stadiums=="yankees")@bbox
subset(stadiums1k, stadiums1k@data$stadiums=="mets")@bbox

# 2 km #
subset(stadiums2k, stadiums2k@data$stadiums=="yankees")@bbox
subset(stadiums2k, stadiums2k@data$stadiums=="mets")@bbox