# Big Data Project
# Regression outputs 

library('stringr')
library('ggplot2')

# setwd("~/Desktop/big_data/project/BigDataProject")

data_results <- readLines("data/BaseballAnalysis.txt")

# Same problem, the comma on two categories: 
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

#### 

data_results$pValue[data_results$pValue > 1 | data_results$pValue < 0] <- NA
data_filtered_results <- subset(data_results, is.na(data_results$pValue)==F)
data_invalid_results <- subset(data_results, is.na(data_results$pValue)==T)

#### 

data_filtered_results$gameday_average <- data_filtered_results$intercept + data_filtered_results$coefficient
data_filtered_results$difference <- data_filtered_results$gameday_average  - data_filtered_results$intercept
data_filtered_results <- data_filtered_results[order(-data_filtered_results$difference),]
d <- data_filtered_results

crimes <- unique(d$crime)
stadium = unique(d$stadium)


crimes[1]

attach(d)
d[ d$crime == crimes[1] & d$stadium  == ' Yankees',]
d$dist_factor <- as.factor(d$distance)
d$percent_coeff <- d$coefficient/d$intercept

boxplot(coefficient~stadium,data=d[d$crime == crimes[52],], main="Car Milage Data", 
        xlab="Stadium", ylab="Coefficient")


i <- 1

ggplot(data=d[d$crime == crimes[i]   ,], aes(x=stadium, y=coefficient, fill=stadium)) + 
  geom_bar( stat="identity") + 
  guides(fill=FALSE) +
  xlab("Stadium") + ylab("Coefficient") +
  ggtitle(paste('Mean increase on gameday \n Crime:',crimes[i])) + facet_wrap( ~ dist_factor, ncol=2)

sig_crimes <- c(" ASSAULT 3 & RELATED OFFENSES" , " CRIMINAL MISCHIEF & RELATED OF",
                " DANGEROUS DRUGS"," DANGEROUS WEAPONS",  " FELONY ASSAULT", " GRAND LARCENY" ,
                " GRAND LARCENY OF MOTOR VEHICLE", " HARRASSMENT 2" , " MISCELLANEOUS PENAL LAW",
                " OFF. AGNST PUB ORD SENSBLTY &" ," OFFENSES INVOLVING FRAUD", " PETIT LARCENY",
                " ROBBERY", " VEHICLE AND TRAFFIC LAWS") # ," all")
                
# mean increase for significant crimes

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=dist_factor, y=coefficient, fill=stadium, ylim=c(5) )) +
  geom_bar(stat="identity", position=position_dodge()) + facet_wrap(~ crime, ncol=4) + xlab('Distance to stadium') + ylab('Coefficient') +
  ggtitle('Mean increase on gameday by crime \n (Crimes with all coefficents siginicative)')

#### Box plot ####

# Significant Crimes, split by distance and crime

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=stadium, y=coefficient, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + facet_wrap(~ crime, ncol=4) + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Distribution of mean increase on gameday by distance \n (Crimes with all coefficents siginicant)')

# Significant Crimes combineed, split by distance
ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=stadium, y=coefficient, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + facet_wrap(~ dist_factor, ncol=4) + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Distribution of mean increase on gameday by distance \n All crimes')

#Significant crimes combined
ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=stadium, y=coefficient, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Distribution of mean increase on gameday \n All crimes')


# Plots as percentage increase

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes,], aes(x=stadium, y=percent_coeff, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Percentage mean increase on gameday \n All crimes')

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=stadium, y=percent_coeff, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + facet_wrap(~ dist_factor, ncol=4) + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Percentage of mean increase on gameday by distance \n All crimes')

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=stadium, y=percent_coeff, fill=stadium, ylim=c(5) )) +
  geom_boxplot() + facet_wrap(~ crime, ncol=4) + xlab('Stadium') + ylab('Coefficient') +
  ggtitle('Percent mean increase on gameday by distance \n (Crimes with all coefficents siginicant)')

ggplot(data=d[ d$pValue <= .05 & d$crime %in% sig_crimes  ,], aes(x=dist_factor, y=percent_coeff, fill=stadium, ylim=c(5) )) +
  geom_bar(stat="identity", position=position_dodge()) + facet_wrap(~ crime, ncol=4) + xlab('Distance to stadium') + ylab('Coefficient') +
  ggtitle('Percent Mean increase on gameday by crime \n (Crimes with all coefficents siginicative)')

