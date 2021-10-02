## Car Crash Case Study

To execute the package use spark-submit.sh script, add root path where data is stored.

To package execute: python setup.py bdist_egg

The Assignment is solved based on below logic:

 ### Analytics 1: 
 
 #### The number of crashes in which number of persons killed are Male:
 logic: in primary person csv, filter driver who are female and take unique count of crash id
 
### Analytics 2:
#### count of two wheelers booked for crashes

logic: in unit csv, count crash id which involved motorcycle vehicle type

### Analytics 3:

#### State has highest number of accidents in which females are involved

logic: in primary person file, filter drivers who are female and check crashes against state, and finding top state with maximum number of crash


### Analytics 4:

#### Top 5th to 15th VEH_MAKE_IDs that contribute to largest number of injuries including death

logic: in unit csv, sum up total injuries by using TOT_INJRY_CNT and DEATH_CNT
       group By on veh_make_id and sum up total injuries, selecting top 5th to 15th veh_make_id  

### Analytics 5:

#### top ethnic user group of each unique body style

logic: get ethnic user group information from person csv and body style from unit csv,
 join them on crash id, and with group by and count, selecting top ethnic group corresponding to body styles.

### Analytics 6:

#### Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash

logic: in person csv, filter where PRSN_ALC_RSLT_ID is positive and group by zip code, count unique number of crashes and 
selecting top 5 zip codes.

### Analytics 7:

#### Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~)is above 4 and car avails Insurance

logic: from damages csv, filter crash id where no property damage is observed and 
from unit csv, corresponding to above crash ids, check if VEH_DMAG_SCL > 4 and insurance is present 

### Analytics 8:

#### Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences

logic: from unit csv and charges csv, get 25 states where offences are in highest rate
from unit csv: get top 10 colours of vehicle
from person csv, get drivers who are licensed and use top 10 colour vehicle car, and car licensed in top 25 states
join this with unit csv, and group by-count on vehicle maker and get top 5 rows.
