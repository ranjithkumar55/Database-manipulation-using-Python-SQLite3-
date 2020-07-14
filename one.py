import sqlite3
import pandas as pd
from sqlite3 import Error
#============================ create connection==========================
conn=sqlite3.connect(r"C:\sqlite\SQLite.db")
c=conn.cursor()

#=============================create Tables==========================================
c.execute('''CREATE TABLE `PastResults` (
  `Fixture_ID` integer,
  `Country_ID` integer,
  `Country_Name` integer,
  `League_ID` integer,
  `League_Name` string,
  `League_Type` string,
  `Is_Cup` boolean,
  `Season_ID` integer,
  `Season_Name` string,
  `Stage_ID` integer,
  `Round_ID` integer,
  `Group_ID` integer,
  `Date` datetime,
  `Time` datetime,
  `TimeZone` string,
  `Current_Season` boolean,
  `LocalTeam_ID` integer,
  `VisitorTeam_ID` integer,
  `Neutral_Venue` boolean,
  `LocalTeam_Score` integer,
  `VisitorTeam_Score` integer,
  `LocalTeam_Pen_Score` integer,
  `VisitorTeam_Pen_Score` integer,
  `HT_Score` string,
  `ET_Score` string,
  `PS_Score` string,
  `Leg` string,
  `Status` string
);
'''
)
c.execute('''CREATE TABLE `UpcomingMatches_Next24Hours` (
  `Fixture_ID` integer,
  `Country_ID` integer,
  `Country_Name` integer,
  `League_ID` integer,
  `League_Name` string,
  `League_Type` string,
  `Is_Cup` boolean,
  `Season_ID` integer,
  `Season_Name` string,
  `Current_Season` boolean,
  `Stage_ID` integer,
  `Round_ID` integer,
  `Group_ID` integer,
  `Date` datetime,
  `Time` datetime,
  `TimeZone` string,
  `LocalTeam_ID` integer,
  `VisitorTeam_ID` integer,
  `Neutral_Venue` boolean,
  `Leg` string,
  `Status` string
);
'''
)
#========================================= Read & Manipulate data====================================

read_data = pd.read_csv(r'C:\Users\Chellakkutty\Desktop\Football_Data.csv',index_col=0)
df=pd.DataFrame(read_data)
df=df.drop(['FT_Score'],axis=1)
FT_match=df[df['Status']=='FT']

FT_PEN_match=df[df['Status']=='FT_PEN']

AET_match=df[df['Status']=='AET']

past_results=pd.concat([FT_match,FT_PEN_match,AET_match])





df=df.drop(['HT_Score','ET_Score','Localteam_Score',
  'Visitorteam_Score',
  'Localteam_Pen_Score',
  'Visitorteam_Pen_Score',
  'PS_Score'],axis=1)

TBA_match=df[df['Status']=='TBA']

NS_match=df[df['Status']=='NS']
POSTP_match=df[df['Status']=='POSTP']
upcoming_matchs=pd.concat([TBA_match,NS_match,POSTP_match])

start_date = "2020-07-06"
end_date = "2020-07-07"

after_start_date = upcoming_matchs["Date"] >= start_date
before_end_date = upcoming_matchs["Date"] <= end_date
between_two_dates = after_start_date & before_end_date
filtered_dates = upcoming_matchs.loc[between_two_dates]

start_time = "00:00:00"
end_time = "16:00:00"

after_start_time = filtered_dates["Time"] >= start_time
before_end_time = filtered_dates["Time"] <= end_time
between_two_time = after_start_time & before_end_time
upcoming_results = filtered_dates.loc[between_two_time]
upcoming_results.rename(columns = {'Time_Zone':'TimeZone'}, inplace = True)
past_results.rename(columns = {'Time_Zone':'TimeZone'}, inplace = True)



#============================= Insert data into tables================================================




upcoming_results.to_sql('UpcomingMatches_Next24Hours1',conn, if_exists='append', index = False)

past_results.to_sql('PastResults2',conn, if_exists='append', index = False)

#============================= displaying data=======================================================

c.execute('''
SELECT DISTINCT *
FROM PastResults2''')

Past_results_df = pd.DataFrame(c.fetchall())
print (Past_results_df)

c.execute('''
SELECT DISTINCT *
FROM UpcomingMatches_Next24Hours1''')

upcoming_results_df = pd.DataFrame(c.fetchall())
print (upcoming_results_df)


