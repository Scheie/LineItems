import csv
import sqlite3
import pandas as pd

connection = sqlite3.connect('sqlite.db')
connection.execute("DROP TABLE IF EXISTS everything")

location = r'C:\Users\Marcus\Desktop\Project\data_case.csv'
reader = pd.read_csv(location)

c = connection.cursor()
c.execute("DROP TABLE IF EXISTS kampanje")
c.execute("DROP TABLE IF EXISTS linje")
c.execute("DROP TABLE IF EXISTS stats")

c.execute(''' CREATE TABLE if not exists kampanje
                (CampaignID int PRIMARY KEY not null,
                Campaign text,
                CampaignStartDate datetime,
                CampaignEndDate datetime)''')

c.execute(''' CREATE TABLE if not exists linje
                (LineItemID int PRIMARY KEY not null,
                LineItem text,
                LineItemStartDate datetime,
                LineItemEndDate datetime)''')

c.execute(''' CREATE TABLE if not exists stats
                (CampaignID int not null,
                LineItemID int not null,
                Impressions int,
                Clicks int,
                Cost float,
                PRIMARY KEY (CampaignID, LineItemID))''')

reader.to_sql('everything', connection)
dataframe = pd.io.sql.read_sql("Select * from everything", connection)

c.execute("""INSERT INTO kampanje (CampaignID, Campaign, CampaignStartDate, CampaignEndDate)
SELECT distinct "Campaign ID", Campaign, "Campaign Start Date", "Campaign End Date"
FROM   everything""")

c.execute("""INSERT INTO linje (LineItemID, LineItem, LineItemStartDate, LineItemEndDate)
SELECT distinct "Line Item ID", "Line Item", "Line Item Start Date", "Line Item End Date"
FROM   everything""")

c.execute("""INSERT INTO stats (CampaignID, LineItemID, Impressions, Clicks, Cost)
SELECT distinct "Campaign ID", "Line Item ID", "Impressions", "Clicks", "Cost"
FROM   everything""")

df = pd.read_sql("""select k.Campaign, k.CampaignID, k.CampaignStartDate, k.CampaignEndDate, l.LineItem, l. LineItemID, l.LineItemEndDate, l.LineItemStartDate, s.Cost, s.Impressions, s.Clicks
from kampanje k, linje l, stats s
where k.CampaignID = s.CampaignID and l.LineItemID = s.LineItemID""", connection)

df.to_csv('output2.csv', index = True)

connection.commit()
connection.close()
