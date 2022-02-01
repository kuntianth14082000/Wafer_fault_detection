import pandas as pd
import shutil
from os import listdir
import sqlite3
#print('imported...')
path = 'Training_FileFromDB/'
cv = "InputFile.csv"

#df = pd.read_csv(path+cv)
#print(df.shape)


import pymongo
client = pymongo.MongoClient("mongodb+srv://test:test@freecluster.efvve.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.test
tbl=db.emp
#tbl.insert_one({'name':'Satyajeet'})


"""
DatabaseName = "Training"
db=sqlite3.connect(path+DatabaseName+'.db')
c=db.cursor()
#cur = c.execute("SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
cur = c.execute("SELECT * from Good_Raw_Data")
for i in cur:
    print(i)"""
'''
#c.execute("create table tbl(name text,no INT)")
c.execute("insert into tbl values('kapila',2),('kunthal',1)")
cur = c.execute("SELECT * from tbl")
for i in cur:
    print(i)'''
#c.close()
#c.execute("insert into std values('kunthal',27,21,'karadaga','B.Tech')")

#conn = sqlite3.connect("Training_Database/Training.db")
#c = conn.cursor()
#c.execute("Show database")

