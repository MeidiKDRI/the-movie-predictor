from pymongo import MongoClient
from tsv import TSV
from db import DB
import pprint
from cleaner import Cleaner

my_db = 'TMP'
my_coll = 'title.basics.compressed'

my_file = TSV('./data/title.basics.10000.tsv')
db = DB(my_db, my_coll)

liste = []

# Creation of a mongo database from a huge file
# Using Sequential reading

while True :
    lines = my_file.read_sequential(10000)
    
    if lines == '':
        break
    
    for line in lines :
        line = Cleaner().tconst_to_id(line)
        line = Cleaner().to_integer(line, 'isAdult')
        line = Cleaner().to_integer(line, 'startYear')
        line = Cleaner().replace_nan(line, 'startYear')
        line = Cleaner().to_integer(line, 'endYear')
        line = Cleaner().replace_nan(line, 'endYear')        
        line = Cleaner().to_integer(line, 'runtimeMinutes')
        line = Cleaner().replace_nan(line, 'runtimeMinutes')        
        line = Cleaner().spliter(line, 'genres')
        liste.append(line)
        
        if len(liste) >= 10 :
            db.bulk_insert(liste)        
            liste = []
    break
            
# mongoClient.close()
# End of function

# Search all datas filtered by a condition
'''
filter = ''
condition = ''

request = db.filt({filter : condition})

for x in request :
    pprint.pprint(x)
'''
# mongoClient.close()
# End of function

# Search an element with its _id
""" id = ''
element = db.find_element(id)
print(element) """
# End of function


