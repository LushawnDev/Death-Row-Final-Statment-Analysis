# deathrow_SA.py does a sentiment analysis (I think...) on information from
# https://www.tdcj.texas.gov/death_row/dr_executed_offenders.html which contains the final words of
# encarcerated death row inmates within the Texas Department of Criminal Justice. SQL databases are also created for
# further analysis. :) It's a bit morbid...

import urllib.request
from bs4 import BeautifulSoup
import ssl
import re
import sqlite3

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Connects to SQL database and creates database handle
conn = sqlite3.connect("deathrowdata.sqlite")
cur = conn.cursor()

# Drops tables to reset them if required
# Analysis table is always reset
cur.execute("DROP TABLE IF EXISTS Analysis")
if input("Would you like to reset any existing death row data table? (y/n): ") == "y":
    cur.executescript("DROP TABLE IF EXISTS Statements; DROP TABLE IF EXISTS Offenders")

# Creates SQL databases for storing info
cur.executescript("""
    CREATE TABLE IF NOT EXISTS Statements (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        statement TEXT );
        
    CREATE TABLE IF NOT EXISTS Offenders (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT );
        
    CREATE TABLE IF NOT EXISTS Analysis (
        type TEXT UNIQUE,
        freq INTEGER )  
        """)

# Website is opened and parsed by beautiful soup to ensure data is cleaned up
html = urllib.request.urlopen("https://www.tdcj.texas.gov/death_row/dr_executed_offenders.html").read()
soup = BeautifulSoup(html, "html.parser")
print("Texas Department of Criminal Justice website has been received...")

print("Retrieving links for offenders' final statements...")
# Retrieves anchor using beautiful soup and assigns them to list
tags = soup("a")
# Using re the relevant "last" html links are found
lastlinks = list()
nostate = 0
for tag in tags:
    if "last" not in str(tag): continue    # Skips if irrelevant link
    if "no_last_statement" in str(tag): # Adds to tally of no last statement given
        nostate += 1
        continue
    # Remaining links should only be for "good" final statements
    lasturl = "https://www.tdcj.texas.gov/death_row/" + re.findall("(dr_info/.+last.+html)",str(tag))[0]
    lastlinks.append(lasturl)


# Checks if data table of statements already exists and thus skips accessing more links (very time-consuming)
cur.execute("SELECT * FROM Statements")
table_len = len(cur.fetchall())
# if table_len is None:
#     print("ERROR:  Could not find table length.")
if table_len == len(lastlinks):
    print("Database contains up to date statement data.")
# Last statement extraction begins if SQL database has not retrieved all the information yet
else: # table_len < len(lastlinks) or table_len is None:
    conn.commit()
    print("Accessing final statements and creating list of data...")
    # Cycles through links of last statements to extract data
    statements = list()
    for link in lastlinks:
        html = urllib.request.urlopen(link).read()
        soup = BeautifulSoup(html, "html.parser")

        text = soup("p")
        namepos = None
        for element in text:
            if "Last Statement" in element.get_text(): # Find position of last statement
                lastpos = text.index(element) + 1
                continue
            if "Offender" in element.get_text() and namepos is None:  # Find position of last statement
                namepos = text.index(element) + 1

        # Name of offender is assigned to a variable and added to SQL table
        name = text[namepos].getText()
        cur.execute("INSERT INTO Offenders (name) VALUES (?)", (name,))

        # All parts of last statement are sliced and concatenated together
        allstates = text[lastpos:]
        statement = ""
        for item in allstates: statement += item.get_text()
        statements.append(statement)
        cur.execute("INSERT INTO Statements (statement) VALUES (?)", (statement,))
    print("All statements received and recorded.")
conn.commit()

# JOIN statement if needed:
# cur.execute("""SELECT Statements.statement, Offenders.name INTO Full FROM Offenders JOIN Statements
#             ON Offenders.id == Statements.id  ORDER BY Offenders.name""")

### ANALYSIS STAGE BEGINS ###
# TODO Analysis Ideas: god/lord/allah/jesus, forgiveness, justice, love, family, hate, [expletive], peace, thanks, ...
#  crime denial? "I did not, innocent, justice, not guilty", sorry, None/declined
# Will need to put all in lowercase and possible other cleaning up of strings

# Counters for different analysis points are initialised:
analys = {"Religion":0, "Forgiveness":0, "Justice":0, "Love":0, "Own family":0, "Other's family":0, "Hate":0, "Swear":0,
   "Peace":0, "Gratitude":0, "Denial":0, "Sorry":0, "Hell":0, "No statement":nostate, "Total Statements":len(lastlinks)}
# Statements are selected from the SQL database and strings are read for keywords
cur.execute("SELECT statement FROM Statements")
for tup in cur:
    if "god" in tup[0].lower() or "allah" in tup[0].lower() or "jesus" in tup[0].lower() or "lord" in tup[0].lower() \
            or "muhammad" in tup[0].lower() or "heaven" in tup[0].lower():
        analys["Religion"] += 1
    if "forgive" in tup[0].lower():
        analys["Forgiveness"] += 1
    if "justice" in tup[0].lower():
        analys["Justice"] += 1
    if "love" in tup[0].lower():
        analys["Love"] += 1
    if "my family" in tup[0].lower():
        analys["Own family"] += 1
    if "your family" in tup[0].lower() or "families" in tup[0].lower():
        analys["Other's family"] += 1
    if "hate" in tup[0].lower() or "loathe" in tup[0].lower() or "detest" in tup[0].lower() \
            or "hatred" in tup[0].lower():
        analys["Hate"] += 1
    if "expletive" in tup[0].lower():
        analys["Swear"] += 1
    if "peace" in tup[0].lower():
        analys["Peace"] += 1
    if "thank" in tup[0].lower():
        analys["Gratitude"] += 1
    if "injustice" in tup[0].lower() or ("i did not" in tup[0].lower() and "i did not mean" not in tup[0].lower()) or \
            ("i didn't" in tup[0].lower() and "i didn't mean" not in tup[0].lower()) or \
             ("i did not" in tup[0].lower() and "i did not want" not in tup[0].lower()) or \
             ("i didn't" in tup[0].lower() and "i didn't want" not in tup[0].lower()) or \
               "innocent" in tup[0].lower() or "not guilty" in tup[0].lower():
        analys["Denial"] += 1
    if "sorry" in tup[0].lower() or "apologize" in tup[0].lower() or "apologise" in tup[0].lower():
        analys["Sorry"] += 1
    if "no statement" in tup[0].lower() or "no final statement" in tup[0].lower() or len(tup[0]) < 8:
        analys["No statement"] += 1
    if "hell" in tup[0].lower():
        analys["Hell"] += 1

# Results are outputted to Analysis SQL Table
for item in analys:
    cur.execute("INSERT OR REPLACE INTO Analysis (type, freq) VALUES (?,?)", (item, analys[item]))
print("Results outputted to SQL \"Analysis\" table.")
conn.commit()

conn.close()