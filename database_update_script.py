############# REQUISITES #############

# Install Neo4j 

# Get TKinter set on System
# TKinter doesn't agree with Python 3.8.3, so we're using 3.7.4
# https://github.com/pyenv/pyenv/issues/1375 was helpful, steps outlined below
# 1. brew install pyenv
# 2. brew install tcl-tk
# 3. search for python-build executable, mine was in '/usr/local/Cellar/pyenv/1.2.20/plugins/python-build/bin/'
# 4. Once you have the script file open, search for:
# $CONFIGURE_OPTS ${!PACKAGE_CONFIGURE_OPTS} "${!PACKAGE_CONFIGURE_OPTS_ARRAY}" || return 1
# and replace with:
# $CONFIGURE_OPTS --with-tcltk-includes='-I/usr/local/opt/tcl-tk/include' --with-tcltk-libs='-L/usr/local/opt/tcl-tk/lib -ltcl8.6 -ltk8.6' ${!PACKAGE_CONFIGURE_OPTS} "${!PACKAGE_CONFIGURE_OPTS_ARRAY}" || return 1
# 5. pyenv install 3.7.4
# 6. pyenv global 3.7.4
# 7. pyenv version (to check)
# 8. add 'if command -v pyenv 1>/dev/null 2>&1; then
#   eval "$(pyenv init -)"
# fi' 
# to .bashrc/.bash_profile and reload shell
# 9. test with
# python -m tkinter -c 'tkinter._test()'
# should pop open a gui window

# Create Neo4j Database
# Install APOC & GDSL Plug-ins
# Change Settings
    # dbms.memory.heap.initial_size=1m
    # dbms.memory.heap.max_size=3G
# Start up database

# Manually add CSV files to database's 'import' folder

# Create 'python' folder under database folder
    # Add provided requirements.txt and database_update_script.py (this file) to python folder
    # Set up a virtualenv in this python folder
        # pip install virtualenv
        # virtualenv --version (just to check)
        # virtualenv venv
        # source venv/bin/activate
        # pip install -r requirements.txt

# Now ready to run the script!
# Make sure to deactivate venv after running is complete
# Can change pyenv global back to 3.8.3 or other version after script is ran

from bs4 import BeautifulSoup
import requests
import re
import os
import sys
from py2neo import Graph
import PySimpleGUI as gui
import time

############# SIMPLE GUI TO TAKE BASIC ARGUMENTS #############
############# PRESENTED AT BEGINNING OF SCRIPT ONLY #############

assumed_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'import')) #assume path but allow changes
standard_font = 'Courier', 16

layout = [  [gui.Text('Database Port', font=(standard_font)), gui.InputText('7687', font=(standard_font))],
            [gui.Text('Database Password', font=(standard_font)), gui.InputText(font=(standard_font))], 
            [gui.Text('Path to Import Folder', font=(standard_font)), gui.InputText(assumed_path, font=(standard_font))],
            [gui.Button('Update Database', font=(standard_font)), gui.Button('Cancel', font=(standard_font))] ]
window = gui.Window('Database Details', layout)
while True:
    event, inputs = window.read()
    # close program if input window is closed
    if event == gui.WIN_CLOSED or event == 'Cancel':
        quit()
    # before updating, make sure to have the inputs you need - otherwise, send message and don't proceed
    if event == 'Update Database': 
        if inputs[0] and inputs[1] and inputs[2]:
            port = inputs[0]
            pswd = inputs[1]
            path = inputs[2]
            window.close()
            break
        else:
            # identify missing info
            if not inputs[0]:
                missing = 'Port'
            elif not inputs[1]:
                missing = 'Password'
            elif not inputs[2]:
                missing = 'Path'
            # present alert box with the message
            alert = gui.Window(' ', [[gui.Text('Missing ' + missing, font=(standard_font))]])
            if alert.read() == gui.WIN_CLOSED:
                alert.close()

############# SIMPLE GUI TO DISPLAY PROGRESS UPDATES #############
############# INVOKED THROUGHOUT SCRIPT #############

def update(string=''):
    print(string)
    # present message for a few seconds then auto-close
    window = gui.Window('Progress Update', [[gui.Text(string, font=(standard_font))]]).Finalize()
    time.sleep(3)
    window.close()

############# IMPORT UPDATED .TXT FILES FROM ONET DATABASE #############
total_program_time_start = time.perf_counter() # start timer to log total program time
update('Recieved details, starting updates.')

# make sure that import folder under neo4j database exists
if not os.path.exists(path):
    update('ERROR: Database import folder is required to update the database, but does not exist.')
    quit()
else:
    update('SUCCESS: Database import folder exists, proceeding with file imports.')

# validate url, then process html doc from url
url = 'https://www.onetcenter.org/database.html#all-files'
try:
    r = requests.get(url)
except requests.exceptions.RequestException as e:
    update('ERROR: Could not access ONET Database URL. See console for exception details.')
    raise SystemExit(e)
soup = BeautifulSoup(r.text, 'html.parser')
update('SUCCESS: Accessed ONET Database, starting to import files.')

file_count = 1
file_import_time_start = time.perf_counter() # start timer to log total file import time
# file importing status box initialization
layout = [  [gui.Text('IMPORTING FILES', font=(standard_font))],
            [gui.Text('Total Files Imported: ', size=(50, 1), font=(standard_font), key='files_imported')], 
            [gui.Text('Most Recent File Imported: ', size=(50, 1), font=(standard_font), key='recent_file')]]
window = gui.Window('Progress Updates', layout, finalize=True)

# scrape page for all links
for link in soup.find_all('a'):
    # if it's an href attribute & contains .txt, consider it
    if 'href' in link.attrs:
        if '.txt' in link.attrs['href']:
            # extract the specific file_url and create a sanitized file_name from it
            file_url = link.attrs['href']
            file_name = re.search('[^/]+$', file_url).group(0)
            file_name = file_name.replace('%20', '')
            file_name = file_name.replace('%2C', '')
            # get the html of that specific .txt file page (aka file contents) by appending to the base ONET url
            response = requests.get('https://www.onetcenter.org' + file_url)
            # write contents to a file
            open(os.path.join(path, file_name), 'wb').write(response.content)
            # logging
            print('Total Files Imported: ' + str(file_count))
            print('Most Recent File Imported: ' + file_name)
            window.read(timeout=0.1) #timeout was the make or break piece
            window['files_imported'].update('Total Files Imported: ' + str(file_count))
            window['recent_file'].update('Most Recent File Imported: ' + file_name)
            file_count += 1
            

file_import_time_stop = time.perf_counter() # stop timer to log total file import time
if window.read() == gui.WIN_CLOSED:
    window.close()
update('Completed importing .txt files from ONET database.')

############# CONNECT TO DATABASE #############

# Make sure the database is started first, otherwise attempt to connect will fail
try:
    graph = Graph('bolt://localhost:'+port, auth=('neo4j', pswd))
    update('SUCCESS: Connected to the Neo4j Database. Starting cypher queries to create database; please do not interrupt process during runtime. This may take more than an hour; completion will be confirmed by a message to the console.')
    total_queries_time_start = time.perf_counter() # start timer to log total query time
except Exception as e:
    update('ERROR: Could not connect to the Neo4j Database. See console for details.')
    raise SystemExit(e)

############# APPEND THE QUERIES TO QUERY_ LIST #############

query_list = []

query_list.append("""LOAD CSV WITH HEADERS 
FROM 'file:///OccupationData.txt' AS line FIELDTERMINATOR '	'
WITH line
Limit 1
RETURN line""")


# TODO need to add constraints, this is example only
query_list.append("""CREATE CONSTRAINT ON (occupation:Occupation) ASSERT occupation.onet_soc_code IS UNIQUE;""")
query_list.append("""CREATE CONSTRAINT ON (element:Element ) ASSERT element.ElementID IS UNIQUE;""")
query_list.append("""CREATE CONSTRAINT ON (occupation:MajorGroup) ASSERT occupation.onet_soc_code IS UNIQUE;""")

# Load.
# Import Occupation Data, known as SOC Level
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS 
FROM 'file:///OccupationData.txt' AS line FIELDTERMINATOR '	'
MERGE (occupation:Occupation { onet_soc_code: line.`O*NET-SOC Code`} )
ON CREATE SET occupation.title = toLower(line.Title),
            occupation.description = toLower(line.Description),
            occupation.source = 'ONET'
RETURN count(occupation)
;""")

# Load the reference model for the elements
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ContentModelReference.txt' AS line FIELDTERMINATOR '	'
MERGE (element:Element {elementID: line.`Element ID`})
ON CREATE SET element.title = toLower(line.`Element Name`),
            element.description = toLower(line.Description),
            element.source = 'ONET'
RETURN count(element)
;""")

# Load the relationships of the reference model. Self created
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///content_model_relationships.csv' AS line

MATCH (a:Element), (b:Element) 
WHERE a.elementID = line.From AND b.elementID = line.To AND a.elementID <> b.elementID
MERGE (a)<-[r:Sub_Element_Of]-(b)
RETURN count(r)
;""")

# Remove Element label and add Worker Characteristics & Ability Label to Remaining
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '1'
SET n:Worker_Characteristics
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('1.A')
SET a:Abilities
REMOVE a:Element
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('1.B')
SET b:Interests
REMOVE b:Element
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('1.C')
SET c:Work_Styles
REMOVE c:Element
;""")

# Worker Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '2'
SET n:Worker_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('2.A')
SET a:Basic_Skills
REMOVE a:Element
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('2.B')
SET b:Cross_Functional_Skills
REMOVE b:Element
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('2.C')
SET c:Knowledge
REMOVE c:Element
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('2.D')
SET d:Education
REMOVE d:Element
;""")

# Experience Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '3'
SET n:Experience_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('3.A')
SET a:Experience_And_Training
REMOVE a:Element
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('3.B')
SET b:Basic_Skills_Entry_Requirement
REMOVE b:Element
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('3.C')
SET c:Cross_Functional_Skills_Entry_Requirement
REMOVE c:Element
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('3.D')
SET d:Licensing
REMOVE d:Element
;""")

# Occupational Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '4'
SET n:Occupational_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('4.A')
SET a:Generalized_Work_Activities
REMOVE a:Element
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('4.B')
SET b:Organizational_Context
REMOVE b:Element
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('4.C')
SET c:Work_Context
REMOVE c:Element
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('4.D')
SET d:Detailed_Work_Activities
REMOVE d:Element
;""")
query_list.append("""MATCH (e:Element)
WHERE e.elementID CONTAINS('4.E')
SET e:Intermediate_Work_Activities
REMOVE e:Element
;""")

# Occupation Specific Information
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '5'
SET n:Occupation_Specific_Information
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('5.A')
SET a:Task
REMOVE a:Element
;""")
# There is no 5.B
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('5.C')
SET c:Title
REMOVE c:Element
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('5.D')
SET d:Description
REMOVE d:Element
;""")
query_list.append("""MATCH (e:Element)
WHERE e.elementID CONTAINS('5.E')
SET e:Alternate_Titles
REMOVE e:Element
;""")
query_list.append("""MATCH (f:Element)
WHERE f.elementID CONTAINS('5.F')
SET f:Technology_Skills
REMOVE f:Element
;""")
query_list.append("""MATCH (g:Element)
WHERE g.elementID CONTAINS('5.G')
SET g:Tools
REMOVE g:Element
;""")

# Workforce Characteristics
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '6'
SET n:Workforce_Characteristics
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('6.A')
SET a:Labor_Market_Information
REMOVE a:Element
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('6.B')
SET b:Occupational_Outlook
REMOVE b:Element
;""")

# Load The SOC Major Group Occupation, Change label to MajorGroup
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///SOCMajorGroup.csv' AS line
MERGE (occupation:MajorGroup { onet_soc_code: line.SOCMajorGroupCode})
ON CREATE SET occupation.title = toLower(line.SOCMajorGroupTitle),
            occupation.source = 'ONET'
;""")

# Load SOC Level with Detail Occupations, Change label
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_With_Detailed.csv' AS line
MERGE (occupation:Occupation { onet_soc_code: line.SOCLevelCode})
ON CREATE SET occupation.title = toLower(line.SOCLevelTitle),
            occupation.description = toLower(line.SOCLevelDescription),
            occupation.source = 'ONET'
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_With_Detailed.csv' AS line
MATCH (a:MajorGroup), (b:Occupation)
WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Major_Group]-(b)
;""")

# Load SOC Level without Detail Occupations, Change label
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_Without_Detailed.csv' AS line
MERGE (occupation:Occupation { onet_soc_code: line.SOCLevelCode})
ON CREATE SET occupation.title = toLower(line.SOCLevelTitle),
            occupation.description = toLower(line.SOCLevelDescription),
            occupation.source = 'ONET'
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_Without_Detailed.csv' AS line
MATCH (a:MajorGroup), (b:Occupation)
WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Major_Group]-(b)
;""")

# Load Detailed Occupations, Change label to Detailed Occupation or something else
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///DetailedOccupation.csv' AS line
MERGE (occupation:Workrole { onet_soc_code: line.SOCDetailCode})
ON CREATE SET occupation.title = toLower(line.SOCDetailTitle),
            occupation.description = toLower(line.SOCDetailDescription),
            occupation.source = 'ONET'
RETURN count(occupation)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///DetailedOccupation.csv' AS line
MATCH (a:Occupation), (b:Workrole)
WHERE a.onet_soc_code = line.SOCLevelCode AND b.onet_soc_code = line.SOCDetailCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Occupation]-(b)
;""")

# Create Scale Nodes. Each element will have an edge to a scale with
# associated statistical measures
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
From 'file:///ScalesReference.txt' AS line FIELDTERMINATOR '	'
MERGE (scale:Scale {scaleId: line.`Scale ID`})
ON CREATE SET scale.title = toLower(line.`Scale Name`),
            scale.min = toInteger(line.Minimum),
            scale.max = toInteger(line.Maximum)
;""")

# The following section creates the relationships between the Elements and the Occuptions
# Elements include abilities, knowledge, skills, and work activities
# Remove Element label here, add property of source of data = ONET, remove ONET_XXXXX label for all below
# Load Abilities
# Add relationships to Occupation and Workrole

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Abilities.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Abilities {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'ability'},  a) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Abilities.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Abilities {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'ability'},  a) YIELD rel
RETURN count(rel)
;""")

# TAKES A WHILE
# Add Alternative titles for Occupations and Workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///AlternateTitles.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MERGE (t:AlternateTitles {title: line.`Alternate Title`,
    shorttitle: line.`Short Title`, source: line.`Source(s)`})
WITH a, t, line
CALL apoc.create.relationship(a, 'Equivalent_To', {}, t) YIELD rel
RETURN count(rel)
;""")

# Trying Match to see if the properties are not removed.
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///AlternateTitles.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MERGE (t:AlternateTitles {title: line.`Alternate Title`,
    shorttitle: line.`Short Title`, source: line.`Source(s)`})
WITH a, t, line
CALL apoc.create.relationship(a, 'Equivalent_To', {}, t) YIELD rel
RETURN count(rel)
;""")

# Add IWA and DWA to Generalized Work Activities
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///IWAReference.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Generalized_Work_Activities {elementID: line.`Element ID`})
MERGE (b:Generalized_Work_Activities {elementID: line.`IWA ID`, title: line.`IWA Title`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Sub_Element_Of', {type: 'IWA'}, a) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///DWAReference.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Generalized_Work_Activities {elementID: line.`IWA ID`})
MERGE (b:Generalized_Work_Activities {elementID: line.`DWA ID`, title: line.`DWA Title`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Sub_Element_Of', {type: 'DWA'}, a) YIELD rel
RETURN count(rel)
;""")

# Add Education, Experience and Training relationships and measures
# to Occupationa and workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///EducationTrainingandExperience.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Education {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'education'},  a) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///EducationTrainingandExperience.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Experience_And_Training {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'experience'},  a) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///EducationTrainingandExperience.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Education {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'education'},  a) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///EducationTrainingandExperience.txt' AS line FIELDTERMINATOR '	'
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Experience_And_Training {elementID: line.`Element ID`})
WITH a, b, line
CALL apoc.create.relationship(b, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'experience'},  a) YIELD rel
RETURN count(rel)
;""")

# Interests
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Interests.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (i:Interests {elementID: line.`Element ID`})
WITH o, i, line
CALL apoc.create.relationship(i, 'Found_In', {datavalue: toFloat(line.`Data Value`),
    scale: line.`Scale ID`,
    element: 'interest'}, o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Interests.txt' AS line FIELDTERMINATOR '	'
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (i:Interests {elementID: line.`Element ID`})
WITH w, i, line
CALL apoc.create.relationship(i, 'Found_In', {datavalue: toFloat(line.`Data Value`),
    scale: line.`Scale ID`,
    element: 'interest'}, w) YIELD rel
RETURN count(rel)
;""")

# Job Zones
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///JobZoneReference.txt' AS line FIELDTERMINATOR '	'
MERGE (j:JobZone {jobzone: toInteger(line.`Job Zone`)})
ON CREATE SET j.name = toLower(line.Name),
    j.experience = toLower(line.Experience),
    j.education = toLower(line.Education),
    j.training = toLower(line.`Job Training`),
    j.example = toLower(line.Examples),
    j.svpRange = line.`SVP Range`
RETURN count(j)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///JobZones.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (j:JobZone {jobzone: toInteger(line.`Job Zone`)})
WITH o, j, line
CALL apoc.create.relationship(o, 'In_Job_Zone', {jobzone: line.`Job Zone`,
    date: line.Date}, j) YIELD rel
RETURN count(rel)
;""")

# Knowledge
# Add relationships to Occupation and Workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Knowledge.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (k:Knowledge {elementID: line.`Element ID`})
WITH o, k, line
CALL apoc.create.relationship(k, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'knowledge'},  o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Knowledge.txt' AS line FIELDTERMINATOR '	'
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (k:Knowledge {elementID: line.`Element ID`})
WITH w, k, line
CALL apoc.create.relationship(k, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'knowledge'},  w) YIELD rel
RETURN count(rel)
;""")

# Skills
# Add relationships to Occupation and Workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Skills.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Basic_Skills {elementID: line.`Element ID`})
WITH o, s, line
CALL apoc.create.relationship(s, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'basic_skill'},  o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Skills.txt' AS line FIELDTERMINATOR '	'
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Basic_Skills {elementID: line.`Element ID`})
WITH w, s, line
CALL apoc.create.relationship(s, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'basic_skill'},  w) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Skills.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Cross_Functional_Skills {elementID: line.`Element ID`})
WITH o, s, line
CALL apoc.create.relationship(s, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'cf_skill'},  o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///Skills.txt' AS line FIELDTERMINATOR '	'
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Cross_Functional_Skills {elementID: line.`Element ID`})
WITH w, s, line
CALL apoc.create.relationship(s, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'cf_skill'},  w) YIELD rel
RETURN count(rel)
;""")

# This sections will add task and their statements as nodes and create relationships to occupations.
# Add relationships to Occupation and Workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TaskStatements.txt' AS line FIELDTERMINATOR '	'
MERGE (task:Task { taskID: toInteger(line.`Task ID`)})
ON CREATE SET task.description = toLower(line.Task),
            task.tasktype = toLower(line.`Task Type`),
            task.incumbentsresponding = line.`Incumbents Responding`,
            task.date = line.Date,
            task.domainsource = line.`Domain Source`,
            task.source = 'ONET'
RETURN count(task)
;""")

# TAKES A WHILE
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TaskRatings.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
WITH o, task, line
CALL apoc.create.relationship(task, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'task'},  o) YIELD rel
RETURN count(rel)
;""")

# TAKES A WHILE
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TaskRatings.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
WITH o, task, line
CALL apoc.create.relationship(task, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'task'},  o) YIELD rel
RETURN count(rel)
;""")

# TAKES A WHILE
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TaskstoDWAs.txt' AS line FIELDTERMINATOR '	'
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
MATCH (a:Generalized_Work_Activities {elementID: line.`DWA ID`})
WITH a, task, line
CALL apoc.create.relationship(task, 'Task_For_DWA', {date: line.Date, domainsource: line.`Domain Source`}, a) YIELD rel
RETURN count(rel)
;""")

# Commodities, to include tools and tech
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///UNSPSCReference.txt' AS line FIELDTERMINATOR '	'
MERGE (s:Segement {segmentID: toInteger(line.`Segment Code`), title: toLower(line.`Segment Title`)})
MERGE (f:Family {familyID: toInteger(line.`Family Code`), title: toLower(line.`Family Title`)})
MERGE (c:Class { classID: toInteger(line.`Class Code`), title: toLower(line.`Class Title`)})
MERGE (m:Commodity {commodityID: toInteger(line.`Commodity Code`), title: toLower(line.`Commodity Title`)})
MERGE (s)<-[r:Sub_Segment]-(f)
MERGE (f)<-[a:Sub_Segment]-(c)
MERGE (c)<-[b:Sub_Segment]-(m)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TechnologySkills.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (t:Technology_Skills {elementID: '5.F.1'})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
SET m:Technology_Skills
REMOVE m:Commodity
MERGE (m)-[r:Sub_Element_Of]-(t)
WITH o, m, line
CALL apoc.create.relationship(m, 'Technology_Used_In', {example: line.Example, hottech: line.`Hot Technology`}, o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///TechnologySkills.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Technology_Skills {commodityID: toInteger(line.`Commodity Code`)})
WITH o, m, line
CALL apoc.create.relationship(m, 'Technology_Used_In', {example: line.Example, hottech: line.`Hot Technology`}, o) YIELD rel
RETURN count(rel)
;""")

# Tools
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ToolsUsed.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (t:Tools {elementID: '5.G.1'})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
SET m:Tools
REMOVE m:Commodity
MERGE (m)-[r:Sub_Element_Of]-(t)
WITH o, m, line
CALL apoc.create.relationship(m, 'Tools_Used_In', {example: line.Example}, o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ToolsUsed.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Tools {commodityID: toInteger(line.`Commodity Code`)})
WITH o, m, line
CALL apoc.create.relationship(m, 'Tools_Used_In', {example: line.Example}, o) YIELD rel
RETURN count(rel)
;""")

# Activities
# Add relationships to Occupation and Workrole
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///WorkActivities.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Generalized_Work_Activities { elementID: line.`Element ID`})
WITH o, a, line
CALL apoc.create.relationship(a, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'activity'},  o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///WorkActivities.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Generalized_Work_Activities { elementID: line.`Element ID`})
WITH o, a, line
CALL apoc.create.relationship(a, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'activity'}, o) YIELD rel
RETURN count(rel)
;""")

# Work Styles
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///WorkStyles.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Work_Styles { elementID: line.`Element ID`})
WITH o, a, line
CALL apoc.create.relationship(a, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'work_style'},  o) YIELD rel
RETURN count(rel)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///WorkStyles.txt' AS line FIELDTERMINATOR '	'
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Work_Styles { elementID: line.`Element ID`})
WITH o, a, line
CALL apoc.create.relationship(o, 'Found_In', {datavalue: toFloat(line.`Data Value`),
     scale: line.`Scale ID`,
     element: 'work_style'},  a) YIELD rel
RETURN count(rel)
;""")

############# NCC OPM Crosswalk #############
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
MERGE (ncc:NASAClassCode {ncc_class_code: line.`NASA Class Code`})
ON CREATE SET ncc.title = toLower(line.`NASA Specialty Title`)

MERGE (nccgrp:NCCGroup {ncc_grp_num: line.`NCC GRP NUM`})
ON CREATE SET nccgrp.title = toLower(line.`NCC Group`)

MERGE (opm:OPMSeries {series: line.`OPMSeries`})
ON CREATE SET opm.title = line.`OPM Series Title`

MERGE (smg:SkillMixGrp { title: line.`Skill Mix Group`})

MERGE (opm)-[r1:IN_NCC_Class]->(ncc)
MERGE (ncc)-[r:IN_NCC_GRP]->(nccgrp)
MERGE (nccgrp)-[r2:IN_Skill_Mix_Grp]->(smg)

;""")

# OPM Series to ONET crosswalk
query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
MATCH (occ:Occupation), (opm:OPMSeries)
WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
RETURN count(r)
;""")

query_list.append("""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
MATCH (occ:Workrole), (opm:OPMSeries)
WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
RETURN count(r)""")


# For a specific SOC
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code = '17-2071.00' AND opm.series CONTAINS("855")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm)
RETURN count(r)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code = '17-2072.00' AND opm.series CONTAINS("855")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm)
RETURN count(r)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code CONTAINS('17-206') AND opm.series CONTAINS("854")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1400', censustitle: toLower('COMPUTER HARDWARE ENGINEERS')}]->(opm)
RETURN count(r)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code CONTAINS('15-1111') AND opm.series CONTAINS("1550")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1005', censustitle: toLower('COMPUTER & INFORMATION RESEARCH SCIENTISTS')}]->(opm)
RETURN count(r)
;""")

query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code CONTAINS("15-1") AND opm.series CONTAINS("2210")
WITH o, opm
CALL apoc.create.relationship(o, 'IN_OPM_Series', {censuscode: '1050',
     censustitle: toLower('COMPUTER SUPPORT SPECIALISTS')
     }, opm) YIELD rel
RETURN count(rel)
;""")

# # NOT TESTED BECAUSE I DON'T HAVE EMPLOYEE DATA YET
# ############# Employees #############

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///Employees_2020-05-28.csv' AS line
# MERGE (emp:Employee {uupic: line.UUPIC})
# ON CREATE SET emp.fname = line.`Name First`,
# 			emp.minitial = line.`Name Middle`,
# 			emp.lname = line.`Name Last`,
# 			emp.date_position = line.`Date Entered Current Position`,
# 			emp.email = toLower(line.`Email Address Work`),
# 			emp.age = toInteger(line.`Employee Age in Yrs`),
# 			emp.status = line.`Employee Status`,
# 			emp.type = line.`Employee Type`,
# 			emp.grade = line.Grade,
# 			emp.service_years = line.`Years of Service - Federal`,
# 			emp.accession = line.`Date Accession` // you need to change format to YYYY-MM-DD

# MERGE (center:Center { center: line.Center})

# MERGE (org:Organizations { org_code: line.`Organization Code`})
# On CREATE SET org.title = line.`Organization Title`

# MERGE (map:MapOrganization {map_org: line.`Map Organization Code`})

# MERGE (emp)-[:Located_At]->(center)
# MERGE (emp)-[:In_Organization]->(org)
# MERGE (org)-[:In_MAP]->(map)
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///Employees_2020-05-27.csv' AS line
# MATCH (emp:Employee), (opm:OPMSeries)
# WHERE emp.uupic = line.UUPIC and opm.series CONTAINS(line.`Occupational Series`)
# MERGE (emp)-[:IN_OPM_Series]->(opm)
# ;""")

# # Map Elements to Employees
# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementAbilities.csv' AS line
# MATCH (emp:Employee), (elem:Abilities)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.Abilities
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "ability"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementBasicSkills.csv' AS line
# MATCH (emp:Employee), (elem:Basic_Skills)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.BasicSkills
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "basic_skill"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementCrossFunctionalSkills.csv' AS line
# MATCH (emp:Employee), (elem:Cross_Functional_Skills)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.CrossFunctionalSkills
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "cf_skill"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementKnowledge.csv' AS line
# MATCH (emp:Employee), (elem:Knowledge)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.Knowledge
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "knowledge"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementTasks.csv' AS line
# MATCH (emp:Employee), (elem:Task)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.Tasks
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "task"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementTechSkills.csv' AS line
# MATCH (emp:Employee), (elem:Technology_Skills)
# WHERE emp.uupic = line.UUPIC AND elem.title = line.TechSkills
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "tech_skill"
# ;""")

# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///elementWorkActivities.csv' AS line
# MATCH (emp:Employee), (elem:Generalized_Work_Activities)
# WHERE emp.uupic = line.UUPIC AND elem.description = line.WorkActivities
# MERGE (emp)-[f:Found_In]->(elem)
# SET f.datavalue = toFloat(2.5),
# 	f.scale = "IM",
# 	f.element = "activity"
# ;""")

# # Update Center Inforation
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "HQ"
# SET c.title = "Headquarters",
# 	c.business_area = toInteger(10)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "OIG"
# SET c.title = "Office of the Inspector General",
# 	c.business_area = toInteger(99)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "NSSC"
# SET c.title = "NASA Shared Services Center",
# 	c.business_area = toInteger(99)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "ARC"
# SET c.title = "Ames Research Center",
# 	c.business_area = toInteger(21)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "GRC"
# SET c.title = "Glenn Research Center",
# 	c.business_area = toInteger(22)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "LARC"
# SET c.title = "Langley Research Center",
# 	c.business_area = toInteger(23)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "AFRC"
# SET c.title = "Armstrong Filght Research Center",
# 	c.business_area = toInteger(24)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "GSFC"
# SET c.title = "Goddard Space Flight Center",
# 	c.business_area = toInteger(51)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "MSFC"
# SET c.title = "Marshall Space Flight Center",
# 	c.business_area = toInteger(62)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "SSC"
# SET c.title = "Stennis Space Center",
# 	c.business_area = toInteger(64)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "JSC"
# SET c.title = "Johnson Space Center",
# 	c.business_area = toInteger(72)
# ;""")
# query_list.append("""MATCH (c:Center)
# WHERE c.center = "KSC"
# SET c.title = "Kennedy Space Center",
# 	c.business_area = toInteger(74)
# ;""")
# # ADD Mission, Theme, Program, Project, Cost Center From ALDS info
# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///FTE2020.csv' AS line
# MATCH (emp:Employee)
# WHERE emp.uupic = line.UUPIC 

# MERGE (mis:Mission {acronym: line.`Mission/Mission Equivalent`})
# ON CREATE SET mis.title = line.`Mission Equivalent`

# MERGE (theme:Theme {acronym: line.`Theme/Theme  Equivalent`})
# ON CREATE SET theme.title = line.`Theme  Equivalent`

# MERGE (program:Program {program_code: line.`Program/Program Equivalent`})
# ON CREATE SET program.title = line.`Program Equivalent`

# MERGE (project:Project {project_code: line.`Project/Project  Equivalent`})
# ON CREATE SET project.title = line.`Project  Equivalent`

# MERGE (cost:Cost_Center {cost_code: line.`Cost Center`})
# ON CREATE SET cost.title = line.`Cost Center Equivalent`

# MERGE (emp)-[:Charged_To]->(cost)
# MERGE (cost)-[:Charged_To]->(project)
# MERGE (project)-[:Charged_To]->(program)
# MERGE (program)-[:Charged_To]->(theme)
# MERGE (theme)-[:Charged_To]->(mis)
# ;""")

# # Add NASA Competency Library
# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///NASACompetencyLibrary.csv' AS line

# MERGE (comptype:CompetencyType {prefix: line.Prefix})
# ON CREATE SET comptype.title = line.comptype,
# comptype.source = "NASA"

# MERGE (compsuite:CompetencySuite)
# ON CREATE SET compsuite.title = line.CompSuite,
# compsuite.source = "NASA" 

# MERGE (compdesg:CompetencyDesignation {acronym: line.CompDesg})
# ON CREATE SET compdesg.source = "NASA"

# MERGE (comp:Competency {compid: toInteger(line.CompID)})
# ON CREATE SET comp.title = line.CompTitle,
# comp.description = line.CompDescription,
# comp.source = "NASA"

# MERGE (compsuite)-[:In_Comp_Type]->(comptype)
# MERGE (compdesg)-[:In_Comp_Suite]->(compsuite)
# MERGE (comp)-[:Has_Comp_Desgination]->(compdesg)
# ;""")


# query_list.append("""USING PERIODIC COMMIT
# LOAD CSV WITH HEADERS
# FROM 'file:///OPMCompetencyLibrary.csv' AS line

# MERGE (comp:Competency {compid: toInteger(line.id)})
# ON CREATE SET comp.title = line.CompetencyTitle,
# comp.description = line.CompetencyDefinition,
# comp.source = "OPM"
# ;""")


# ############# DON'T UNCOMMENT #############
# ############# CREATE INDEX #############
# # Template for single-property
# query_list.append("""CREATE INDEX [index_name]
# FOR (n:LabelName)
# ON (n.propertyName)""")

# # Template for composite
# query_list.append("""CREATE INDEX [index_name]
# FOR (n:LabelName)
# ON (n.propertyName_1,
#     n.propertyName_2,
#     …
#     n.propertyName_n)""")

# query_list.append("""CREATE INDEX [workrole]
# FOR (w:Workrole)
# ON (w.onet_soc_code,
# 	w.title)
# ;""")

# query_list.append("""CREATE INDEX [occupation]
# FOR (o:Occupation)
# ON (o.onet_soc_code,
# 	o.title)	
# ;""")

############# EXECUTE QUERIES THROUGH A LOOP #############
############# runs query, logs query execution time, sends update message about query progress #############

query_counter = 1 # number of queries, manually increment through loop
total_queries = len(query_list)
query_times = [0] # list to track execution time of queries (used for logging)
query_times_and_summary_log_file = open(os.path.join(path, 'query_times_and_summary_logs.txt'), 'w+') # create query exection time log file

# query status box initialization
layout = [[gui.Text('EXECUTING QUERIES', font=(standard_font))],
                    [gui.Text('', size=(50, 1), font=(standard_font), key='query_count')],
                    [gui.Text('', size=(50, 1), font=(standard_font), key='query_time')]]
window = gui.Window('Progress Updates', layout, finalize=True)

for query in query_list:
    query_time_start = time.perf_counter() # start individual query exection timer
    graph.run(query) # execute query
    query_time_stop = time.perf_counter() # stop individual query exection timer
    query_times.append(query_time_stop - query_time_start) # add query execution time to the list
    #logging
    print('Completed ' + str(query_counter) + '/' + str(total_queries) + ' Queries')
    print('Query ' + str(query_counter) + f' completed in {query_times[query_counter]:0.4f} seconds.')
    query_times_and_summary_log_file.write('Query ' + str(query_counter) + f' completed in {query_times[query_counter]:0.4f} seconds.\n')
    window.read(timeout=0.1) #timeout was the make or break piece
    window['query_count'].update('Completed ' + str(query_counter) + '/' + str(total_queries) + ' Queries')
    window['query_time'].update('Query ' + str(query_counter) + f' completed in {query_times[query_counter]:0.4f} seconds.')
    query_counter += 1

if window.read() == gui.WIN_CLOSED:
    window.close()

############# FINISHED QUERIES, WRAP UP LOGGING #############

# finish clocking process times
total_queries_time_stop = time.perf_counter() # stop timer to log total query time
total_program_time_stop = time.perf_counter() # stop timer to log total program time

# log messages
total_time_message = f'Updates took a total of: {total_program_time_stop - total_program_time_start:0.4f} seconds.\n'
file_time_message = f'File importing took: {file_import_time_stop - file_import_time_start:0.4f} seconds.\n'
query_time_message = f'Queries took: {total_queries_time_stop - total_queries_time_start:0.4f} seconds.\n'

# final logs and close file
query_times_and_summary_log_file.write(total_time_message)
query_times_and_summary_log_file.write(file_time_message)
query_times_and_summary_log_file.write(query_time_message)
query_times_and_summary_log_file.close()

# final success message
print('SUCCESS: Completed building the database.')
success = gui.Window(' ', [[gui.Text('SUCCESS: Completed building the database.', font=(standard_font))],
                            [gui.Text(total_time_message, font=(standard_font))],
                            [gui.Text(file_time_message, font=(standard_font))],
                            [gui.Text(query_time_message, font=(standard_font))],
                            [gui.Text('Full log of individual query times and this summary in query_times_and_summary_logs.txt.', font=(standard_font))],
                            [gui.Text('Close this window to finish program, and make sure to deactivate your virtualenv.', font=(standard_font))]])
if success.read() == gui.WIN_CLOSED:
    success.close()
