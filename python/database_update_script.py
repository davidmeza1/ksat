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
# WAS ABLE TO OMIT WHEN SETTING THIS UP ON WINDOWS
# 9. test with
# python -m tkinter -c 'tkinter._test()'
# should pop open a gui window

# Create Neo4j Database
# Install APOC & GDSL Plug-ins
# Change Settings
    # dbms.memory.heap.initial_size=1G
    # dbms.memory.heap.max_size=3G
    # apoc.import.file.enabled=true AFTER dbms.security.procedures.unrestricted=apoc.*,gds.* (will get errors otherwise)
    # apoc.export.file.enabled=true
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

# If this is your first time running this script, choose 'Fresh Install'
    # This will work from the most updated version of ONET data
    # And erase any existing graph components
# If you've used this script before, choose 'Update'
    # This will compare the new files with the old ones
    # And only update what you need
    # If you don't have old files in your import folder, it'll just use the new files 

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
import xlrd
import csv


############# SIMPLE GUI TO TAKE BASIC ARGUMENTS #############
############# PRESENTED AT BEGINNING OF SCRIPT ONLY #############

assumed_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'import')) #assume path but allow changes
standard_font = 'Courier', 16

layout = [  [gui.Text('Database Port', font=(standard_font)), gui.InputText(font=(standard_font))],
            [gui.Text('Database Password', font=(standard_font)), gui.InputText(font=(standard_font))], 
            [gui.Text('Path to Import Folder', font=(standard_font)), gui.InputText(assumed_path, font=(standard_font))],
            [gui.Text('Install Choice: ', font=(standard_font)),
                gui.Radio('Fresh Install', 'install choice', font=(standard_font), default=False), 
                gui.Radio('Update', 'install choice', font=(standard_font), default=False)], 
            [gui.Button('Start', font=(standard_font)), gui.Button('Cancel', font=(standard_font))] ]
window = gui.Window('Database Details', layout)
while True:
    event, inputs = window.read()
    # close program if input window is closed
    if event == gui.WIN_CLOSED or event == 'Cancel':
        quit()
    # before updating, make sure to have the inputs you need - otherwise, send message and don't proceed
    if event == 'Start': 
        if inputs[0] and inputs[1] and inputs[2] and (inputs[3] or inputs[4]):
            port = inputs[0]
            pswd = inputs[1]
            path = inputs[2]
            firstrun = inputs[3]
            updaterun = inputs[4]
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
            elif not inputs[3] or inputs[4]:
                missing = 'Install Choice'
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
update('Received details, starting updates.')

# make sure that import folder under neo4j database exists
if not os.path.exists(path):
    update('ERROR: Database import folder is required to update the database, but does not exist.')
    quit()
else:
    update('SUCCESS: Database import folder exists, proceeding with file imports.')

#validate url, then process html doc from url
url = 'https://www.onetcenter.org/database.html#all-files'
try:
    r = requests.get(url)
except requests.exceptions.RequestException as e:
    update('ERROR: Could not access ONET Database URL. See console for exception details.')
    raise SystemExit(e)
soup = BeautifulSoup(r.text, 'html.parser')
update('SUCCESS: Accessed ONET Database, starting to import files.')

import_count = 1
skip_count = 0
file_process_time_start = time.perf_counter() # start timer to log total file import time
#file importing status box initialization
layout = [  [gui.Text('IMPORTING AND CONVERTING FILES', font=(standard_font))],
            [gui.Text(' ', size=(100, 1), font=(standard_font), key='recent_file')], 
            [gui.Text(' ', size=(100, 1), font=(standard_font), key='import')],
            [gui.Text(' ', size=(100, 1), font=(standard_font), key='skip')]]
window = gui.Window('Progress Updates', layout, finalize=True)

# list of files used in script, used to filter out unnecessary files
files_used = ['occupationdata.csv',
    'contentmodelreference.csv',
    'content_model_relationships.csv',
    'SOCMajorGroup.csv',
    'SOC_Level_With_Detailed.csv',
    'SOC_Level_Without_Detailed.csv',
    'DetailedOccupation.csv',
    'scalesreference.csv',
    'abilities.csv',
    'alternatetitles.csv',
    'iwareference.csv',
    'dwareference.csv',
    'educationtrainingandexperience.csv',
    'interests.csv',
    'jobzonereference.csv',
    'jobzones.csv',
    'knowledge.csv',
    'skills.csv',
    'taskstatements.csv',
    'taskratings.csv',
    'taskstodwas.csv',
    'unspscreference.csv',
    'technologyskills.csv',
    'toolsused.csv',
    'workactivities.csv',
    'workstyles.csv',
    'ncc_crosswalk.csv',
    'Employees_2020-05-28.csv',
    'Employees_2020-05-27.csv',
    'elementAbilities.csv',
    'elementBasicSkills.csv',
    'elementCrossFunctionalSkills.csv',
    'elementKnowledge.csv',
    'elementTasks.csv',
    'elementTechSkills.csv',
    'elementWorkActivities.csv',
    'FTE2020.csv',
    'NASACompetencyLibrary.csv',
    'OPMCompetencyLibrary.csv']
#TODO: add files used in employee data to this list

# scrape page for all links
for link in soup.find_all('a'):
    # if it's an href attribute & contains .xlsx, consider it
    if 'href' in link.attrs:
        if '.xlsx' in link.attrs['href']:
            # extract the specific file_url and get the html of that specific .xlsx file page (aka file contents) by appending to the base ONET url
            file_url = link.attrs['href']
            response = requests.get('https://www.onetcenter.org' + file_url)
            # create a sanitized xlsx_file_name
            xlsx_file_name = re.search('[^/]+$', file_url).group(0)
            xlsx_file_name = xlsx_file_name.replace('%20', '')
            xlsx_file_name = xlsx_file_name.replace('%2C', '')
            xlsx_file_name = xlsx_file_name.replace('-', '')
            xlsx_file_name = xlsx_file_name.replace(',', '')
            xlsx_file_name = xlsx_file_name.replace('_', '')
            xlsx_file_name = xlsx_file_name.lower()
            # only work with the file if it's actually used
            if xlsx_file_name.replace('.xlsx', '.csv') in files_used:
                open(os.path.join(path, xlsx_file_name), 'wb').write(response.content)
                # open excel workbook and grab name of first sheet
                workbook = xlrd.open_workbook(os.path.join(path, xlsx_file_name))
                sheet = workbook.sheet_by_name(workbook.sheet_by_index(0).name)
                csv_file_name = xlsx_file_name.replace('.xlsx', '.csv') 
                csv_file = open(os.path.join(path, csv_file_name), 'w')
                wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                for rownum in range(sheet.nrows):
                    wr.writerow(sheet.row_values(rownum))
                csv_file.close()
                # remove unnecessary xlsx
                os.remove(os.path.join(path, xlsx_file_name))
                print('Imported ' + xlsx_file_name + ' and converted it to ' + csv_file_name) 
                print('Total Imported: ' + str(import_count))
                print('Total Skipped: ' + str(skip_count))
                window.read(timeout=0.1) #timeout was the make or break piece
                window['recent_file'].update('Imported ' + xlsx_file_name + ' and converted it to ' + csv_file_name)
                window['import'].update('Total Imported: ' + str(import_count))
                window['skip'].update('Total Skipped: ' + str(skip_count))
                import_count += 1
            else: # file isn't used
                #logging
                print('Skipped: ' + xlsx_file_name)
                print('Total Imported: ' + str(import_count))
                print('Total Skipped: ' + str(skip_count))
                window.read(timeout=0.1) #timeout was the make or break piece
                window['recent_file'].update('Skipped: ' + xlsx_file_name)
                window['import'].update('Total Imported: ' + str(import_count))
                window['skip'].update('Total Skipped: ' + str(skip_count))
                skip_count += 1

time.sleep(5)
window.close()

# if you need a file and it's not in the import folder, quit
for file_used in files_used:
    if file_used not in os.listdir(path):
        update('Missing '+file_used+' needed for updating database, cannot continue without it.')
        quit()
# remove any files not actively used in script
for file_included in os.listdir(path):
    if file_included not in files_used:
        print('Removing '+file_included+' because it is not needed for updating database.')
        os.remove(os.path.join(path, file_included))

file_process_time_stop = time.perf_counter() # stop timer to log total file import time
update('Completed importing/converting/creating/removing files.')

############# CONNECT TO DATABASE #############

# Make sure the database is started first, otherwise attempt to connect will fail
try:
    graph = Graph('bolt://localhost:'+port, auth=('neo4j', pswd))
    update('SUCCESS: Connected to the Neo4j Database.')
    update('Starting cypher queries to create database; please do not interrupt process during runtime.')
    total_queries_time_start = time.perf_counter() # start timer to log total query time
except Exception as e:
    update('ERROR: Could not connect to the Neo4j Database. See console for details.')
    raise SystemExit(e)

############# APPEND THE QUERIES TO QUERY_ LIST #############

#clear db
if firstrun:
    graph.run("""MATCH (n) DETACH DELETE n""")

graph.run("""CALL apoc.schema.assert({}, {})""")
#TODO check these constraints
graph.run("""CREATE CONSTRAINT ON (scale:Scale) ASSERT scale.scaleId IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (workrole:Workrole ) ASSERT workrole.onet_soc_code IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (jzone:JobZone) ASSERT jzone.jobzone IS UNIQUE;""") 
graph.run("""CREATE CONSTRAINT ON (seg:Segment) ASSERT seg.segmentID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (seg:Segment) ASSERT seg.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (fam:Family) ASSERT fam.familyID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (fam:Family) ASSERT fam.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (cla:Class) ASSERT cla.classID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (cla:Class) ASSERT cla.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (com:Commodity) ASSERT com.commodityID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (com:Commodity) ASSERT com.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (tools:Tools) ASSERT tools.commodityID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (ncc:NASAClassCode) ASSERT ncc.ncc_class_code IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (nccgrp:NCCGroup) ASSERT nccgrp.ncc_grp_num IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (opm:OPMSeries) ASSERT opm.series IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (smg:SkillMixGrp) ASSERT smg.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (alttitles:AlternateTitles) ASSERT alttitles.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (alttitles:AlternateTitles) ASSERT alttitles.shorttitle IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (alttitles:Alternate_Titles) ASSERT alttitles.elementID IS UNIQUE;""")
#don't think we need a constraint for alternate title sources
graph.run("""CREATE CONSTRAINT ON (task:Task) ASSERT task.taskID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (abilities:Abilities) ASSERT abilities.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (interests:Interests) ASSERT interests.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (styles:Work_Styles) ASSERT styles.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (reqs:Worker_Requirements) ASSERT reqs.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (bskills:Basic_Skills) ASSERT bskills.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (cfskills:Cross_Functional_Skills) ASSERT cfskills.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (knowledge:Knowledge) ASSERT knowledge.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (edu:Education) ASSERT edu.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (exreqs:Experience_Requirements) ASSERT exreqs.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (exptr:Experience_And_Training) ASSERT exptr.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (bsenreq:Basic_Skills_Entry_Requirement) ASSERT bsenreq.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (cfsenreq:Cross_Functional_Skills_Entry_Requirement) ASSERT cfsenreq.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (licensing:Licensing) ASSERT licensing.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (ocreqs:Occupational_Requirements) ASSERT ocreqs.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (genacts:Generalized_Work_Activities) ASSERT genacts.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (orgcontext:Organizational_Context) ASSERT orgcontext.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (workcontext:Work_Context) ASSERT workcontext.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (detacts:Detailed_Work_Activities) ASSERT detacts.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (intacts:Intermediate_Work_Activities) ASSERT intacts.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (ocinfo:Occupation_Specific_Information) ASSERT ocinfo.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (task:Task) ASSERT task.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (title:Title) ASSERT title.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (desc:Description) ASSERT desc.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (techskills:Technology_Skills) ASSERT techskills.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (techskills:Technology_Skills) ASSERT techskills.commodityID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (tools:Tools) ASSERT tools.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (wfchar:Workforce_Characteristics) ASSERT wfchar.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (lminfo:Labor_Market_Information) ASSERT lminfo.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (ocoutlook:Occupational_Outlook) ASSERT ocoutlook.elementID IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (tskillprod:Tech_Skill_Product) ASSERT tskillprod.title IS UNIQUE;""")
graph.run("""CREATE CONSTRAINT ON (toolprod:Tool_Product) ASSERT toolprod.title IS UNIQUE;""")

query_list = []

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS 
FROM 'file:///occupationdata.csv' AS line
RETURN line
","
WITH line
Limit 1
RETURN line
",{batchSize:1000})""")

# TODO check these constraints
query_list.append("""CREATE CONSTRAINT ON (occupation:Occupation) ASSERT occupation.onet_soc_code IS UNIQUE;""")
query_list.append("""CREATE CONSTRAINT ON (element:Element) ASSERT element.elementID IS UNIQUE;""")
query_list.append("""CREATE CONSTRAINT ON (majgrp:MajorGroup) ASSERT majgrp.onet_soc_code IS UNIQUE;""")

# Load.
# Import Occupation Data, known as SOC Level
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS 
FROM 'file:///occupationdata.csv' AS line
RETURN line
","
MERGE (occupation:Occupation { onet_soc_code: line.`O*NET-SOC Code`} )
ON CREATE SET occupation.title = toLower(line.Title),
            occupation.description = toLower(line.Description),
            occupation.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #1110 

# Load the reference model for the elements
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///contentmodelreference.csv' AS line
RETURN line
","
MERGE (element:Element {elementID: line.`Element ID`})
ON CREATE SET element.title = toLower(line.`Element Name`),
            element.description = toLower(line.Description),
            element.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #585

# Load the relationships of the reference model. Self created
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///content_model_relationships.csv' AS line
RETURN line
","
MATCH (a:Element), (b:Element) 
WHERE a.elementID = line.From AND b.elementID = line.To AND a.elementID <> b.elementID
MERGE (a)<-[r:Sub_Element_Of]-(b)
",{batchSize:1000})""") #579

# Remove Element label and add Worker Characteristics & Ability Label to Remaining
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '1'
SET n:Worker_Characteristics
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('1.A')
SET a:Abilities
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('1.B')
SET b:Interests
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('1.C')
SET c:Work_Styles
;""")

# Worker Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '2'
SET n:Worker_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('2.A')
SET a:Basic_Skills
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('2.B')
SET b:Cross_Functional_Skills
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('2.C')
SET c:Knowledge
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('2.D')
SET d:Education
;""")

# Experience Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '3'
SET n:Experience_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('3.A')
SET a:Experience_And_Training
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('3.B')
SET b:Basic_Skills_Entry_Requirement
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('3.C')
SET c:Cross_Functional_Skills_Entry_Requirement
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('3.D')
SET d:Licensing
;""")

# Occupational Requirements
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '4'
SET n:Occupational_Requirements
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('4.A')
SET a:Generalized_Work_Activities
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('4.B')
SET b:Organizational_Context
;""")
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('4.C')
SET c:Work_Context
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('4.D')
SET d:Detailed_Work_Activities
;""")
query_list.append("""MATCH (e:Element)
WHERE e.elementID CONTAINS('4.E')
SET e:Intermediate_Work_Activities
;""")

# Occupation Specific Information
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '5'
SET n:Occupation_Specific_Information
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('5.A')
SET a:Task
;""")
# There is no 5.B
query_list.append("""MATCH (c:Element)
WHERE c.elementID CONTAINS('5.C')
SET c:Title
;""")
query_list.append("""MATCH (d:Element)
WHERE d.elementID CONTAINS('5.D')
SET d:Description
;""")
query_list.append("""MATCH (e:Element)
WHERE e.elementID CONTAINS('5.E')
SET e:Alternate_Titles
;""")
query_list.append("""MATCH (f:Element)
WHERE f.elementID CONTAINS('5.F')
SET f:Technology_Skills
;""")
query_list.append("""MATCH (g:Element)
WHERE g.elementID CONTAINS('5.G')
SET g:Tools
;""")

# Workforce Characteristics
query_list.append("""MATCH (n:Element)
WHERE n.elementID = '6'
SET n:Workforce_Characteristics
;""")
query_list.append("""MATCH (a:Element)
WHERE a.elementID CONTAINS('6.A')
SET a:Labor_Market_Information
;""")
query_list.append("""MATCH (b:Element)
WHERE b.elementID CONTAINS('6.B')
SET b:Occupational_Outlook
;""")

# Load The SOC Major Group Occupation, Change label to MajorGroup
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///SOCMajorGroup.csv' AS line
RETURN line
","
MERGE (occupation:MajorGroup { onet_soc_code: line.SOCMajorGroupCode})
ON CREATE SET occupation.title = toLower(line.SOCMajorGroupTitle),
            occupation.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #23

# Load SOC Level with Detail Occupations, Change label
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_With_Detailed.csv' AS line
RETURN line
","
MERGE (occupation:Occupation { onet_soc_code: line.SOCLevelCode})
ON CREATE SET occupation.title = toLower(line.SOCLevelTitle),
            occupation.description = toLower(line.SOCLevelDescription),
            occupation.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #76

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_With_Detailed.csv' AS line
RETURN line
","
MATCH (a:MajorGroup), (b:Occupation)
WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Major_Group]-(b)
",{batchSize:1000})""") #76

# Load SOC Level without Detail Occupations, Change label
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_Without_Detailed.csv' AS line
RETURN line
","
MERGE (occupation:Occupation { onet_soc_code: line.SOCLevelCode})
ON CREATE SET occupation.title = toLower(line.SOCLevelTitle),
            occupation.description = toLower(line.SOCLevelDescription),
            occupation.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #772

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///SOC_Level_Without_Detailed.csv' AS line
RETURN line
","
MATCH (a:MajorGroup), (b:Occupation)
WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Major_Group]-(b)
",{batchSize:1000})""") #772

# Load Detailed Occupations, Change label to Detailed Occupation or something else
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///DetailedOccupation.csv' AS line
RETURN line
","
MERGE (occupation:Workrole { onet_soc_code: line.SOCDetailCode})
ON CREATE SET occupation.title = toLower(line.SOCDetailTitle),
            occupation.description = toLower(line.SOCDetailDescription),
            occupation.source = 'ONET'
",{batchSize:1000, parallel:true, retries: 10})""") #149

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///DetailedOccupation.csv' AS line
RETURN line
","
MATCH (a:Occupation), (b:Workrole)
WHERE a.onet_soc_code = line.SOCLevelCode AND b.onet_soc_code = line.SOCDetailCode AND a.onet_soc_code <> b.onet_soc_code
MERGE (a)<-[r:IN_Occupation]-(b)
",{batchSize:1000})""") #149

# Create Scale Nodes. Each element will have an edge to a scale with
# associated statistical measures
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
From 'file:///scalesreference.csv' AS line
RETURN line
","
MERGE (scale:Scale {scaleId: line.`Scale ID`})
ON CREATE SET scale.title = toLower(line.`Scale Name`),
            scale.min = toInteger(line.Minimum),
            scale.max = toInteger(line.Maximum)
",{batchSize:1000, parallel:true, retries: 10})""") #29

# The following section creates the relationships between the Elements and the Occuptions
# Elements include abilities, knowledge, skills, and work activities
# Remove Element label here, add property of source of data = ONET, remove ONET_XXXXX label for all below
# Load Abilities
# Add relationships to Occupation and Workrole

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///abilities.csv' AS line
RETURN line
","
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Abilities {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'ability'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'ability'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #100672

#to compare updates from query 46
# MATCH (n:Abilities) 
# MATCH (o:Occupation)
# WHERE n.elementID='1.A.1.a.1' AND o.onet_soc_code='11-3031.01'
# RETURN n, o LIMIT 25

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///abilities.csv' AS line
RETURN line
","
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Abilities {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'ability'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'ability'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #100672

# Add Alternative titles for Occupations and Workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///alternatetitles.csv' AS line
RETURN line
","
MERGE (t:AlternateTitles {title: line.`Alternate Title`,
    shorttitle: line.`Short Title`, source: line.`Source(s)`})
WITH t, line
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
WITH t, a, line
MERGE (a)-[:Equivalent_To]->(t)
",{batchSize:10000})""") #56779	

# Trying Match to see if the properties are not removed.
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///alternatetitles.csv' AS line
RETURN line
","
MERGE (t:AlternateTitles {title: line.`Alternate Title`,
    shorttitle: line.`Short Title`, source: line.`Source(s)`})
WITH t, line
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
WITH t, a, line
MERGE (a)-[:Equivalent_To]->(t)
",{batchSize:10000})""") #56779	

# Add IWA and DWA to Generalized Work Activities
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///iwareference.csv' AS line
RETURN line
","
MATCH (a:Generalized_Work_Activities {elementID: line.`Element ID`})
MERGE (b:Generalized_Work_Activities {elementID: line.`IWA ID`, title: line.`IWA Title`})
WITH a, b, line
MERGE (b)-[:Sub_Element_Of]->(a)
",{batchSize:1000})""") #332

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///dwareference.csv' AS line
RETURN line
","
MATCH (a:Generalized_Work_Activities {elementID: line.`IWA ID`})
MERGE (b:Generalized_Work_Activities {elementID: line.`DWA ID`, title: line.`DWA Title`})
WITH a, b, line
MERGE (b)-[:Sub_Element_Of]->(a)
",{batchSize:1000})""") #2067

# Add Education, Experience and Training relationships and measures
# to Occupationa and workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///educationtrainingandexperience.csv' AS line
RETURN line
","
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Education {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'education'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'education'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #40186

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///educationtrainingandexperience.csv' AS line
RETURN line
","
MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Experience_And_Training {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'experience'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'experience'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #40186

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///educationtrainingandexperience.csv' AS line
RETURN line
","
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Education {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'education'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'education'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #40186

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///educationtrainingandexperience.csv' AS line
RETURN line
","
MATCH (a:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (b:Experience_And_Training {elementID: line.`Element ID`})
WITH a, b, line
MERGE (b)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'experience'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'experience'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #40186

# Interests
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///interests.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (i:Interests {elementID: line.`Element ID`})
WITH o, i, line
MERGE (i)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'interest'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'interest'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:1000})""") #8766

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///interests.csv' AS line
RETURN line
","
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (i:Interests {elementID: line.`Element ID`})
WITH w, i, line
MERGE (i)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'interest'}]->(w)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'interest'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:1000})""") #8766

# Job Zones
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///jobzonereference.csv' AS line
RETURN line
","
MERGE (j:JobZone {jobzone: toInteger(line.`Job Zone`)})
ON CREATE SET j.name = toLower(line.Name),
    j.experience = toLower(line.Experience),
    j.education = toLower(line.Education),
    j.training = toLower(line.`Job Training`),
    j.example = toLower(line.Examples),
    j.svpRange = line.`SVP Range`
RETURN count(j)
",{batchSize:1000, parallel:true, retries: 10})""") #5

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///jobzones.csv' AS line
RETURN line
","
MATCH (j:JobZone {jobzone: toInteger(line.`Job Zone`)})
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
WITH j, o, line
MERGE (o)-[:In_Job_Zone {jobzone: line.`Job Zone`, date: line.Date}]->(j)
",{batchSize:1000})""") #969

# Knowledge
# Add relationships to Occupation and Workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///knowledge.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (k:Knowledge {elementID: line.`Element ID`})
WITH o, k, line
MERGE (k)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'knowledge'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'knowledge'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #63888

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///knowledge.csv' AS line
RETURN line
","
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (k:Knowledge {elementID: line.`Element ID`})
WITH w, k, line
MERGE (k)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'knowledge'}]->(w)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'knowledge'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #63888

# Skills
# Add relationships to Occupation and Workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///skills.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Basic_Skills {elementID: line.`Element ID`})
WITH o, s, line
MERGE (s)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'basic_skill'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'basic_skill'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #67760

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///skills.csv' AS line
RETURN line
","
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Basic_Skills {elementID: line.`Element ID`})
WITH w, s, line
MERGE (s)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'basic_skill'}]->(w)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'basic_skill'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #67760

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///skills.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Cross_Functional_Skills {elementID: line.`Element ID`})
WITH o, s, line
MERGE (s)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'cf_skill'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'cf_skill'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #67760

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///skills.csv' AS line
RETURN line
","
MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (s:Cross_Functional_Skills {elementID: line.`Element ID`})
WITH w, s, line
MERGE (s)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'cf_skill'}]->(w)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'cf_skill'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #67760

# This sections will add task and their statements as nodes and create relationships to occupations.
# Add relationships to Occupation and Workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///taskstatements.csv' AS line
RETURN line
","
MERGE (task:Task { taskID: toInteger(line.`Task ID`)})
ON CREATE SET task.description = toLower(line.Task),
            task.tasktype = toLower(line.`Task Type`),
            task.incumbentsresponding = line.`Incumbents Responding`,
            task.date = line.Date,
            task.domainsource = line.`Domain Source`,
            task.source = 'ONET'
",{batchSize:10000, parallel:true, retries: 10})""") #19735

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///taskratings.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
WITH o, task, line
MERGE (task)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'task'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'task'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #175977

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///taskratings.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
WITH o, task, line
MERGE (task)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'task'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'task'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #175977

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///taskstodwas.csv' AS line
RETURN line
","
MATCH (a:Generalized_Work_Activities {elementID: line.`DWA ID`})
MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
WITH a, task, line
MERGE (task)-[:Task_For_DWA {date: line.Date, domainsource: line.`Domain Source`}]->(a)
",{batchSize:10000})""") #23307

# Commodities, to include tools and tech
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///unspscreference.csv' AS line
RETURN line
","
MERGE (s:Segment {segmentID: toInteger(line.`Segment Code`), title: toLower(line.`Segment Title`)})
MERGE (f:Family {familyID: toInteger(line.`Family Code`), title: toLower(line.`Family Title`)})
MERGE (c:Class { classID: toInteger(line.`Class Code`), title: toLower(line.`Class Title`)})
MERGE (m:Commodity {commodityID: toInteger(line.`Commodity Code`), title: toLower(line.`Commodity Title`)})
MERGE (s)<-[r:Sub_Segment]-(f)
MERGE (f)<-[a:Sub_Segment]-(c)
MERGE (c)<-[b:Sub_Segment]-(m)
",{batchSize:1000})""") #4307

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///technologyskills.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
MATCH (t:Technology_Skills {elementID: '5.F.1'})
SET m:Technology_Skills
MERGE (m)-[r:Sub_Element_Of]-(t)
MERGE (p:Tech_Skill_Product {title: line.Example})
ON CREATE SET p.hottech = line.`Hot Technology`
WITH o, m, p, line
MERGE (m)-[:Technology_Used_In]->(o)
MERGE (p)-[:Technology_Product]-(m)
",{batchSize:10000})""") #29370

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///technologyskills.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Technology_Skills {commodityID: toInteger(line.`Commodity Code`)})
MERGE (p:Tech_Skill_Product {title: line.Example})
ON CREATE SET p.hottech = line.`Hot Technology`
WITH o, m, p, line
MERGE (m)-[:Technology_Used_In]->(o)
MERGE (p)-[:Technology_Product]-(m)
",{batchSize:10000})""") #29370

# Tools
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///toolsused.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
MATCH (t:Tools {elementID: '5.G.1'})
SET m:Tools
MERGE (m)-[r:Sub_Element_Of]-(t)
MERGE (p:Tool_Product {title: line.Example})
ON CREATE SET p.hottech = 'N'
WITH o, m, p, line
MERGE (m)-[:Tools_Used_In]->(o)
MERGE (p)-[:Tool_Product]-(m)
",{batchSize:10000})""") #42278

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///toolsused.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (m:Tools {commodityID: toInteger(line.`Commodity Code`)})
MERGE (p:Tool_Product {title: line.Example})
ON CREATE SET p.hottech = 'N'
WITH o, m, p,line
MERGE (m)-[:Tools_Used_In]->(o)
MERGE (p)-[:Tool_Product]-(m)
",{batchSize:10000})""") #42278

# Activities
# Add relationships to Occupation and Workrole
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///workactivities.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Generalized_Work_Activities { elementID: line.`Element ID`})
WITH o, a, line
MERGE (a)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'activity'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'activity'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #79376

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///workactivities.csv' AS line
RETURN line
","
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Generalized_Work_Activities { elementID: line.`Element ID`})
WITH o, a, line
MERGE (a)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'activity'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'activity'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:10000})""") #79376

# Work Styles
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///workstyles.csv' AS line
RETURN line
","
MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
MATCH (a:Work_Styles { elementID: line.`Element ID`})
WITH o, a, line
MERGE (a)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'work_style'}]->(o)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'work_style'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:1000})""") #15472

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///workstyles.csv' AS line
RETURN line
","
MATCH (a:Work_Styles { elementID: line.`Element ID`})
MATCH (o:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
WITH a, o, line
MERGE (o)-[Found_In:Found_In {scale: line.`Scale ID`, element: 'work_style'}]->(a)
ON CREATE SET Found_In.scale = toString(line.`Scale ID`), Found_In.element = 'work_style'
SET Found_In.datavalue = toFloat(line.`Data Value`)
",{batchSize:1000})""") #15472

############# NCC OPM Crosswalk #############
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
RETURN line
","
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
",{batchSize:1000})""") #572

# OPM Series to ONET crosswalk
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
RETURN line
","
MATCH (occ:Occupation), (opm:OPMSeries)
WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
",{batchSize:1000})""") #572

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///ncc_crosswalk.csv' AS line
RETURN line
","
MATCH (occ:Workrole), (opm:OPMSeries)
WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
",{batchSize:1000})""") #572

# For a specific SOC
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code = '17-2071.00' AND opm.series CONTAINS("855")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code = '17-2072.00' AND opm.series CONTAINS("855")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code CONTAINS('17-206') AND opm.series CONTAINS("854")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1400', censustitle: toLower('COMPUTER HARDWARE ENGINEERS')}]->(opm)
;""")
query_list.append("""MATCH (o:Occupation), (opm:OPMSeries)
WHERE o.onet_soc_code CONTAINS('15-1111') AND opm.series CONTAINS("1550")
MERGE (o)-[r:IN_OPM_Series {censuscode: '1005', censustitle: toLower('COMPUTER & INFORMATION RESEARCH SCIENTISTS')}]->(opm)
;""")

query_list.append("""MATCH (opm:OPMSeries), (o:Occupation)
WHERE opm.series CONTAINS('2210') AND o.onet_soc_code CONTAINS('15-1')
MERGE (o)-[:IN_OPM_Series {censuscode: '1050', censustitle: toLower('COMPUTER SUPPORT SPECIALISTS')}]->(opm)
;""")

############# Employees #############

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///Employees_2020-05-28.csv' AS line
RETURN line
","
MERGE (emp:Employee {uupic: line.UUPIC})
ON CREATE SET emp.fname = line.`Name First`,
			emp.minitial = line.`Name Middle`,
			emp.lname = line.`Name Last`,
			emp.date_position = line.`Date Entered Current Position`,
			emp.email = toLower(line.`Email Address Work`),
			emp.age = toInteger(line.`Employee Age in Yrs`),
			emp.status = line.`Employee Status`,
			emp.type = line.`Employee Type`,
			emp.grade = line.Grade,
			emp.service_years = line.`Years of Service - Federal`,
			emp.accession = line.`Date Accession` // you need to change format to YYYY-MM-DD

MERGE (center:Center { center: line.Center})

MERGE (org:Organizations { org_code: line.`Organization Code`})
On CREATE SET org.title = line.`Organization Title`

MERGE (map:MapOrganization {map_org: line.`Map Organization Code`})

MERGE (emp)-[:Located_At]->(center)
MERGE (emp)-[:In_Organization]->(org)
MERGE (org)-[:In_MAP]->(map)
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///Employees_2020-05-27.csv' AS line
RETURN line
","
MATCH (emp:Employee), (opm:OPMSeries)
WHERE emp.uupic = line.UUPIC and opm.series CONTAINS(line.`Occupational Series`)
MERGE (emp)-[:IN_OPM_Series]->(opm)
",{batchSize:10000})""")

# Map Elements to Employees
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementAbilities.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Abilities)
WHERE emp.uupic = line.UUPIC AND elem.description = line.Abilities
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'ability'
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementBasicSkills.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Basic_Skills)
WHERE emp.uupic = line.UUPIC AND elem.description = line.BasicSkills
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'basic_skill'
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementCrossFunctionalSkills.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Cross_Functional_Skills)
WHERE emp.uupic = line.UUPIC AND elem.description = line.CrossFunctionalSkills
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'cf_skill'
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementKnowledge.csv' AS line
RETURN line
","
MATCH (emp:Employee {uupic: line.UUPIC}), (elem:Knowledge {description: line.Knowledge})
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'knowledge'
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementTasks.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Task)
WHERE emp.uupic = line.UUPIC AND elem.description = line.Tasks
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'task'
",{batchSize:10000})""")

#TODO: RESUME HERE

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementTechSkills.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Technology_Skills)
WHERE emp.uupic = line.UUPIC AND elem.title = line.TechSkills
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'tech_skill'
",{batchSize:10000})""")

query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///elementWorkActivities.csv' AS line
RETURN line
","
MATCH (emp:Employee), (elem:Generalized_Work_Activities)
WHERE emp.uupic = line.UUPIC AND elem.description = line.WorkActivities
MERGE (emp)-[f:Found_In]->(elem)
SET f.datavalue = toFloat(2.5),
	f.scale = 'IM',
	f.element = 'activity'
",{batchSize:10000})""")

# Update Center Inforation
query_list.append("""MATCH (c:Center)
WHERE c.center = 'HQ'
SET c.title = 'Headquarters',
	c.business_area = toInteger(10)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'OIG'
SET c.title = 'Office of the Inspector General',
	c.business_area = toInteger(99)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'NSSC'
SET c.title = 'NASA Shared Services Center',
	c.business_area = toInteger(99)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'ARC'
SET c.title = 'Ames Research Center',
	c.business_area = toInteger(21)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'GRC'
SET c.title = 'Glenn Research Center',
	c.business_area = toInteger(22)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'LARC'
SET c.title = 'Langley Research Center',
	c.business_area = toInteger(23)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'AFRC'
SET c.title = 'Armstrong Filght Research Center',
	c.business_area = toInteger(24)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'GSFC'
SET c.title = 'Goddard Space Flight Center',
	c.business_area = toInteger(51)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'MSFC'
SET c.title = 'Marshall Space Flight Center',
	c.business_area = toInteger(62)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'SSC'
SET c.title = 'Stennis Space Center',
	c.business_area = toInteger(64)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'JSC'
SET c.title = 'Johnson Space Center',
	c.business_area = toInteger(72)
;""")
query_list.append("""MATCH (c:Center)
WHERE c.center = 'KSC'
SET c.title = 'Kennedy Space Center',
	c.business_area = toInteger(74)
;""")
# ADD Mission, Theme, Program, Project, Cost Center From ALDS info
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///FTE2020.csv' AS line
RETURN line
","
MATCH (emp:Employee)
WHERE emp.uupic = line.UUPIC 

MERGE (mis:Mission {acronym: line.`Mission/Mission Equivalent`})
ON CREATE SET mis.title = line.`Mission Equivalent`

MERGE (theme:Theme {acronym: line.`Theme/Theme  Equivalent`})
ON CREATE SET theme.title = line.`Theme  Equivalent`

MERGE (program:Program {program_code: line.`Program/Program Equivalent`})
ON CREATE SET program.title = line.`Program Equivalent`

MERGE (project:Project {project_code: line.`Project/Project  Equivalent`})
ON CREATE SET project.title = line.`Project  Equivalent`

MERGE (cost:Cost_Center {cost_code: line.`Cost Center`})
ON CREATE SET cost.title = line.`Cost Center Equivalent`

MERGE (emp)-[:Charged_To]->(cost)
MERGE (cost)-[:Charged_To]->(project)
MERGE (project)-[:Charged_To]->(program)
MERGE (program)-[:Charged_To]->(theme)
MERGE (theme)-[:Charged_To]->(mis)
",{batchSize:10000})""")

# Add NASA Competency Library
query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///NASACompetencyLibrary.csv' AS line
RETURN line
","

MERGE (comptype:CompetencyType {prefix: line.Prefix})
ON CREATE SET comptype.title = line.comptype,
comptype.source = 'NASA'

MERGE (compsuite:CompetencySuite)
ON CREATE SET compsuite.title = line.CompSuite,
compsuite.source = 'NASA' 

MERGE (compdesg:CompetencyDesignation {acronym: line.CompDesg})
ON CREATE SET compdesg.source = 'NASA'

MERGE (comp:Competency {compid: toInteger(line.CompID)})
ON CREATE SET comp.title = line.CompTitle,
comp.description = line.CompDescription,
comp.source = 'NASA'

MERGE (compsuite)-[:In_Comp_Type]->(comptype)
MERGE (compdesg)-[:In_Comp_Suite]->(compsuite)
MERGE (comp)-[:Has_Comp_Desgination]->(compdesg)
",{batchSize:10000})""")


query_list.append("""CALL apoc.periodic.iterate("
LOAD CSV WITH HEADERS
FROM 'file:///OPMCompetencyLibrary.csv' AS line
RETURN line
","
MERGE (comp:Competency {compid: toInteger(line.id)})
ON CREATE SET comp.title = line.CompetencyTitle,
comp.description = line.CompetencyDefinition,
comp.source = 'OPM'
",{batchSize:10000, parallel:true, retries: 10})""")


############# DON'T UNCOMMENT #############
############# CREATE INDEX #############
# # Template for single-property
# query_list.append("""CREATE INDEX index_name
# FOR (n:LabelName)
# ON (n.propertyName)""")

# # Template for composite
# query_list.append("""CREATE INDEX [index_name]
# FOR (n:LabelName)
# ON (n.propertyName_1,
#     n.propertyName_2,
#     …
#     n.propertyName_n)""")

query_list.append("""CREATE INDEX workrole
FOR (w:Workrole)
ON (w.onet_soc_code,
	w.title)
;""")

query_list.append("""CREATE INDEX occupation
FOR (o:Occupation)
ON (o.onet_soc_code,
	o.title)	
;""")

############# EXECUTE QUERIES THROUGH A LOOP #############
############# runs query, logs query execution time, sends update message about query progress #############

query_counter = 1 # number of queries, manually increment through loop
total_queries = len(query_list)
query_times = [0] # list to track execution time of queries (used for logging)
log_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'logs'))
query_times_and_summary_log_file = open(os.path.join(log_path, 'query_times_and_summary_logs.txt'), 'w+') # create query exection time log file

# query status box initialization
layout = [[gui.Text('EXECUTING QUERIES', font=(standard_font))],
                    [gui.Text('', size=(50, 1), font=(standard_font), key='query_count')],
                    [gui.Text('', size=(50, 1), font=(standard_font), key='query_time')]]
window = gui.Window('Progress Updates', layout, finalize=True)

for query in query_list:
    query_time_start = time.perf_counter() # start individual query exection timer
    g = graph.begin() # open transaction
    g.run(query) # execute query
    g.commit() # close transaction
    query_time_stop = time.perf_counter() # stop individual query exection timer
    query_times.append(query_time_stop - query_time_start) # add query execution time to the list
    #logging
    print('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
    query_times_and_summary_log_file.write('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.\n')
    window.read(timeout=0.1) #timeout was the make or break piece
    window['query_count'].update('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
    query_counter += 1

time.sleep(5)
window.close()

############# FINISHED QUERIES, WRAP UP LOGGING #############

# finish clocking process times
total_queries_time_stop = time.perf_counter() # stop timer to log total query time
total_program_time_stop = time.perf_counter() # stop timer to log total program time

# log messages
total_time_message = f'Updates took a total of: {total_program_time_stop - total_program_time_start:0.4f} seconds.\n'
file_time_message = f'File processing took: {file_process_time_stop - file_process_time_start:0.4f} seconds.\n'
query_time_message = f'Queries took: {total_queries_time_stop - total_queries_time_start:0.4f} seconds.\n'

# final logs
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
                            [gui.Text('Full log of individual query times and this summary in query_times_and_summary_logs.txt, under "logs" folder.', font=(standard_font))],
                            [gui.Text('Close this window to finish program, and make sure to deactivate your virtualenv.', font=(standard_font))]])
if success.read() == gui.WIN_CLOSED:
    success.close()
