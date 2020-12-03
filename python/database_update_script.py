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

#BEFORE YOU UPDATE AN EXISTING DATABASE
#CLONE THE DATABASE TO HAVE A BACKUP- JUST IN CASE :)

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
import pandas as pd
from itertools import chain
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import gensim
from gensim.models import Doc2Vec
from gensim.models.doc2vec import TaggedDocument
import nltk
import nltk.data
from nltk.stem import PorterStemmer
from scipy import spatial
import multiprocessing


############# SIMPLE GUI TO TAKE BASIC ARGUMENTS #############
############# PRESENTED AT BEGINNING OF SCRIPT ONLY #############
def present_gui():
    assumed_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'import')) #assume path but allow changes

    layout = [  [gui.Text('Database Port', font=(standard_font)), gui.InputText(font=(standard_font))],
                [gui.Text('Database Username', font=(standard_font)), gui.InputText(font=(standard_font))], 
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
                user = inputs[1]
                pswd = inputs[2]
                path = inputs[3]
                firstrun = inputs[4]
                window.close()
                break
            else:
                # identify missing info
                if not inputs[0]:
                    missing = 'Port'
                elif not inputs[1]:
                    missing = 'Username'
                elif not inputs[2]:
                    missing = 'Password'
                elif not inputs[3]:
                    missing = 'Path'
                elif not inputs[4] or inputs[5]:
                    missing = 'Install Choice'
                # present alert box with the message
                alert = gui.Window(' ', [[gui.Text('Missing ' + missing, font=(standard_font))]])
                if alert.read() == gui.WIN_CLOSED:
                    alert.close()
    return port, user, pswd, path, firstrun 

############# SIMPLE GUI TO DISPLAY PROGRESS UPDATES #############
############# INVOKED THROUGHOUT SCRIPT #############
def update(string=''):
    print(string)
    log_file.write(string)
    # present message for a few seconds then auto-close
    window = gui.Window('Progress Update', [[gui.Text(string, font=(standard_font))]]).Finalize()
    time.sleep(3)
    window.close()

############# IMPORT UPDATED .TXT FILES FROM ONET DATABASE #############
def import_onet_data(path):
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

    # scrape page for all links
    import_count = 1
    skip_count = 0
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
                    #logging
                    print('Imported ' + xlsx_file_name + ' and converted it to ' + csv_file_name) 
                    print('Total Imported: ' + str(import_count))
                    print('Total Skipped: ' + str(skip_count))
                    window.read(timeout=0.1) #timeout was the make or break piece
                    window['recent_file'].update('Imported ' + xlsx_file_name + ' and converted it to ' + csv_file_name)
                    window['import'].update('Total Imported: ' + str(import_count))
                    window['skip'].update('Total Skipped: ' + str(skip_count))
                    import_count += 1 #record successful imports
                else: # file isn't used
                    #logging
                    print('Skipped: ' + xlsx_file_name)
                    print('Total Imported: ' + str(import_count))
                    print('Total Skipped: ' + str(skip_count))
                    window.read(timeout=0.1) #timeout was the make or break piece
                    window['recent_file'].update('Skipped: ' + xlsx_file_name)
                    window['import'].update('Total Imported: ' + str(import_count))
                    window['skip'].update('Total Skipped: ' + str(skip_count))
                    skip_count += 1 #record skipped files

    time.sleep(5)
    window.close()

    # if you need a file and it's not in the import folder, quit
    files_missing = []
    for file_used in files_used:
        if file_used not in os.listdir(path):
            files_missing.append(file_used)
    if files_missing != []:
        for file_missing in files_missing:
            update('Missing '+file_missing+'. Cannot continue.')
        quit()
    # remove any files not actively used in script
    for file_included in os.listdir(path):
        if file_included not in files_used:
            update('Removing '+file_included+' because it is not needed for updating database.')
            os.remove(os.path.join(path, file_included))

    update('Completed importing/converting/creating/removing files.')

############# CONNECT TO DATABASE #############
def connect_to_database(port, user, pswd):
    # Make sure the database is started first, otherwise attempt to connect will fail
    try:
        graph = Graph('bolt://localhost:'+port, auth=(user, pswd))
        update('SUCCESS: Connected to the Neo4j Database.')
        update('Starting cypher queries to create database; please do not interrupt process during runtime.')
    except Exception as e:
        update('ERROR: Could not connect to the Neo4j Database. See console for details.')
        raise SystemExit(e)
    return graph

############# APPEND THE QUERIES TO QUERY_LIST #############
def append_queries(firstrun):
    #clear db if this is the first run
    if firstrun:
        query_list.append("""CALL apoc.schema.assert({},{})""")
        query_list.append("""MATCH (n) DETACH DELETE n""")

    #TODO check these constraints
    query_list.append("""CREATE CONSTRAINT scaleid ON (scale:Scale) ASSERT scale.scaleId IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT workrolecode ON (workrole:Workrole ) ASSERT workrole.onet_soc_code IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT jobzone ON (jzone:JobZone) ASSERT jzone.jobzone IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT segid ON (seg:Segment) ASSERT seg.segmentID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT segtitle ON (seg:Segment) ASSERT seg.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT famid ON (fam:Family) ASSERT fam.familyID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT famtitle ON (fam:Family) ASSERT fam.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT classid ON (cla:Class) ASSERT cla.classID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT classtitle ON (cla:Class) ASSERT cla.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT comid ON (com:Commodity) ASSERT com.commodityID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT comtitle ON (com:Commodity) ASSERT com.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT techelid ON (techskills:Technology_Skills) ASSERT techskills.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT techcomid ON (techskills:Technology_Skills) ASSERT techskills.commodityID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT toolselid ON (tools:Tools) ASSERT tools.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT toolscomid ON (tools:Tools) ASSERT tools.commodityID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ncc_code ON (ncc:NASAClassCode) ASSERT ncc.ncc_class_code IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ncc_group ON (nccgrp:NCCGroup) ASSERT nccgrp.ncc_grp_num IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT opm ON (opm:OPMSeries) ASSERT opm.series IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT smg ON (smg:SkillMixGrp) ASSERT smg.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT altid ON (alttitles:Alternate_Titles_ONET) ASSERT alttitles.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT taskid ON (task:Task) ASSERT task.taskID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT taskelid ON (task:Task) ASSERT task.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT abilid ON (abilities:Abilities) ASSERT abilities.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT intid ON (interests:Interests) ASSERT interests.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT styid ON (styles:Work_Styles) ASSERT styles.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT reqsid ON (reqs:Worker_Requirements) ASSERT reqs.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT bskillsid ON (bskills:Basic_Skills) ASSERT bskills.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT cfskillsid ON (cfskills:Cross_Functional_Skills) ASSERT cfskills.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT knowid ON (knowledge:Knowledge) ASSERT knowledge.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT eduid ON (edu:Education) ASSERT edu.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT exreqsid ON (exreqs:Experience_Requirements) ASSERT exreqs.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT exptrid ON (exptr:Experience_And_Training) ASSERT exptr.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT bsenreqid ON (bsenreq:Basic_Skills_Entry_Requirement) ASSERT bsenreq.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT cfsenreqid ON (cfsenreq:Cross_Functional_Skills_Entry_Requirement) ASSERT cfsenreq.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT licid ON (licensing:Licensing) ASSERT licensing.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ocreqsid ON (ocreqs:Occupational_Requirements) ASSERT ocreqs.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT genactsid ON (genacts:Generalized_Work_Activities) ASSERT genacts.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT orgconid ON (orgcontext:Organizational_Context) ASSERT orgcontext.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT workconid ON (workcontext:Work_Context) ASSERT workcontext.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT detactsid ON (detacts:Detailed_Work_Activities) ASSERT detacts.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT intactsid ON (intacts:Intermediate_Work_Activities) ASSERT intacts.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ocinfoid ON (ocinfo:Occupation_Specific_Information) ASSERT ocinfo.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT titldid ON (title:Title) ASSERT title.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT descid ON (desc:Description) ASSERT desc.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT wfcharid ON (wfchar:Workforce_Characteristics) ASSERT wfchar.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT lminfoid ON (lminfo:Labor_Market_Information) ASSERT lminfo.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ocoutlook ON (ocoutlook:Occupational_Outlook) ASSERT ocoutlook.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT tskillprod ON (tskillprod:Tech_Skill_Product) ASSERT tskillprod.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT toolprod ON (toolprod:Tool_Product) ASSERT toolprod.title IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT occ ON (occupation:Occupation) ASSERT occupation.onet_soc_code IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT ele ON (element:Element) ASSERT element.elementID IS UNIQUE;""")
    query_list.append("""CREATE CONSTRAINT majgrp ON (majgrp:MajorGroup) ASSERT majgrp.onet_soc_code IS UNIQUE;""")
    # CREATE CONSTRAINT ON (alttitles:AlternateTitles) ASSERT alttitles.title IS UNIQUE;
    # CREATE CONSTRAINT ON (alttitles:AlternateTitles) ASSERT alttitles.shorttitle IS UNIQUE;
    #don't think we need a constraint for alternate title sources

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""") 

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""") 

    # Load the relationships of the reference model. Self created
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///content_model_relationships.csv' AS line
    RETURN line
    ","
    MATCH (a:Element), (b:Element) 
    WHERE a.elementID = line.From AND b.elementID = line.To AND a.elementID <> b.elementID
    MERGE (a)<-[r:Sub_Element_Of]-(b)
    ",{batchSize:1000}) YIELD operations""")

    # Remove Element label and add others where appropriate
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '1' SET n:Worker_Characteristics;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('1.A') SET a:Abilities;""")
    query_list.append("""MATCH (b:Element) WHERE b.elementID CONTAINS('1.B') SET b:Interests;""")
    query_list.append("""MATCH (c:Element) WHERE c.elementID CONTAINS('1.C') SET c:Work_Styles;""")
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '2' SET n:Worker_Requirements;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('2.A') SET a:Basic_Skills;""")
    query_list.append("""MATCH (b:Element) WHERE b.elementID CONTAINS('2.B') SET b:Cross_Functional_Skills;""")
    query_list.append("""MATCH (c:Element) WHERE c.elementID CONTAINS('2.C') SET c:Knowledge;""")
    query_list.append("""MATCH (d:Element) WHERE d.elementID CONTAINS('2.D') SET d:Education;""")
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '3' SET n:Experience_Requirements;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('3.A') SET a:Experience_And_Training;""")
    query_list.append("""MATCH (b:Element) WHERE b.elementID CONTAINS('3.B') SET b:Basic_Skills_Entry_Requirement;""")
    query_list.append("""MATCH (c:Element) WHERE c.elementID CONTAINS('3.C') SET c:Cross_Functional_Skills_Entry_Requirement;""")
    query_list.append("""MATCH (d:Element) WHERE d.elementID CONTAINS('3.D') SET d:Licensing;""")
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '4' SET n:Occupational_Requirements;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('4.A') SET a:Generalized_Work_Activities;""")
    query_list.append("""MATCH (b:Element) WHERE b.elementID CONTAINS('4.B') SET b:Organizational_Context;""")
    query_list.append("""MATCH (c:Element) WHERE c.elementID CONTAINS('4.C') SET c:Work_Context;""")
    query_list.append("""MATCH (d:Element) WHERE d.elementID CONTAINS('4.D') SET d:Detailed_Work_Activities;""")
    query_list.append("""MATCH (e:Element) WHERE e.elementID CONTAINS('4.E') SET e:Intermediate_Work_Activities;""")
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '5' SET n:Occupation_Specific_Information;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('5.A') SET a:Task;""")
    # There is no 5.B
    query_list.append("""MATCH (c:Element) WHERE c.elementID CONTAINS('5.C') SET c:Title;""")
    query_list.append("""MATCH (d:Element) WHERE d.elementID CONTAINS('5.D') SET d:Description;""")
    query_list.append("""MATCH (e:Element) WHERE e.elementID CONTAINS('5.E') SET e:Alternate_Titles_ONET;""")
    query_list.append("""MATCH (f:Element) WHERE f.elementID CONTAINS('5.F') SET f:Technology_Skills;""")
    query_list.append("""MATCH (g:Element) WHERE g.elementID CONTAINS('5.G') SET g:Tools;""")
    query_list.append("""MATCH (n:Element) WHERE n.elementID = '6' SET n:Workforce_Characteristics;""")
    query_list.append("""MATCH (a:Element) WHERE a.elementID CONTAINS('6.A') SET a:Labor_Market_Information;""")
    query_list.append("""MATCH (b:Element) WHERE b.elementID CONTAINS('6.B') SET b:Occupational_Outlook;""")

    # Load The SOC Major Group Occupation, Change label to MajorGroup
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///SOCMajorGroup.csv' AS line
    RETURN line
    ","
    MERGE (occupation:MajorGroup { onet_soc_code: line.SOCMajorGroupCode})
    ON CREATE SET occupation.title = toLower(line.SOCMajorGroupTitle),
                occupation.source = 'ONET'
    ",{batchSize:1000}) YIELD operations""")

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///SOC_Level_With_Detailed.csv' AS line
    RETURN line
    ","
    MATCH (a:MajorGroup), (b:Occupation)
    WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
    MERGE (a)<-[r:IN_Major_Group]-(b)
    ",{batchSize:1000}) YIELD operations""") 

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///SOC_Level_Without_Detailed.csv' AS line
    RETURN line
    ","
    MATCH (a:MajorGroup), (b:Occupation)
    WHERE a.onet_soc_code = line.SOCMajorGroupCode AND b.onet_soc_code = line.SOCLevelCode AND a.onet_soc_code <> b.onet_soc_code
    MERGE (a)<-[r:IN_Major_Group]-(b)
    ",{batchSize:1000}) YIELD operations""")

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""") 

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///DetailedOccupation.csv' AS line
    RETURN line
    ","
    MATCH (a:Occupation), (b:Workrole)
    WHERE a.onet_soc_code = line.SOCLevelCode AND b.onet_soc_code = line.SOCDetailCode AND a.onet_soc_code <> b.onet_soc_code
    MERGE (a)<-[r:IN_Occupation]-(b)
    ",{batchSize:1000}) YIELD operations""")

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
    ",{batchSize:1000, parallel:true, retries: 10}) YIELD operations""")

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
    MATCH (c:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH a, b, c, line
    MERGE (b)-[f1:Found_In {scale: line.`Scale ID`, element: 'ability'}]->(a)
    MERGE (b)-[f2:Found_In {scale: line.`Scale ID`, element: 'ability'}]->(c)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'ability'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'ability'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    # Add Alternative titles for Occupations and Workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///alternatetitles.csv' AS line
    RETURN line
    ","
    MERGE (t:AlternateTitles {title: line.`Alternate Title`,
        shorttitle: line.`Short Title`, source: line.`Source(s)`})
    ",{batchSize:1000}) YIELD operations""")

    #takes a very long time- 2099s
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///alternatetitles.csv' AS line
    RETURN line
    ","
    MATCH (t:AlternateTitles {title: line.`Alternate Title`,
        shorttitle: line.`Short Title`, source: line.`Source(s)`})
    WITH t, line
    MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (b:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH t, a, b, line
    MERGE (a)-[:Equivalent_To]->(t)
    MERGE (b)-[:Equivalent_To]->(t)
    ",{batchSize:10000}) YIELD operations""")

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
    ",{batchSize:1000}) YIELD operations""") 

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///dwareference.csv' AS line
    RETURN line
    ","
    MATCH (a:Generalized_Work_Activities {elementID: line.`IWA ID`})
    MERGE (b:Generalized_Work_Activities {elementID: line.`DWA ID`, title: line.`DWA Title`})
    WITH a, b, line
    MERGE (b)-[:Sub_Element_Of]->(a)
    ",{batchSize:1000}) YIELD operations""")

    # Add Education, Experience and Training relationships and measures
    # to Occupationa and workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///educationtrainingandexperience.csv' AS line
    RETURN line
    ","
    MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (b:Education {elementID: line.`Element ID`})
    MATCH (c:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH a, b, c, line
    MERGE (b)-[f1:Found_In {scale: line.`Scale ID`, element: 'education'}]->(a)
    MERGE (b)-[f2:Found_In {scale: line.`Scale ID`, element: 'education'}]->(c)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'education'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'education'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///educationtrainingandexperience.csv' AS line
    RETURN line
    ","
    MATCH (a:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (b:Experience_And_Training {elementID: line.`Element ID`})
    MATCH (c:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH a, b, c, line
    MERGE (b)-[f1:Found_In {scale: line.`Scale ID`, element: 'experience'}]->(a)
    MERGE (b)-[f2:Found_In {scale: line.`Scale ID`, element: 'experience'}]->(c)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'experience'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'experience'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    # Interests
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///interests.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (i:Interests {elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, i, w, line
    MERGE (i)-[f1:Found_In {scale: line.`Scale ID`, element: 'interest'}]->(o)
    MERGE (i)-[f2:Found_In {scale: line.`Scale ID`, element: 'interest'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'interest'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'interest'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:1000}) YIELD operations""")

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
    ",{batchSize:1000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///jobzones.csv' AS line
    RETURN line
    ","
    MATCH (j:JobZone {jobzone: toInteger(line.`Job Zone`)})
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    WITH j, o, line
    MERGE (o)-[:In_Job_Zone {jobzone: line.`Job Zone`, date: line.Date}]->(j)
    ",{batchSize:1000}) YIELD operations""") 

    # Knowledge
    # Add relationships to Occupation and Workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///knowledge.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (k:Knowledge {elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, k, w, line
    MERGE (k)-[f1:Found_In {scale: line.`Scale ID`, element: 'knowledge'}]->(o)
    MERGE (k)-[f2:Found_In {scale: line.`Scale ID`, element: 'knowledge'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'knowledge'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'knowledge'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    # Skills
    # Add relationships to Occupation and Workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///skills.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (s:Basic_Skills {elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, s, w, line
    MERGE (s)-[f1:Found_In {scale: line.`Scale ID`, element: 'basic_skill'}]->(o)
    MERGE (s)-[f2:Found_In {scale: line.`Scale ID`, element: 'basic_skill'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'basic_skill'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'basic_skill'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///skills.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (s:Cross_Functional_Skills {elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, s, w, line
    MERGE (s)-[f1:Found_In {scale: line.`Scale ID`, element: 'cf_skill'}]->(o)
    MERGE (s)-[f2:Found_In {scale: line.`Scale ID`, element: 'cf_skill'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'cf_skill'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'cf_skill'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""") 

    # This sections will add task and their statements as nodes and create relationships to occupations.
    # Add relationships to Occupation and Workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///taskstatements.csv' AS line
    RETURN line
    ","
    MERGE (task:Task { taskID: toInteger(line.`Task ID`)})
    ON CREATE SET task.title = toLower(line.Task),
                task.description = toLower(line.Task),
                task.tasktype = toLower(line.`Task Type`),
                task.incumbentsresponding = line.`Incumbents Responding`,
                task.date = line.Date,
                task.domainsource = line.`Domain Source`,
                task.source = 'ONET'
    ",{batchSize:10000, parallel:true, retries: 10}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///taskratings.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, task, w, line
    MERGE (task)-[f1:Found_In {scale: line.`Scale ID`, element: 'task'}]->(o)
    MERGE (task)-[f2:Found_In {scale: line.`Scale ID`, element: 'task'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'task'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'task'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///taskstodwas.csv' AS line
    RETURN line
    ","
    MATCH (a:Generalized_Work_Activities {elementID: line.`DWA ID`})
    MATCH (task:Task { taskID: toInteger(line.`Task ID`)})
    WITH a, task, line
    MERGE (task)-[:Task_For_DWA {date: line.Date, domainsource: line.`Domain Source`}]->(a)
    ",{batchSize:10000}) YIELD operations""")

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
    ",{batchSize:1000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///technologyskills.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
    MATCH (t:Technology_Skills {elementID: '5.F.1'})
    MERGE (m)-[:Sub_Element_Of]-(t)
    MERGE (s:Specific_Technology_Skills {title: line.Example})
    ON CREATE SET s.hottech = (line.`Hot Technology`)
    WITH o, w, m, s, line
    SET m:General_Technology_Skills
    SET s:Element
    MERGE (s)-[:Found_In]->(o)
    MERGE (s)-[:Found_In]->(w)
    MERGE (s)-[:Sub_Element_Of]-(m)
    ",{batchSize:10000}) YIELD operations""")

    # Tools
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///technologyskills.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (m:Commodity {commodityID: toInteger(line.`Commodity Code`)})
    MATCH (t:Tools {elementID: '5.G.1'})
    MERGE (m)-[:Sub_Element_Of]-(t)
    MERGE (s:Specific_Tools {title: line.Example})
    ON CREATE SET s.hottech = 'N'
    WITH o, w, m, s, line
    SET m:General_Tools
    SET s:Element
    MERGE (s)-[:Found_In]->(o)
    MERGE (s)-[:Found_In]->(w)
    MERGE (s)-[:Sub_Element_Of]-(m)
    ",{batchSize:10000}) YIELD operations""")

    # Activities
    # Add relationships to Occupation and Workrole
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///workactivities.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (a:Generalized_Work_Activities { elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, a, w, line
    MERGE (a)-[f1:Found_In {scale: line.`Scale ID`, element: 'activity'}]->(o)
    MERGE (a)-[f2:Found_In {scale: line.`Scale ID`, element: 'activity'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'activity'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'activity'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:10000}) YIELD operations""")

    # Work Styles
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///workstyles.csv' AS line
    RETURN line
    ","
    MATCH (o:Occupation {onet_soc_code: line.`O*NET-SOC Code`})
    MATCH (a:Work_Styles { elementID: line.`Element ID`})
    MATCH (w:Workrole {onet_soc_code: line.`O*NET-SOC Code`})
    WITH o, a, w, line
    MERGE (a)-[f1:Found_In {scale: line.`Scale ID`, element: 'work_style'}]->(o)
    MERGE (a)-[f2:Found_In {scale: line.`Scale ID`, element: 'work_style'}]->(w)
    ON CREATE SET f1.scale = toString(line.`Scale ID`), f1.element = 'work_style'
    ON CREATE SET f2.scale = toString(line.`Scale ID`), f2.element = 'work_style'
    SET f1.datavalue = toFloat(line.`Data Value`)
    SET f2.datavalue = toFloat(line.`Data Value`)
    ",{batchSize:1000}) YIELD operations""")

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
    ",{batchSize:1000}) YIELD operations""")

    # OPM Series to ONET crosswalk
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///ncc_crosswalk.csv' AS line
    RETURN line
    ","
    MATCH (occ:Occupation), (opm:OPMSeries)
    WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
    MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
    ",{batchSize:1000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///ncc_crosswalk.csv' AS line
    RETURN line
    ","
    MATCH (occ:Workrole), (opm:OPMSeries)
    WHERE occ.onet_soc_code CONTAINS(line.`2010 SOC CODE`) AND opm.series = line.`OPMSeries`
    MERGE (occ)-[r:IN_OPM_Series {censuscode: line.`2010 EEO TABULATION (CENSUS) CODE`, censustitle: toLower(line.`2010 EEO TABULATION (CENSUS) OCCUPATION TITLE`)}]->(opm)
    ",{batchSize:1000}) YIELD operations""")

    # For a specific SOC
    query_list.append("""MATCH (o:Occupation), (opm:OPMSeries) WHERE o.onet_soc_code = '17-2071.00' AND opm.series CONTAINS("855")
                        MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm);""")
    query_list.append("""MATCH (o:Occupation), (opm:OPMSeries) WHERE o.onet_soc_code = '17-2072.00' AND opm.series CONTAINS("855")
                        MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm);""")
    query_list.append("""MATCH (o:Occupation), (opm:OPMSeries) WHERE o.onet_soc_code = '17-2072.00' AND opm.series CONTAINS("855")
                        MERGE (o)-[r:IN_OPM_Series {censuscode: '1410', censustitle: toLower('ELECTRICAL & ELECTRONIC ENGINEERS')}]->(opm);""")
    query_list.append("""MATCH (o:Occupation), (opm:OPMSeries) WHERE o.onet_soc_code CONTAINS('17-206') AND opm.series CONTAINS("854")
                        MERGE (o)-[r:IN_OPM_Series {censuscode: '1400', censustitle: toLower('COMPUTER HARDWARE ENGINEERS')}]->(opm);""")
    query_list.append("""MATCH (o:Occupation), (opm:OPMSeries) WHERE o.onet_soc_code CONTAINS('15-1111') AND opm.series CONTAINS("1550")
                        MERGE (o)-[r:IN_OPM_Series {censuscode: '1005', censustitle: toLower('COMPUTER & INFORMATION RESEARCH SCIENTISTS')}]->(opm);""")
    query_list.append("""MATCH (opm:OPMSeries), (o:Occupation) WHERE opm.series CONTAINS('2210') AND o.onet_soc_code CONTAINS('15-1')
                        MERGE (o)-[:IN_OPM_Series {censuscode: '1050', censustitle: toLower('COMPUTER SUPPORT SPECIALISTS')}]->(opm);""")

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
                emp.accession = date(line.`Date Accession`)

    MERGE (center:Center { center: line.Center})

    MERGE (org:Organizations { org_code: line.`Organization Code`})
    On CREATE SET org.title = line.`Organization Title`

    MERGE (map:MapOrganization {map_org: line.`Map Organization Code`})

    MERGE (emp)-[:Located_At]->(center)
    MERGE (emp)-[:In_Organization]->(org)
    MERGE (org)-[:In_MAP]->(map)
    ",{batchSize:5000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///Employees_2020-05-27.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (opm:OPMSeries)
    WHERE emp.uupic = line.UUPIC and opm.series CONTAINS(line.`Occupational Series`)
    MERGE (emp)-[:IN_OPM_Series]->(opm)
    ",{batchSize:10000}) YIELD operations""")

    # Map Elements to Employees
    #long time- 2740s
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementAbilities.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Abilities)
    WHERE emp.uupic = line.UUPIC AND elem.description = line.Abilities
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'ability'
    ",{batchSize:10000}) YIELD operations""")

    #long time- 2222s
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementBasicSkills.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Basic_Skills)
    WHERE emp.uupic = line.UUPIC AND elem.description = line.BasicSkills
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'basic_skill'
    ",{batchSize:10000}) YIELD operations""")

    #long time- 2364s
    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementCrossFunctionalSkills.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Cross_Functional_Skills)
    WHERE emp.uupic = line.UUPIC AND elem.description = line.CrossFunctionalSkills
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'cf_skill'
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementKnowledge.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee {uupic: line.UUPIC}), (elem:Knowledge {description: line.Knowledge})
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'knowledge'
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementTasks.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Task)
    WHERE emp.uupic = line.UUPIC AND elem.description = line.Tasks
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'task'
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementTechSkills.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Specific_Technology_Skills)
    WHERE emp.uupic = line.UUPIC AND elem.title = line.TechSkills
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'tech_skill'
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///elementWorkActivities.csv' AS line
    RETURN line
    ","
    MATCH (emp:Employee), (elem:Generalized_Work_Activities)
    WHERE emp.uupic = line.UUPIC AND elem.description = line.WorkActivities
    MERGE (emp)-[f:Found_In]->(elem)
    WITH toFloat(apoc.coll.shuffle(range(15,45,1))[0]*0.1) AS value, f
    SET f.datavalue = (round(100 * value) / 100),
        f.scale = 'IM',
        f.element = 'activity'
    ",{batchSize:10000}) YIELD operations""")

    # Update Center Inforation
    query_list.append("""MATCH (c:Center) WHERE c.center = 'HQ'SET c.title = 'Headquarters', c.business_area = toInteger(10);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'OIG' SET c.title = 'Office of the Inspector General', c.business_area = toInteger(99);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'NSSC' SET c.title = 'NASA Shared Services Center', c.business_area = toInteger(99);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'ARC' SET c.title = 'Ames Research Center', c.business_area = toInteger(21);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'GRC' SET c.title = 'Glenn Research Center', c.business_area = toInteger(22);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'LARC' SET c.title = 'Langley Research Center', c.business_area = toInteger(23);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'AFRC' SET c.title = 'Armstrong Filght Research Center', c.business_area = toInteger(24);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'GSFC' SET c.title = 'Goddard Space Flight Center', c.business_area = toInteger(51);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'MSFC' SET c.title = 'Marshall Space Flight Center', c.business_area = toInteger(62);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'SSC' SET c.title = 'Stennis Space Center', c.business_area = toInteger(64);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'JSC' SET c.title = 'Johnson Space Center', c.business_area = toInteger(72);""")
    query_list.append("""MATCH (c:Center) WHERE c.center = 'KSC' SET c.title = 'Kennedy Space Center', c.business_area = toInteger(74);""")

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
    ",{batchSize:10000}) YIELD operations""")

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
    ",{batchSize:10000}) YIELD operations""")

    query_list.append("""CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS
    FROM 'file:///OPMCompetencyLibrary.csv' AS line
    RETURN line
    ","
    MERGE (comp:Competency {compid: toInteger(line.id)})
    ON CREATE SET comp.title = line.CompetencyTitle,
    comp.description = line.CompetencyDefinition,
    comp.source = 'OPM'
    ",{batchSize:10000, parallel:true, retries: 10}) YIELD operations""")


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

    query_list.append("""CREATE INDEX workrole FOR (w:Workrole) ON (w.onet_soc_code, w.title)""")
    query_list.append("""CREATE INDEX occupation FOR (o:Occupation) ON (o.onet_soc_code, o.title)""")
    query_list.append("""CALL db.index.fulltext.createNodeIndex('Employees', ['Employee'], ['fname', 'lname', 'email', 'status', 'grade', 'type', 'date_position', 'accession', 'service_years', 'uupic'])""")
    query_list.append("""CALL db.index.fulltext.createNodeIndex("emp_occ_ksatt",["Employee", "Occupation", "Element"],['fname', 'lname', "title", "description"])""")

    return query_list

############# EXECUTE QUERIES THROUGH A LOOP #############
############# runs query, logs query execution time, sends update message about query progress #############

def execute_queries():
    # query status box initialization
    layout = [[gui.Text('EXECUTING QUERIES', font=(standard_font))],
            [gui.Text('', size=(50, 1), font=(standard_font), key='query_count')],
            [gui.Text('', size=(50, 1), font=(standard_font), key='query_time')]]
    window = gui.Window('Progress Updates', layout, finalize=True)

    query_counter = 1 # number of queries, manually increment through loop
    total_queries = len(query_list)
    query_times = [0] # list to track execution time of queries (used for logging)

    for query in query_list:
        query_time_start = time.perf_counter() # start individual query exection timer
        g = graph.begin() # open transaction
        result = g.run(query).to_data_frame() # execute query
        query_time_stop = time.perf_counter() # stop individual query exection timer
        query_times.append(query_time_stop - query_time_start) # add query execution time to the list
        try:
            if result['operations'][0]['failed'] > 0:
                #logging
                print('Could not finish query ' + str(query_counter) + '/' + str(total_queries))
                window.read(timeout=0.1) #timeout was the make or break piece
                window['query_count'].update('Could not finish query ' + str(query_counter) + '/' + str(total_queries))
            else:
                g.commit() # close transaction
                #logging
                print('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
                window.read(timeout=0.1) #timeout was the make or break piece
                window['query_count'].update('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
        except Exception as e:
            update(e)
            g.commit() # close transaction
            #logging
            print('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
            window.read(timeout=0.1) #timeout was the make or break piece
            window['query_count'].update('Completed query ' + str(query_counter) + '/' + str(total_queries) + f' in {query_times[query_counter]:0.4f} seconds.')
        query_counter += 1
    
    time.sleep(5)
    window.close()

############# SIMILARITY RELATIONSHIPS #############
def similar_relationships():
    update('Creating similar_to relationships.')

    #clear out gds graphs
    graphs = graph.run("""CALL gds.graph.list""").data()
    for g in graphs:
        if g['graphName'] == 'Similar_Trio':
            graph.run("""CALL gds.graph.drop('Similar_Trio')""")
        if g['graphName'] == 'Similar_Elements':
            graph.run("""CALL gds.graph.drop('Similar_Elements')""")

    update('Starting similarities between employees, occupations, and workroles.')

    #employees & occupations & workroles
    graph.run("""MATCH (n:Occupation) SET n :Employee""") #make all occupations -> employees
    graph.run("""MATCH (n:Workrole) SET n :Employee""") #make all workroles -> employees

    cypher_projection = """CALL gds.graph.create.cypher('Similar_Trio', 
                        'MATCH (n) WHERE (n:OPMSeries) OR (n:Employee) OR (n:Element) RETURN id(n) AS id', 
                        'MATCH (ncc:NASAClassCode)-[]-(opm:OPMSeries)-[r:IN_OPM_Series]-(s:Employee) WITH opm, s 
                        MATCH (s)-[f:Found_In]-(t:Element) WHERE f.datavalue > 3.49 AND f.scale = "IM" 
                        RETURN id(s) AS source, id(t) AS target')"""
    graph.run(cypher_projection)

    stream = """CALL gds.nodeSimilarity.stream('Similar_Trio', {similarityCutoff:0.5, degreeCutoff:1}) YIELD node1, node2, similarity
        WITH gds.util.asNode(node1).uupic AS uupic1, gds.util.asNode(node2).uupic AS uupic2, gds.util.asNode(node1).title AS job1, gds.util.asNode(node2).title AS job2, similarity
        RETURN similarity, uupic1, uupic2, job1, job2 ORDER BY similarity ASCENDING, uupic1, uupic2, job1, job2"""
    stream_results = graph.run(stream).data()

    for result in stream_results:
        graph.run("""MATCH (e1:Employee) WHERE e1.uupic='{}' OR e1.title='{}'
                    MATCH (e2:Employee) WHERE e2.uupic='{}' OR e2.title='{}'
                    MERGE (e1)-[s:similar_to]-(e2) SET s.datavalue = toFloat({})""".format(result['uupic1'], result['job1'], result['uupic2'], result['job2'], result['similarity']))

    graph.run("""CALL gds.graph.drop('Similar_Trio')""")

    graph.run("""MATCH (n:Occupation) REMOVE n :Employee""") #revert all occupations to only have one label
    graph.run("""MATCH (n:Workrole) REMOVE n :Employee""") #revert all workroles to only have one label

    update('Starting similarities between elements.')

    #skills & skills
    cypher_projection = """CALL gds.graph.create.cypher('Similar_Elements', 
                        'MATCH (n) WHERE (n:Occupation) OR (n:Element) RETURN id(n) AS id', 
                        'MATCH (s:Element)-[f:Found_In]-(t:Occupation) WHERE f.datavalue > 3.49 AND f.scale = "IM" 
                        RETURN id(s) AS source, id(t) AS target')"""
    graph.run(cypher_projection)

    stream = """CALL gds.nodeSimilarity.stream('Similar_Elements', {similarityCutoff:0.5, degreeCutoff:1}) YIELD node1, node2, similarity
        WITH gds.util.asNode(node1).title AS skill1, gds.util.asNode(node2).title AS skill2, similarity
        RETURN similarity, skill1, skill2 ORDER BY similarity ASCENDING, skill1, skill2"""
    stream_results = graph.run(stream).data()

    for result in stream_results:
        graph.run("""MATCH (e1:Element) WHERE e1.title='{}'
                    MATCH (e2:Element) WHERE e2.title='{}'
                    MERGE (e1)-[s:similar_to]-(e2) SET s.datavalue = toFloat({})""".format(result['skill1'], result['skill2'], result['similarity']))

    graph.run("""CALL gds.graph.drop('Similar_Elements')""")

    #remove any accidental self-directed relationships
    graph.run("""MATCH (n)-[r:similar_to]-(n) DELETE r""")

    update('Done creating similarity relationships.')

############# DOC2VEC RELATIONSHIPS BETWEEN COMPETENCIES AND KSATTS #############

################ LOAD EXISTING MODEL (OR CREATE ONE) ################
def load_model():
    model = None
    #load existing doc2vec model
    try:
        model = Doc2Vec.load("doc2vec.model")
    except:
        update('COULD NOT LOAD DOC2VEC MODEL, BUILDING ONE INSTEAD.')
    if not model:
        model = create_model()
        update('Finished creating Doc2Vec Model.')
    else:
        update("USING EXISITNG DOC2VEC MODEL.")
    return model

################ TRAIN DOC2VEC ON ELEMENTS_AND_COMPETENCIES_VOCAB ################
def create_model(dm=0, vector_size=300, min_count=3, epochs=15, window=4, dbow_words=1):
    model = None
    elements_and_competencies_vocab = ("""
        MATCH (n) WHERE EXISTS(n.elementID)
        RETURN n.elementID AS ElementID, labels(n) AS Group, n.title AS Title, n.description AS Description
        UNION
        MATCH (n) WHERE EXISTS(n.taskID)
        RETURN DISTINCT n.taskID AS ElementID, labels(n) AS Group, n.description AS Title, n.description AS Description
        UNION
        MATCH (n) WHERE EXISTS(n.commodityID)
        RETURN n.commodityID AS ElementID, labels(n) AS Group, n.title AS Title, n.title AS Description
        UNION
        MATCH (n:Tech_Skill_Product)
        RETURN id(n) AS ElementID, labels(n) AS Group, n.title AS Title, n.title AS Description
        UNION
        MATCH (n:Tool_Product)
        RETURN id(n) AS ElementID, labels(n) AS Group, n.title AS Title, n.title AS Description
        """)
    vocab = graph.run(elements_and_competencies_vocab).to_data_frame()
    vocab.to_csv("elements_and_competencies_vocab.csv") #save as csv 
    # Create ID for Elements and Occupations
    vocab["ESentID"] = vocab["Title"].map(str)
    element_df = vocab[["ESentID", "Description"]]
    element_df['ESentID'] = element_df['ESentID'].astype(str)
    element_df['Description'] = element_df['Description'].astype(str)
    # Converts the column to a list then joins the lists into one
    element_df['sent'] = element_df['Description'].apply(sent_tokenize)
    element_df['sent'] = element_df['sent'].astype(str)
    # Tagged Document
    element_sent_tagged = element_df.apply(lambda r: TaggedDocument(words=tokenize_text(r['sent']), tags=[r.ESentID]), axis=1)
    element_sent_tagged = element_sent_tagged.tolist()
    # Build and Save the Model
    train_model_start = time.perf_counter()
    model = Doc2Vec(dm=dm, workers=multiprocessing.cpu_count(), vector_size=vector_size, min_count=min_count, epochs=epochs, window=window, dbow_words=dbow_words)
    model.build_vocab(element_sent_tagged)
    model.train(element_sent_tagged, total_examples=model.corpus_count, epochs=model.epochs)
    train_model_end = time.perf_counter()
    update(f'TIME TAKEN TO TRAIN MODEL: {train_model_end-train_model_start:0.4f}s')
    model.save("doc2vec.model")
    return model

################ TOKENIZE ################
# Function to Create Tagged Documents for each df
def tokenize_text(text):
    tokens = []
    for sent in nltk.sent_tokenize(text):
        for word in nltk.word_tokenize(sent):
            if len(word) < 2:
                continue
            tokens.append(word.lower())
    return tokens

# tokenize and sanitize a string
def cleanString(string):
    stops = set(stopwords.words("english"))
    token = word_tokenize(string.lower()) #tokenize
    words = []
    for w in token:
        if not w in stops: #don't include stop words
            w = (re.sub('[^A-Za-z0-9]+', '', w).lower()).strip() #remove punc & special chars
            if w:
                words.append(w)
    return words

def tag_node_descriptions(nodetype): #nodetype='Competency' or 'Element'
    update('Tagging node descriptions for '+nodetype)

    if nodetype == 'Element':
        descriptions = graph.run("""
            MATCH (n) WHERE EXISTS(n.elementID) RETURN DISTINCT n.title AS Title, n.description AS Description
            UNION MATCH (n) WHERE EXISTS(n.taskID) RETURN DISTINCT n.description AS Title, n.description AS Description
            UNION MATCH (n) WHERE EXISTS(n.commodityID) RETURN DISTINCT n.title AS Title, n.title AS Description
            UNION MATCH (n:Tech_Skill_Product) RETURN DISTINCT n.title AS Title, n.title AS Description
            UNION MATCH (n:Tool_Product) RETURN DISTINCT n.title AS Title, n.title AS Description
        """).data()
    elif nodetype == 'Competency':
        descriptions = graph.run("""MATCH (n:Competency) RETURN n.title AS Title, n.description AS Description""").data()
    tagged_descriptions = []
    description_titles = []
    for index, description in enumerate(descriptions):
        if description['Description'] is None:
            desc = description['Title']
        else:
            desc = description['Description']
        clean_description = cleanString(desc) #clean
        tagged_descriptions.append(TaggedDocument(clean_description, [index])) #tag
        description_titles.append(description['Title']) #keep track of the corresponding node titles
    return tagged_descriptions, description_titles

def competency_relationships_csv(tagdoc1, tagdoc2, doctitles1, doctitles2):  
    update('Creating dataframe with similarity relationships between competencies and ksatts.')
    doctitles_1 = []
    doctitles_2 = []
    sims = []
    #loop through all docs
    for esent_id_1 in range(0, len(tagdoc1)-1): #620
        #loop through all docs
        print("progressing: "+str(esent_id_1+1)+"/"+str(len(tagdoc1)))
        for esent_id_2 in range(0, len(tagdoc2)-1): #about 2k
            #if docs aren't the same one, find similarity between them
            if esent_id_1 != esent_id_2:
                doctitles_1.append(doctitles1[esent_id_1])
                doctitles_2.append(doctitles2[esent_id_2])
                desc1_vector = model.infer_vector(tagdoc1[esent_id_1].words) 
                desc2_vector = model.infer_vector(tagdoc2[esent_id_2].words)
                sim = 1 - spatial.distance.cosine(desc1_vector, desc2_vector)
                sims.append(sim)
                
    sim_dataframe = pd.DataFrame([(doctitles_1[i], doctitles_2[i], sim) for i, sim in enumerate(sims)], 
                                columns=['Compentency1', 'Compentency2', 'Similarity'])
    sim_dataframe= sim_dataframe.sort_values(by=['Similarity'], ascending=False)
    update('Finished creating dataframe with similarity relationships between competencies and ksatts.')
    return sim_dataframe

def create_comp_ele_similarities():    
    update('Creating similarity relationships.')
    query = """CALL apoc.periodic.iterate("
    LOAD CSV WITH HEADERS 
    FROM 'file:///compentency_element_similarities.csv' AS line
    RETURN line
    ","
    MATCH (a) WHERE a.title = (line.`Compentency1`)
    MATCH (b) WHERE b.title = (line.`Compentency2`)
    MERGE (a)-[r:similarity]-(b) ON CREATE SET r.datavalue = (line.`Similarity`)
    ",{batchSize:1000}) YIELD operations"""
    g = graph.begin() # open transaction
    result = g.run(query).to_data_frame() # execute query
    g.commit() # close transaction
    update('Finished creating similarity relationships.')

if __name__ == "__main__":
    standard_font = 'Courier', 16
    graph = None
    query_list = []
    log_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'logs'))
    log_file = open(os.path.join(log_path, 'script_log_file.txt'), 'w+') # create query exection time log file

    port, user, pswd, path, firstrun = present_gui()
    total_program_time_start = time.perf_counter() # start timer to log total program time
    file_process_time_start = time.perf_counter() # start timer to log file processing time
    import_onet_data(path)
    file_process_time_stop = time.perf_counter() # stop timer to log file processing time

    total_queries_time_start = time.perf_counter() # start timer to log query run time
    graph = connect_to_database(port, user, pswd)
    query_list = append_queries(firstrun)
    execute_queries()
    total_queries_time_stop = time.perf_counter() # stop timer to log query run time

    sim_rel_time_start = time.perf_counter() # start timer to log creation of similarity relationships
    similar_relationships()
    sim_rel_time_stop = time.perf_counter() # stop timer to log creation of similarity relationships

    comp_ksatt_time_start = time.perf_counter() # start timer to log creation of compentency-ksatt relationships
    filename = "compentency_element_similarities.csv"
    import_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'import'))
    current_path = os.path.abspath(os.path.join(os.path.dirname( __file__ )))
    if not os.path.exists(os.path.join(import_path, filename)):
        model = load_model()
        tagdoc1, doctitles1 = tag_node_descriptions('Competency')
        tagdoc2, doctitles2 = tag_node_descriptions('Element')
        compentency_element_similarities = competency_relationships_csv(tagdoc1, tagdoc2, doctitles1, doctitles2)
        compentency_element_similarities.to_csv(filename)
        os.rename(os.path.join(current_path, filename), os.path.join(import_path, filename))
        update('Saved compentency_element_similarities.csv to import folder.')
    create_comp_ele_similarities()
    comp_ksatt_time_stop = time.perf_counter() # stop timer to log creation of compentency-ksatt relationships
    total_program_time_stop = time.perf_counter() # stop timer to log total program time

    ############# FINISHED EVERYTHING, WRAP UP LOGGING #############

    # finish clocking process time
    total_program_time_stop = time.perf_counter() # stop timer to log total program time

    # final logs
    total_time_message = f'All updates took a total of: {total_program_time_stop - total_program_time_start:0.4f} seconds.\n'
    file_time_message = f'File processing took: {file_process_time_stop - file_process_time_start:0.4f} seconds.\n'
    query_time_message = f'Queries took: {total_queries_time_stop - total_queries_time_start:0.4f} seconds.\n'
    sim_rel_message = f'Similarity relationship creation took: {sim_rel_time_stop - sim_rel_time_start:0.4f} seconds.\n'
    comp_ksatt_message = f'Competency & KSATT relationship creation took: {comp_ksatt_time_stop - comp_ksatt_time_start:0.4f} seconds.\n'

    log_file.write(total_time_message)
    log_file.write(file_time_message)
    log_file.write(query_time_message)
    log_file.write(sim_rel_message)
    log_file.write(comp_ksatt_message)
    log_file.close()

    print('SUCCESS: Completed building the database.')
    success = gui.Window(' ', [[gui.Text('SUCCESS: Completed building the database.', font=(standard_font))],
                                [gui.Text(total_time_message, font=(standard_font))],
                                [gui.Text(file_time_message, font=(standard_font))],
                                [gui.Text(query_time_message, font=(standard_font))],
                                [gui.Text(sim_rel_message, font=(standard_font))],
                                [gui.Text(comp_ksatt_message, font=(standard_font))],
                                [gui.Text('Full log of individual query times and this summary in query_times_and_summary_logs.txt, under "logs" folder.', font=(standard_font))],
                                [gui.Text('Close this window to finish program, and make sure to deactivate your virtualenv.', font=(standard_font))]])
    if success.read() == gui.WIN_CLOSED:
        success.close()
