# Projects

This project scrapes professor and school data from ratemyprofessors.com and inserts it into sql server tables. Below is a description of each files:

# Scripts
Database_Config.py - read the config variables from the .ini file <br />
Database_Utility.py - a library of database functions to execute sql queries <br />
ProfessorTaskTable.py - populates a task table (list of urls) that is used by RateMyProfessor.py & RatemyProfessorSchools.py <br />
RateMyProfessor.py - Populates the the professor data (professor info, reviews, etc.) <br />
RateMyProfessorSchools.py - populates the school data (reputation, food, etc.) <br />
setup.py - This turns the RateMyProfessor.py and RatemyProfessorSchools.py into executables. This way you don't need to have python installed on your computer
to run the code
