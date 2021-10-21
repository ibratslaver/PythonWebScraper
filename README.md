# Projects

This project scrapes professor and school data from ratemyprofessors.com and inserts it into sql server tables. Below is a description of each files:

Database_Config.py - read the config variables from the .ini file
Database_Utility.py - a library of database functions to execute sql queries
ProfessorTaskTable.py - populates a task table (list of urls) that is used by RateMyProfessor.py & RatemyProfessorSchools.py
RateMyProfessor.py - Populates the the professor data (professor info, reviews, etc.)
RateMyProfessorSchools.py - populates the school data (reputation, food, etc.)
setup.py - This turns the RateMyProfessor.py and RatemyProfessorSchools.py into executables. This way you don't need to have python installed on your computer
to run the code
