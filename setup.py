#Setup.py
import os
import sys

from cx_Freeze import setup, Executable


build_exe_options = {"packages":["re","requests","time","datetime","pyodbc","socket","multiprocessing","idna","os","sys"],
                     "include_files":["DB Config File.ini"]}


base=None
if sys.platform == "win32":
    base = "Console"

setup(name='RateMyProfesors'
      , version = '5.1'
      , description = 'Collect Data from RateMyProfessors'
      , options = {"build_exe":build_exe_options}
      , executables = [Executable("RateMyProfessor.py",base = base),Executable("RateMyProfessorSchools.py",base=base), Executable("ProfessorTaskTable.py",base=base)]
      )

#,Executable("RateMyProfessorSchools.py",base=base)]