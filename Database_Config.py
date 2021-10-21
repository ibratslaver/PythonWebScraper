import configparser
import os
import sys


#os.chdir(r'C:\Users\Igor\Desktop')
with open("DB Config File.ini", "r") as myfile:
   data = myfile.readlines()

config = configparser.ConfigParser()
config.read_file(open(r"DB Config File.ini"))
#config.read_file(open(r'c:\Users\Igor\eclipse-workspace\Independent Study - Web Carwling\Core Modules\DB Config File.ini'))
path1 = config.get('My Section', 'ip_address')
path2 = config.get('My Section', 'user')
path3 = config.get('My Section', 'password')
path4 = config.get('My Section', 'start_num')
path5 = config.get('My Section', 'end_num')
path6 = config.get('My Section', 'start_num_schools')
path7 = config.get('My Section', 'end_num_schools')

ip_address = path1
user = path2
password = path3
start_num = path4
end_num = path5
start_num_schools = path6
end_num_schools = path7





