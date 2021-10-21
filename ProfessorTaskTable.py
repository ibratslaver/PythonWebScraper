'''
Created on Mar 8, 2019

@author: Igor
'''

from Database_Utility import *
from datetime import datetime
import re
import time

import requests

from Database_Config import *


starting_task_table_url = "http://www.ratemyprofessors.com/search.jsp?queryBy=teacherName&queryoption=HEADER&facetSearch=true"
base_professor_url = "http://www.ratemyprofessors.com/ShowRatings.jsp?tid=$ID$"
starting_url = base_professor_url.replace("$ID$",str(2))
starting_task_table_response = requests.get(starting_task_table_url)
starting_task_table_content = str(starting_task_table_response.content)

task_table_num_professor= re.compile(r"Showing.*? of (\d+)\s",re.S|re.I).findall(starting_task_table_content)
#print(task_table_num_professor)
task_table_num_professor = int(task_table_num_professor[0])
#print(task_table_num_professor)
task_table_num_pages = task_table_num_professor //20 + 1
#print(task_table_num_pages)
offset_end_num = 20* (task_table_num_pages-1)
#print(offset_end_num)

starting_task_table_url = "http://www.ratemyprofessors.com/search.jsp?queryBy=teacherName&facetSearch=true&schoolName=&offset=$NUM$&max=20"

if task_table_record_count() == 0:
    offset = 0
    task_id = 0
    page_num = 1
    crawler_name = "NULL"
    task_completion_time = "NULL"
    i=0
else:
    # Offset starts at 0, which is why we subtract 20
    offset = task_table_max_page_num()*20 - 20
    
    task_id = task_table_max_task_id()
    page_num = task_table_max_page_num()
    crawler_name = "NULL"
    task_completion_time = "NULL"
    #i represents the number of pages, so it's the same as page number
    i = task_table_max_page_num()

''' UPDATING VARIABLES FOR TASK TABLE IF FALIURE OCCURS

* Reset i to the page number
* Offset (which represents which base page were on), should be equal to the page_nbr*20 - 20 to
account for the fact that it starts at 0 and -20 to account for the fact that i increment it
if last successfull task is end of page, then offset should page_nbr+1*20
1st page has an offset of 0, so page_num * offset gives you one page ahead
* Task_ID should be set to the current task id value (gets incremented in loop)
* Page Nbr should be set to the previous page nbr (bc it gets incremented in loop). If 
last successfull task is end of page, keep it at the current page number
'''


while i <= task_table_num_pages:
    page_url = starting_task_table_url.replace("$NUM$", str(offset))
    page_url = str(requests.get(page_url).content)
    professor_page_ids = re.compile(r"ShowRatings.jsp\?tid=(\d+)\"",re.S|re.I).findall(page_url)
    for professor_page_id in professor_page_ids:
        target_url = base_professor_url.replace("$ID$",professor_page_id)
        task_id +=1
        insert_task_table(str(task_id),target_url,crawler_name,task_completion_time,str(page_num))
    offset+=20
    page_num+=1
    i+=1

    


