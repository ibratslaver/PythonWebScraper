'''
Created on Mar 2, 2019

@author: Igor
'''

'''
Created on Feb 21, 2019

@author: Igor
'''

'''
base_school_url = "http://www.ratemyprofessors.com/campusRatings.jsp?sid=$SCHOOLID$"
starting_task_table_url = "http://www.ratemyprofessors.com/search.jsp?query=&queryoption=HEADER&stateselect=&country=&dept=&queryBy=schoolName&facetSearch=true&schoolName=&offset=$NUM$&max=20"
url_to_calc_num_total_pages = starting_task_table_url.replace("$NUM$","0")
page = requests.get(url_to_calc_num_total_pages)
soup = BeautifulSoup(page.content,'html.parser')

#Calculate number of pages
results = soup.find_all('div',attrs={'class':'result-count'})
results = str(results[1]).split()
num_schools = results[4]
num_pages = int(num_schools)//20 + 1
offset = 0
crawler_name = "NULL"
task_completion_time = "NULL"
page_num = 1
task_id = 0

for i in range(num_pages):
    
    #Beautiful Soup for main page
    url = starting_task_table_url.replace("$NUM$",str(offset))
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    
    school_ids = []
    
    school_listings = soup.find_all('li',attrs = {'class':'listing SCHOOL'})
    for listing in school_listings:
        anchor = listing.find_all('a', attrs={'href':True})                      
        for a in anchor:
            school_id = a['href'][23:].strip()
            school_ids.append(school_id)
            
    for school_id in school_ids:
        target_url = base_school_url.replace("$SCHOOLID$",school_id)
        task_id+=1
        #print(task_id,target_url,crawler_name,task_completion_time,page_num)
        insert_school_task_table(str(task_id),target_url,crawler_name,task_completion_time,str(page_num))
        
    offset+=20
    page_num+=1
'''

from Database_Utility import *
from datetime import datetime
import re 
import time

from bs4 import BeautifulSoup
import requests


my_file = open("LogSchools.txt", "a")
with my_file:
    my_file.write("========================="+str(datetime.now())+"=========================\n")

base_paginate_url = "http://www.ratemyprofessors.com/campusrating/paginatecampusRatings?page=$PAGE_ID$&sid=$SCHOOL_ID$"

flag = calculate_nbr_pending_tasks_schools()
if flag >0:
    flag = True
else:
    flag = False

wait_time = 1
nbr_retries = 0
max_retries= 5
    
while flag == True:
    try:
        wait_time = 30
        target_url = select_available_target_urls_schools()
        target_url = target_url[0]
        for url in target_url:
            starting_url = url
            task_id = get_task_id_schools(starting_url)
            update_task_table_with_current_crawler_schools(starting_url) #assign a crawler name for the tasks about to be executed
            page = requests.get(starting_url)
            soup = BeautifulSoup(page.content, 'html.parser')
            
            #INSERT INTO SCHOOLS TABLE
            
            #School ID
            school_id = soup.find_all('meta', attrs={'property':'og:url', 'content':True})
            school_id = str(school_id).split()
            school_id = school_id[1]
            id_position = school_id.find('sid')+4
            school_id = school_id[id_position:].replace("\"","")
            
            #School Name
            result_text = soup.find_all('div', attrs={'class':'result-text'})
            for r in result_text:
                school_name = r.text
                school_name = school_name.strip()
            
            #Location
            result_text = soup.find('div', attrs={'class':'result-title'})
            location = result_text.find('span').text.strip()
            
            #Insert into schools table 
            insert_schools(school_id,school_name,location)
            
        
            #INSERT INTO SCHOOL REVIEWS TABLE
            
            #Get nbr of pages for School Page
            nbr_reviews = soup.find('div', attrs={'class':'table-toggle rating-count active h1'})
            nbr_reviews = nbr_reviews.text.split()
            nbr_pages = int(nbr_reviews[0])//20 +1
            
            #School ID, Need to calculate this using base page, doesn't exist on JSON output
            school_id = soup.find_all('meta', attrs={'property':'og:url', 'content':True})
            school_id = str(school_id).split()
            school_id = school_id[1]
            id_position = school_id.find('sid')+4
            school_id = school_id[id_position:].replace("\"","")
            
            for p in range(nbr_pages):
                p+=1
                paginate_url = base_paginate_url.replace("$SCHOOL_ID$",str(school_id)).replace("$PAGE_ID$",str(p)) 
                
                #Json Output
                response = requests.get(paginate_url)
                dict = response.json()
                reviews = dict['ratings']
                
                #INSERT REVIEWS TABLE
                attributes = ['id','crCreateDate', 'crSchoolReputation','crCampusLocation','crInternetSpeed','crFoodQuality',
                                              'crCampusCondition','crSocialActivities','crSchoolSatisfaction','crCareerOpportunities','crClubAndEventActivities','crSchoolSafety','crComments',
                                              'helpCount','notHelpCount']
                                              
                # Create List of Lists to store attributes
                nbr_attributes = len(attributes)
                attribute_lists = []
                for i in range(nbr_attributes):
                    attribute_lists.append([])
                    
                
                for i in range(nbr_attributes):
                        for review_list in reviews:
                            curr_attribute = attributes[i]
                            attribute_lists[i].append(review_list[curr_attribute])
                            
                
                review_id_values = attribute_lists[0]
                #print(review_id_values)
                create_date_values = attribute_lists[1]
                #print(create_date_values)
                reputation_values = attribute_lists[2]
                #print(reputation_values)
                location_values = attribute_lists[3]
                #print(location_values)
                internet_values = attribute_lists[4]
                #print(internet_values)
                food_values = attribute_lists[5]
                #print(food_values)
                facilities_values = attribute_lists[6]
                #print(facilities_values)
                social_values = attribute_lists[7]
                #print(social_values)
                happiness_values = attribute_lists[8]
                #print(happiness_values)
                opportunities_values = attribute_lists[9]
                #print(opportunities_values)
                club_values = attribute_lists[10]
                #print(club_values)
                safety_values = attribute_lists[11]
                #print(safety_values)
                comment_values = attribute_lists[12]
                #print(comment_values)
                comment_useful_nbr_values = attribute_lists[13]
                #print(comment_useful_nbr_values)
                comment_not_useful_nbr_values = attribute_lists[14]
                #print(comment_not_useful_nbr_values)
                
                #Dynamically get the number of elements in each page
                nbr_reviews_in_page = len(review_id_values)
                                
                for i in range(nbr_reviews_in_page):
                    review_id = str(review_id_values[i])
                    review_date = create_date_values[i]
                    reputation_score = str(reputation_values[i])
                    location_score = str(location_values[i])
                    internet_score = str(internet_values[i])
                    food_score = str(food_values[i])
                    facilities_score = str(facilities_values[i])
                    social_score = str(social_values[i])
                    happiness_score = str(happiness_values[i])
                    opportunities_score = str(opportunities_values[i])
                    clubs_score = str(club_values[i])
                    #Safety Score, not always captured
                    safety_score = str(safety_values[i])
                    if safety_score == "None":
                        safety_score = "NULL"
                    comments = comment_values[i].replace("\\r\\n","").replace("\\","").replace("'","''").strip()
                    comment_useful_nbr = str(comment_useful_nbr_values[i])
                    comment_not_useful_nbr = str(comment_not_useful_nbr_values[i])
                    
                    #School ID
                    school_id = soup.find_all('meta', attrs={'property':'og:url', 'content':True})
                    school_id = str(school_id).split()
                    school_id = school_id[1]
                    id_position = school_id.find('sid')+4
                    school_id = school_id[id_position:].replace("\"","")
                    
                    insert_school_reviews(review_id,school_id,review_date,reputation_score,location_score,internet_score,food_score,facilities_score,social_score,happiness_score,opportunities_score,clubs_score,safety_score,comments,comment_useful_nbr,comment_not_useful_nbr)
                    
            #completion time after review insert 
            completion_time = str(datetime.now())
                            
            #Update completion date times in task table
            update_task_table_with_completion_date_time_schools(completion_time,starting_url)
                
            flag = calculate_nbr_pending_tasks_schools()>0
                   
    #Catch errors
    except Exception as e:
        if e.__class__== requests.exceptions.ConnectionError:
            with open("logSchools.txt", "a") as my_file:
                my_file.write("CONNECTION Faliure for Task ID:" + task_id + str(e) + '\n'*2)
            update_crawler_name_to_null_for_failed_task(starting_url)
            time.sleep(wait_time)
            wait_time*=2
        elif e.__class__ == pyodbc.OperationalError:
            with open("LogSchools.txt", "a") as my_file:
                my_file.write("PYODBC Faliure for Task ID:" + task_id + str(e) + '\n'*2)
            update_crawler_name_to_null_for_failed_task(starting_url)
            time.sleep(wait_time)
            wait_time *=2 
        else:
            with open("LogSchools.txt", "a") as my_file:
                my_file.write("OTHER Faliure for Task ID:" + task_id + str(e) + '\n'*2)
            update_crawler_name_to_null_for_failed_task(starting_url)
            nbr_retries +=1
            if nbr_retries == max_retries:
                completion_time = "MAX_RETRY_FALIURE"
                update_task_table_with_completion_date_time(completion_time,starting_url)
                nbr_retries = 0
                wait_time = 1
            else:
                time.sleep(wait_time)
                wait_time +=1
 

#input('I think this is complete')




