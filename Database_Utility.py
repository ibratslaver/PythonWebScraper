'''
Created on Feb 7, 2019

@author: Igor
'''

import random
import socket

import pyodbc

import Database_Config


#from Database_Config import *
crawler_name= socket.gethostbyname(socket.gethostname()) +":"+str(random.randint(1,100000000))

trusted_connection ='no'
driver='{SQL Server}'
server = Database_Config.ip_address
database= 'RateMyProfessor'
uid = Database_Config.user
pwd = Database_Config.password
start_num = Database_Config.start_num
end_num = Database_Config.end_num
start_num_schools = Database_Config.start_num_schools
end_num_schools = Database_Config.end_num_schools



# trusted_connection = 'yes'
# driver = '{SQL Server}',
# server = 'DESKTOP-F0ROTPP\SQLEXPRESS'
# database = 'Igor_DB'



#con = pyodbc.connect(Trusted_Connection='yes', driver = '{SQL Server}',server = 'GANESHA\SQLEXPRESS' , database = '4YP')

 #Integrated_Security=False) 

#CREATE DATABASE CONNECTION 

def create_local_connection():
    database_connect = pyodbc.connect(Trusted_Connection='yes', driver='{SQL Server}', server='DESKTOP-F0ROTPP\SQLEXPRESS' , database='Igor_DB')
    return database_connect

def create_database_connection():
    database_connect = pyodbc.connect(Trusted_Connection=trusted_connection, driver=driver,server=server , database=database, uid=uid, pwd=pwd, Integrated_Security=False) 
    return database_connect

#FUNCTIONS RELATED TO PROFESSOR_DATA
def select_available_target_urls():
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "select top 1 target_url from task_table where crawler_name is null and task_completion_time is null and task_id between start_num and end_num"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("start_num", start_num).replace("end_num", end_num)
    query = SQL_TEMPLATE
    c.execute(query)
    target_urls = c.fetchall()
    #print(target_urls)
    return target_urls, query #use query object in update_task_table_with_current_crawler

def insert_reviews(review_id, professor_id, review_text, review_date, class_name, overall_quality_score, for_credit, attendance, textbook_used, would_take_again, grade_received, comment_useful_nbr, comment_not_useful_nbr, online_class):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN3$', '$COLUMN4$', '$COLUMN5$', '$COLUMN6$', '$COLUMN7$', '$COLUMN8$', '$COLUMN9$', '$COLUMN10$', '$COLUMN11$', '$COLUMN12$', '$COLUMN13$', '$COLUMN14$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","reviews").replace("$COLUMN1$",review_id).replace("$COLUMN$2",professor_id).replace("$COLUMN3$",review_text).replace("$COLUMN4$",review_date).replace("$COLUMN5$",class_name).replace("$COLUMN6$",overall_quality_score).replace("$COLUMN7$",for_credit).replace("$COLUMN8$",attendance).replace("$COLUMN9$",textbook_used).replace("$COLUMN10$",would_take_again).replace("$COLUMN11$",grade_received).replace("$COLUMN12$",comment_useful_nbr).replace("$COLUMN13$",comment_not_useful_nbr).replace("$COLUMN14$",online_class)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()

def insert_professors(professor_id, professor_name, overall_quality, would_take_again, level_of_difficulty, school_id, department):    
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN3$', $COLUMN4$, '$COLUMN5$', '$COLUMN6$', '$COLUMN7$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","professors").replace("$COLUMN1$",professor_id).replace("$COLUMN$2",professor_name).replace("$COLUMN3$",overall_quality).replace("$COLUMN4$",would_take_again).replace("$COLUMN5$",level_of_difficulty).replace("$COLUMN6$",school_id).replace("$COLUMN7$",department)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def insert_professor_tags(professor_id, tag_name, nbr_instances):    
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN$3')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","professor_tags").replace("$COLUMN1$",professor_id).replace("$COLUMN$2",tag_name).replace("$COLUMN$3",nbr_instances)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()   
    
def insert_review_tags(review_id, tag_name):    
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","review_tags").replace("$COLUMN1$",review_id).replace("$COLUMN$2",tag_name)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def insert_task_table(task_id, target_url, crawler_name, task_completion_time, page_number):    
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN2$', $COLUMN3$, $COLUMN4$, '$COLUMN5$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","task_table").replace("$COLUMN1$",task_id).replace("$COLUMN2$",target_url).replace("$COLUMN3$",crawler_name).replace("$COLUMN4$",task_completion_time).replace("$COLUMN5$",page_number)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def calculate_nbr_pending_tasks():
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "select count(*) from task_table where crawler_name is null and task_id between start_num and end_num"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("start_num", start_num).replace("end_num", end_num)
    c.execute(SQL_TEMPLATE)
    nbr_tasks = c.fetchone()
    for row in nbr_tasks:
        return(row)
    
def task_table_record_count():
    conn = create_database_connection()
    c = conn.cursor()
    c.execute("select count(*) from task_table")
    nbr_tasks = c.fetchone()
    for row in nbr_tasks:
        return(row)
    
def task_table_max_page_num():
    conn = create_database_connection()
    c = conn.cursor()
    c.execute("select max(page_number) from task_table")
    nbr_tasks = c.fetchone()
    for row in nbr_tasks:
        return(row)
    
def task_table_max_task_id():
    conn = create_database_connection()
    c = conn.cursor()
    c.execute("select max(task_id) from task_table")
    nbr_tasks = c.fetchone()
    for row in nbr_tasks:
        return(row)

def update_task_table_with_current_crawler(starting_url):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "UPDATE task_table set crawler_name = '$CRAWLER_NAME$' WHERE target_url ='$PAGEURL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$CRAWLER_NAME$", crawler_name).replace("$PAGEURL$",starting_url)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def update_task_table_with_completion_date_time(completion_date_time, starting_url):
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "UPDATE task_table set task_completion_time = '$COMPLETION_DATE_TIME$' WHERE target_url = '$TARGET_URL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$COMPLETION_DATE_TIME$", completion_date_time).replace("$TARGET_URL$", starting_url)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def update_crawler_name_to_null_for_failed_task(starting_url):
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "UPDATE task_table set crawler_name = $NULL$ WHERE target_url = '$TARGET_URL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$NULL$", "NULL").replace("$TARGET_URL$",starting_url)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def get_task_id(starting_url):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "SELECT TASK_ID FROM task_table WHERE TARGET_URL='$PAGEURL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$PAGEURL$",starting_url)
    #print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    task_id = c.fetchone()
    for row in task_id:
        return str(row)
    
#FUNCTIONS RELATED TO SCHOOLS
def insert_school_reviews(review_id,school_id,review_date,reputation_score,location_score,internet_score,food_score,facilities_score,social_score,happiness_score,opportunities_score,clubs_score,safety_score,comments,comment_useful_nbr,comment_not_useful_nbr):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN3$', '$COLUMN4$', '$COLUMN5$', '$COLUMN6$', '$COLUMN7$', '$COLUMN8$', '$COLUMN9$', '$COLUMN10$', '$COLUMN11$', '$COLUMN12$', $COLUMN13$, '$COLUMN14$', '$COLUMN15$', '$COLUMN16$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","school_reviews").replace("$COLUMN1$",review_id).replace("$COLUMN$2",school_id).replace("$COLUMN3$",review_date).replace("$COLUMN4$",reputation_score).replace("$COLUMN5$",location_score).replace("$COLUMN6$",internet_score).replace("$COLUMN7$",food_score).replace("$COLUMN8$",facilities_score).replace("$COLUMN9$",social_score).replace("$COLUMN10$",happiness_score).replace("$COLUMN11$",opportunities_score).replace("$COLUMN12$",clubs_score).replace("$COLUMN13$",safety_score).replace("$COLUMN14$",comments).replace("$COLUMN15$",comment_useful_nbr).replace("$COLUMN16$",comment_not_useful_nbr)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def insert_schools(school_id,name,location):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN3$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","schools").replace("$COLUMN1$",school_id).replace("$COLUMN$2",name).replace("$COLUMN3$",location)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def insert_school_task_table(task_id,target_url,crawler_name,task_completion_time,page_num):
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN2$', $COLUMN3$, $COLUMN4$, '$COLUMN5$')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$","task_table_schools").replace("$COLUMN1$",task_id).replace("$COLUMN2$",target_url).replace("$COLUMN3$",crawler_name).replace("$COLUMN4$",task_completion_time).replace("$COLUMN5$",page_num)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def select_available_target_urls_schools():
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "select top 1 target_url from task_table_schools where crawler_name is null and task_completion_time is null and task_id between start_num and end_num"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("start_num", start_num_schools).replace("end_num", end_num_schools)
    query = SQL_TEMPLATE
    c.execute(query)
    target_url = c.fetchall()
    return target_url

def calculate_nbr_pending_tasks_schools():
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "select count(*) from task_table_schools where crawler_name is null and task_id between start_num and end_num"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("start_num", start_num_schools).replace("end_num", end_num_schools)
    #print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    nbr_tasks = c.fetchone()
    for row in nbr_tasks:
        return(row)

    
def update_task_table_with_current_crawler_schools(starting_url):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "UPDATE task_table_schools set crawler_name = '$CRAWLER_NAME$' WHERE target_url ='$PAGEURL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$CRAWLER_NAME$", crawler_name).replace("$PAGEURL$",starting_url)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
  
def update_task_table_with_completion_date_time_schools(completion_date_time, starting_url):
    conn = create_database_connection() 
    c = conn.cursor()
    SQL_TEMPLATE = "UPDATE task_table_schools set task_completion_time = '$COMPLETION_DATE_TIME$' WHERE target_url = '$TARGET_URL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$COMPLETION_DATE_TIME$", completion_date_time).replace("$TARGET_URL$", starting_url)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()
    
def get_task_id_schools(starting_url):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "SELECT TASK_ID FROM task_table_schools WHERE TARGET_URL='$PAGEURL$'"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$PAGEURL$",starting_url)
    #print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    task_id = c.fetchone()
    for row in task_id:
        return str(row)
    
    
def insert_review_sentences(review_id, sentence):
    conn = create_database_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN$3')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$", "review_sentences").replace('$COLUMN1$', review_id).replace('$COLUMN$2', sentence).replace('$COLUMN$3','NULL')
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()


def insert_ga_feedback_sentences(feedback_id,sentence,date,ga_recommend):
    conn = create_local_connection()
    c = conn.cursor()
    SQL_TEMPLATE = "INSERT INTO $TABLENAME$ VALUES ('$COLUMN1$', '$COLUMN$2', '$COLUMN$3', '$COLUMN$4')"
    SQL_TEMPLATE = SQL_TEMPLATE.replace("$TABLENAME$", "feedback_sentences").replace('$COLUMN1$', feedback_id).replace('$COLUMN$2', sentence).replace('$COLUMN$3',date).replace('$COLUMN$4', ga_recommend)
    print(SQL_TEMPLATE)
    c.execute(SQL_TEMPLATE)
    conn.commit()
    conn.close()




#insert_review_sentences('1',"She''s so interesting and friendly!!")
#insert_review_sentences('1',"She''s so interesting and friendly!!")




