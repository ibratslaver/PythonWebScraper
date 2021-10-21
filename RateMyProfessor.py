try:  
    import re
    import requests
    import time
    from datetime import datetime
    from Database_Utility import *
    from Database_Config import *
    
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    my_file = open("LogProfessor.txt", "a")
    with my_file:
        my_file.write("========================="+str(datetime.now())+"=========================\n")
        
    # Set Flag Variable
    if calculate_nbr_pending_tasks() >0:
        flag = True
    else:
        flag = False

        
    wait_time = 1
    nbr_retries = 0
    max_retries= 5
        
    while flag is True:
        try:
            target_urls = select_available_target_urls()
            target_urls = target_urls[0]
            for target_url in target_urls:
                starting_url = target_url[0]
                task_id = get_task_id(starting_url)
                update_task_table_with_current_crawler(starting_url) #assign a crawler name for the tasks about to be executed
                response=requests.get(starting_url)
                starting_page_content=str(response.content)
                page_chunk = []
                page_chunk.append(starting_page_content)
                
                #print(page_chunk)
                #Check for type of page
                #no_reviews_page = re.compile("Add A Review",re.S|re.I).findall(starting_page_content)
                #if len(no_reviews_page) == 1:
                
                #Number of Reviews
                total_reviews =re.compile("(\d+)\s+Student.*?Ratings",re.S|re.I).findall(starting_page_content)
                if len(total_reviews) == 0:
                    completion_time = 'No Data Inserted - New Professor'
                    update_task_table_with_completion_date_time(completion_time,starting_url)
                    flag = calculate_nbr_pending_tasks()>0
                else: 
                    num_total_reviews = int(total_reviews[0])
                    nbr_pages = num_total_reviews//20 + 1
    
                    # INSERT INTO PROFESSORS TABLE
                    for page in page_chunk:
                        
                        #Professor ID
                        professor_id = re.compile("professorID.*?\"(\d+)\"",re.S|re.I).findall(page)
                        professor_id = professor_id[0].replace("\\","").replace("'","''").strip()
                        
                        #Professor Name
                        professor_name = re.compile("<title>(.*?)\sat",re.S|re.I).findall(page)
                        professor_name = professor_name[0].replace("\\","").replace("'","''").strip()
                        #.replace("'","''")
                        #print(professor_name)
                        
                        #School ID
                        school_id = re.compile("schoolid\":\"(.*?)\"",re.S|re.I).findall(page)
                        school_id = school_id[0].replace("\\","").replace("'","''").strip()
                        #print(school_id)
                        
                        
                        #Department
                        department = re.compile("prop3\":\"(.*?)\"",re.S|re.I).findall(page)
                        department = department[0].replace("\\","").replace("'","''").strip()
                        #print(department)
                        
                        #Overall Quality
                        overall_quality = re.compile("title=\"\">([\d.]+?)<\/div>",re.S|re.I).findall(page)
                        overall_quality = overall_quality[0].replace("\\","").replace("'","''").strip()
                        #print(overall_quality)
                        
                        #Would Take Again Percent
                        would_take_again = re.compile("\"breakdown-section\stakeAgain\s(.*?)\"",re.S|re.I).findall(page)
                        #print(would_take_again)
                        if would_take_again[0] == "no-rating":
                            would_take_again = "NULL"
                        else:
                            would_take_again = str(int(would_take_again[0])/100)
                        #print(would_take_again)
                        
                        #Level of Difficulty
                        level_of_difficulty = re.compile("breakdown-section difficulty\".*?([\d.]+)",re.S|re.I).findall(page)
                        level_of_difficulty = level_of_difficulty[0].replace("\\","").replace("'","''").strip()
                        #print(level_of_difficulty)
                        
                        #Insert into professors table
                        insert_professors(professor_id,professor_name,overall_quality,would_take_again,level_of_difficulty,school_id,department)
                    
                    
                    # INSERT INTO PROFESSOR_TAGS TABLE
                    for page in page_chunk:
                        
                        # Professor Tags & ID
                        professor_tags = re.compile("choosetags\">(.*?)\s<",re.S|re.I).findall(page)
                        professor_id = re.compile("professorID.*?\"(\d+)\"",re.S|re.I).findall(page)
                        
                        #Nbr Instances Per Tag
                        nbr_tag_instances = re.compile("choosetags\">.*?\((\d+)",re.S|re.I).findall(page)
                        
                        nbr_tags = len(professor_tags)
                        
                        i=0
                        while i <= nbr_tags-1:
                                professor_tag = professor_tags[i].replace("\\","").replace("'","''").strip()
                                nbr_instances = nbr_tag_instances[i]
                                insert_professor_tags(professor_id[0],professor_tag,nbr_instances)
                                i+=1
                    
                    #INSERT INTO REVIEWS TABLE
                    
                    base_paginate_url = "http://www.ratemyprofessors.com/paginate/professors/ratings?tid=$TID$&filter=&courseCode=&page=$PAGE_ID$"
                    
                    #Professor ID
                    professor_id = re.compile("professorID.*?\"(\d+)\"",re.S|re.I).findall(starting_page_content)
                    professor_id = professor_id[0].replace("\\","").replace("'","''").strip()
                    #print(professor_id)
                    
                    for p in range(nbr_pages):
                        p+=1
                        paginate_url = base_paginate_url.replace("$TID$",str(professor_id)).replace("$PAGE_ID$",str(p)) 
                        response = requests.get(paginate_url)    
                        dict = response.json()
                        reviews = dict['ratings']
                    
                        attributes = ['attendance','helpCount', 'id','notHelpCount','rClass','rComments',
                                      'rDate','rTextBookUse','rWouldTakeAgain','takenForCredit','onlineClass','quality','rEasy',
                                      'rOverall','teacherGrade','teacherRatingTags']
                                      
                        # Create List of Lists to store attributes
                        nbr_attributes = len(attributes)
                        attribute_lists = []
                        for i in range(nbr_attributes):
                            attribute_lists.append([])
                            
                        
                        for i in range(nbr_attributes):
                                for review_list in reviews:
                                    curr_attribute = attributes[i]
                                    attribute_lists[i].append(review_list[curr_attribute])
                                    
                        
                        attendance_values = attribute_lists[0]
                        #print(attendance)
                        comment_useful_nbr_values = attribute_lists[1]
                        #print(comment_useful_nbr)
                        review_id_values = attribute_lists[2]
                        #print(review_ids)
                        comment_not_useful_nbr_values = attribute_lists[3]
                        #print(comment_not_useful_nbr)
                        class_name_values = attribute_lists[4]
                        #print(class_name)
                        review_text_values = attribute_lists[5]
                        #print(review_texts)
                        review_date_values = attribute_lists[6]
                        #print(review_dates)
                        textbook_used_values = attribute_lists[7]
                        #print(textbook_used)
                        would_take_again_values = attribute_lists[8]
                        #print(would_take_again)
                        for_credit_values = attribute_lists[9]
                        #print(for_credit)
                        online_class_values = attribute_lists[10]
                        #print(online_class)
                        quality_description_values = attribute_lists[11]
                        #print(quality_description)
                        difficulty_score_values = attribute_lists[12]
                        #print(difficulty_score)
                        overall_quality_score_values = attribute_lists[13]
                        #print(overall_quality_score)
                        grade_received_values = attribute_lists[14]
                        #print(grade_received)
                        teacher_rating_tags = attribute_lists[15]
                        #print(teacher_rating_tags)
                        
                        #Dynamically get the number of elements in each page
                        nbr_reviews_in_page = len(review_id_values)
                        
                        for i in range(nbr_reviews_in_page):
                            attendance = attendance_values[i]
                            comment_useful_nbr = str(comment_useful_nbr_values[i])
                            review_id = str(review_id_values[i])
                            comment_not_useful_nbr = str(comment_not_useful_nbr_values[i])
                            class_name = class_name_values[i]
                            review_text = review_text_values[i].replace("\\r\\n","").replace("\\","").replace("'","''").strip()
                            review_date = review_date_values[i]
                            textbook_used = textbook_used_values[i]
                            would_take_again = would_take_again_values[i]
                            for_credit = for_credit_values[i]
                            online_class = online_class_values[i]
                            if online_class == 'online':
                                online_class = '1'
                            else:
                                online_class = '0'
                            quality_description = quality_description_values[i]
                            difficulty_score = str(difficulty_score_values[i])
                            overall_quality_score = str(overall_quality_score_values[i])
                            grade_received = grade_received_values[i]
                            professor_id = professor_id
                    
                            #print(attendance)
                            #print(comment_useful_nbr)
                            #print(review_id)
                            #print(comment_useful_nbr)
                            #print(class_name)
                            #print(review_text)
                            #print(review_date)
                            #print(textbook_used)
                            #print(would_take_again)
                            #print(for_credit)
                            #print(quality_description)
                            #print(difficulty_score)
                            #print(overall_quality_score)
                            #print(grade_received)
                            #print(professor_id)
                    
                            #Insert into reviews table
                            insert_reviews(review_id,professor_id,review_text,review_date,class_name,overall_quality_score,for_credit,attendance,textbook_used,would_take_again,grade_received,comment_useful_nbr,comment_not_useful_nbr,online_class)
                    
                        # INSERT INTO REVIEW_TAGS_TABLE
                        i=0
                        while i<=nbr_reviews_in_page-1:
                            for tag_list in teacher_rating_tags:
                                if len(tag_list) ==0:
                                    i+=1
                                    continue
                                for tag in tag_list:
                                        tag = tag.replace("\\r\\n","").replace("\\","").replace("'","''").strip()
                                        review_id = str(review_id_values[i]).replace("\\r\\n","").replace("\\","").replace("'","''").strip()
                                        insert_review_tags(review_id,tag)   
                                i+=1
                                
                    #completion time after review insert 
                    completion_time = str(datetime.now())
                                    
                    #Update completion date times in task table
                    update_task_table_with_completion_date_time(completion_time,starting_url)
                        
                    #re-calculate flag to make sure there are still tasks to be completed
                    flag = calculate_nbr_pending_tasks()>0 
                    
                    wait_time = 1      
                                         
                #Catch errors
        except Exception as e:
            #Internet Connection Error 
            if e.__class__== requests.exceptions.ConnectionError:
                with open("logProfessor.txt", "a") as my_file:
                    my_file.write("CONNECTION Faliure for Task ID:" + task_id + str(e) + '\n'*2)
                update_crawler_name_to_null_for_failed_task(starting_url)
                time.sleep(wait_time)
                wait_time*=2
            # Database Connection/Operation Error 
            elif e.__class__ == pyodbc.OperationalError:
                with open("LogProfessor.txt", "a") as my_file:
                    my_file.write("PYODBC Operational Faliure for Task ID:" + task_id + str(e) + '\n'*2)
                update_crawler_name_to_null_for_failed_task(starting_url)
                time.sleep(wait_time)
                wait_time*=2
            # Other Error
            else:
                with open("LogProfessor.txt", "a") as my_file:
                    my_file.write("OTHER Faliure for Task ID:" + task_id + str(e) + '\n'*2)
                    nbr_retries +=1
                    if nbr_retries == max_retries:
                        completion_time = "MAX_RETRY_FALIURE"
                        update_task_table_with_completion_date_time(completion_time,starting_url)
                        nbr_retries = 0
                        wait_time = 1
                    else:
                        time.sleep(wait_time)
                        wait_time+=1
except Exception as e:
    print(e)
    input("hello world")
    
                  
            
#input('I think this is complete')
