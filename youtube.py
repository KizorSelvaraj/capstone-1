import pandas as pd
import numpy as np
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import psycopg2
import seaborn as sns
import streamlit as st




def Api_connect():
    api_key="AIzaSyCXU2Z5o7ggUAZQVZXkVfbDUe6gxqtDfyw"
    api_service_name="youtube"
    api_version="v3"
    youtube= build(api_service_name,api_version,developerKey=api_key)
    return youtube
youtube = Api_connect()

def Channel_details(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                     Channel_id = i["id"],
                     Subscribers = i["statistics"]["subscriberCount"],
                     Views = i["statistics"]["viewCount"],
                     Total = i ["statistics"]["videoCount"],
                     Channel_Description = i ["snippet"]["description"],
                      Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
        return(data)

def video_ids(channel_id):
    video_ids =[]
    response =youtube.channels().list(id =channel_id,
                                      part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        result =youtube.playlistItems().list(part="snippet",
                                         playlistId = Playlist_Id,
                                         maxResults = 50,
                                         pageToken = next_page_token).execute()
                                            
        for i in range (len(result['items'])):
            video_ids.append(result['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = result.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

def video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id= video_id
        )
        response = request.execute()
        for item in response['items']:
            data=dict(Channel_Name = item['snippet']['channelTitle'],
                      Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item ['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favourite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


def comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId = video_id,
                maxResults= 50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_id= item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data


def Playlist_info(channel_id):
    next_page_token = None
    playlist_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,ContentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()
        for item in response['items']:
            data = dict(Playlist_id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        Published_At = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount']
                        )
            playlist_data.append(data)
        next_page_token =response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data



client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube"]


def channels_details(channel_id):
    playlist = Playlist_info(channel_id)
    video_iden = video_ids(channel_id)
    Ch_D = Channel_details(channel_id)
    comment = comment_info(video_iden)
    video_information = video_info(video_iden)
    call1 = db["Channel_details"]
    call1.insert_one({"channel_information":Ch_D,"playlist_information":playlist,"Comment_information":comment,
                        "video_information":video_information})
    return "upload completed successfully"


def channels_function():
        database = psycopg2.connect(host= "localhost",
                                        user = "postgres",
                                        password = "Kizor@1996",
                                        database="youtube_data",
                                        port = 5432)
        cursor = database.cursor()
        drop_query = '''drop table if exists channels'''
        cursor.execute(drop_query)
        database.commit()

        try:
                
                create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_id varchar(100) primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total bigint,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))'''
                cursor.execute(create_query)
                database.commit()
        except:
                print("No Need")
        ch_list=[]
        db = client["youtube"]
        coll1 = db["Channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                
                ch_list.append(ch_data["channel_information"])
        df = pd.DataFrame(ch_list)

        for index,row in df.iterrows():
                insert_query = '''insert into channels(Channel_Name,
                                                        Channel_id,
                                                        Subscribers,
                                                        Views,
                                                        Total,
                                                        Channel_Description,
                                                        Playlist_Id)
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['Channel_Name'],
                        row['Channel_id'],
                        row['Subscribers'],
                        row['Views'],
                        row['Total'],
                        row['Channel_Description'],
                        row['Playlist_Id'])
                try:

                        cursor.execute(insert_query,values)
                        database.commit()
                except:
                        print("No_Need")


def playlist_function():

        database = psycopg2.connect(host= "localhost",
                                        user = "postgres",
                                        password = "Kizor@1996",
                                        database="youtube_data",
                                        port = 5432)
        cursor = database.cursor()
        drop_query = '''drop table if exists Playlist'''
        cursor.execute(drop_query)
        database.commit()

        create_query = '''create table if not exists playlist(Playlist_Id varchar(100) primary key,
                                                                Title varchar(100),
                                                                Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                published_At timestamp,
                                                                Video_Count int)'''
        cursor.execute(create_query)
        database.commit()

        pl_list=[]
        db = client["youtube"]
        coll1 = db["Channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        dataframeA = pd.DataFrame(pl_list)

        for index,row in dataframeA.iterrows():
                        insert_query = '''insert into playlist(Playlist_Id,
                                                                Title,
                                                                Channel_Id,
                                                                Channel_Name,
                                                                Published_At,
                                                                Video_Count
                                                                )

                                                                values(%s,%s,%s,%s,%s,%s)'''
                        values = (row['Playlist_id'],
                                row['Title'],
                                row['Channel_Id'],
                                row['Channel_Name'],
                                row['Published_At'],
                                row['Video_Count']
                                )
                        cursor.execute(insert_query,values)
                        database.commit()
                        
                        


def video_function():
        
    database = psycopg2.connect(host= "localhost",
                                    user = "postgres",
                                    password = "Kizor@1996",
                                    database="youtube_data",
                                    port = 5432)
    cursor = database.cursor()
    cursor = database.cursor()
    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    database.commit()

    try:                
        create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                                Channel_Id varchar(100),
                                                                Video_Id varchar(100),
                                                                Title varchar(100),
                                                                Tags text,
                                                                Thumbnail varchar(300), 
                                                                Description text,
                                                                Published_Date timestamp,
                                                                Duration interval,
                                                                Views bigint,
                                                                Likes int,
                                                                Comments int,
                                                                Favourite_Count bigint,
                                                                Definition varchar(50),
                                                                Caption_Status varchar(100)
                                                                )'''
        cursor.execute(create_query)
        database.commit()
    except:
        print("No_Need")

    vi_list=[]
    db = client["youtube"]
    coll1 = db["Channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    dataframe1 = pd.DataFrame(vi_list)

    for index,row in dataframe1.iterrows():
                    insert_query = '''insert into videos(Channel_Name,
                                                            Channel_Id,
                                                            Video_Id,
                                                            Title,
                                                            Tags,
                                                            Thumbnail,
                                                            Description,
                                                            Published_Date,
                                                            Duration,
                                                            Views,
                                                            Likes,
                                                            Comments,
                                                            Favourite_Count,
                                                            Definition,
                                                            Caption_Status)                                           
                                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values = (row['Channel_Name'],
                            row['Channel_Id'],
                            row['Video_Id'],
                            row['Title'],
                            row['Tags'],
                            row['Thumbnail'],
                            row['Description'],
                            row['Published_Date'],
                            row['Duration'],
                            row['Views'],
                            row['Likes'],
                            row['Comments'],
                            row['Favourite_Count'],
                            row['Definition'],
                            row['Caption_Status']
                            )
                    try:
                            
                        
                        cursor.execute(insert_query,values)
                        database.commit()
                    except:
                            print("No_Need")
                    


def comments_function():    
    database = psycopg2.connect(host= "localhost",
                                    user = "postgres",
                                    password = "Kizor@1996",
                                    database="youtube_data",
                                    port = 5432)
    cursor = database.cursor()
    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    database.commit()

    try:
        create_query = '''create table if not exists comments(Comment_Id varchar(100),
                                                                        Video_id varchar(75),
                                                                        Comment_Text  text,
                                                                        Comment_Author varchar(200),
                                                                        Comment_Published timestamp)'''
        cursor.execute(create_query)
        database.commit()
    except:
        print('No_Need')

    co_list=[]
    db = client["youtube"]
    coll1 = db["Channel_details"]
    for co_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(co_data["Comment_information"])):
                co_list.append(co_data["Comment_information"][i])
    dataframe2 = pd.DataFrame(co_list)

    for index,row in dataframe2.iterrows():
                insert_query = '''insert into comments(Comment_Id,
                                Video_id,
                                Comment_Text,
                                Comment_Author,
                                Comment_Published
                                )                                           
                                values(%s,%s,%s,%s,%s)'''
                
                values = (row['Comment_Id'],
                        row['Video_id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )
                try:
                    cursor.execute(insert_query,values)
                    database.commit()
                except:
                    print("value uploaded")

def SQL_table():
    channels_function()
    playlist_function()
    video_function()
    comments_function()

    return "Table Created"



def channels_tables():
        
    ch_list=[]
    db = client["youtube"]
    coll1 = db["Channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            
            ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def playlist_tables():
        pl_list=[]
        db = client["youtube"]
        coll1 = db["Channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        dataframeA = st.dataframe(pl_list)
        
        return dataframeA


def video_tables():
    vi_list=[]
    db = client["youtube"]
    coll1 = db["Channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    dataframe1 = st.dataframe(vi_list)

    return dataframe1


def comments_tables():

        co_list=[]
        db = client["youtube"]
        coll1 = db["Channel_details"]
        for co_data in coll1.find({},{"_id":0,"Comment_information":1}):
                for i in range(len(co_data["Comment_information"])):
                        co_list.append(co_data["Comment_information"][i])
        dataframe2 = st.dataframe(co_list)

        return dataframe2

st.header("youtube project")

with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING(MY FIRST PROJECT)]")
    st.header("Skills Take Away form the project")
    st.caption("Python Scripting")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MonogoDB and SQL")



Channel_id = st.text_input("Enter The Channel Id")

if st.button("Collect and Store Data"):
    ch_ids=[]
    db = client["youtube"]
    coll1 = db["Channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])
    if Channel_id in ch_ids:
        st.success("Please insert other channel_Ids")
    else:
        insert = channels_details(Channel_id)
        st.success(insert)

    
if st.button("Transfer to SQL"):
    Table = SQL_table()
    st.success(Table)

show_table  = st.radio("Select For View",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
    channels_tables()

if show_table == "PLAYLISTS":
    playlist_tables()

if show_table == "VIDEOS":
    video_tables()

if show_table == "COMMENTS":
    comments_tables()

 

database = psycopg2.connect(host= "localhost",
                                    user = "postgres",
                                    password = "Kizor@1996",
                                    database="youtube_data",
                                    port = 5432)
cursor = database.cursor()
question = st.selectbox("Select your question",("1. What are the videos' names and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?", 
                                                 "3. What are the top 10 most viewed videos and their respective channels? ",
                                                  "4. How many comments were made on each video, and what are their corresponding video names?",
                                                   "5. Which videos have the highest number of likes and what are their corresponding channel names?",                                                   
                                                   "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                   "8. What are the names of all the channels that have published videos in the year 2022?",
                                                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                   "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                   ))
if question == "1. What are the videos' names and their corresponding channels?":
    query1 = '''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    database.commit()
    q1 = cursor.fetchall()
    d1= pd.DataFrame(q1,columns= ["video title","channel name"])
    st.write(d1)


elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as channelname,total as no_videos from channels
                order by total desc'''
    cursor.execute(query2)
    database.commit()
    q2 = cursor.fetchall()
    d2= pd.DataFrame(q2,columns= ["channel","channel name"])
    st.write(d2)

elif question == "3. What are the top 10 most viewed videos and their respective channels? ":
    query3 = '''select views as views, channel_name as channelname,title as videotitle from videos where videos is not null order by views desc limit 10'''
    cursor.execute(query3)
    database.commit()
    q3 = cursor.fetchall()
    d3= pd.DataFrame(q3,columns= ["views","channel name","videotitle"])
    st.write(d3)

elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    database.commit()
    q4 = cursor.fetchall()
    d4= pd.DataFrame(q4,columns= ["no_comments","videotitle"])
    st.write(d4)

elif question == "5. Which videos have the highest number of likes and what are their corresponding channel names?":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likescount from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    database.commit()
    q5 = cursor.fetchall()
    d5= pd.DataFrame(q5,columns= ["videotitle","channelname","likescount"])
    st.write(d5)

elif question == "6. What is the total number of likes for each video, and what are their corresponding video names?":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    database.commit()
    q6 = cursor.fetchall()
    d6= pd.DataFrame(q6,columns= ["likecount","videotitle"])
    st.write(d6)

elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":


    query7 = '''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    database.commit()
    q7 = cursor.fetchall()
    d7= pd.DataFrame(q7,columns= ["channel_name","totalviews"])
    st.write(d7)

elif question == "8. What are the names of all the channels that have published videos in the year 2022?":


    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    database.commit()
    q8 = cursor.fetchall()
    d8 = pd.DataFrame(q8,columns= ["videotitle","published_date","channelname"])
    st.write(d8)

elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":


    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    database.commit()
    q9 = cursor.fetchall()
    d9 = pd.DataFrame(q9,columns= ["channelname","averageduration"])
    Q9=[]
    for index,row in d9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        Q9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
    Qu9 = pd.DataFrame(Q9)
    st.write(Qu9)

elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":


    query10 = '''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    database.commit()
    q10 = cursor.fetchall()
    d10 = pd.DataFrame(q10,columns= ["video title","channelname","comments"])
    st.write(d10)

