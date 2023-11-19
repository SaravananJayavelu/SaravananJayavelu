
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import datetime
import streamlit as st
from googleapiclient.errors import HttpError


api_key = 'AIzaSyCuxfRWfkDvukulzs-Z3asJaKrs0EWBV4Y'
youtube = build('youtube','v3',developerKey=api_key)

# Get Channel Details 
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id  # Use the passed channel_id
    )
    response = request.execute()

    channel_data_list = []  # List to store data for each channel

    for i in response.get('items', []):
        data = dict(
            channel_name=i['snippet']['title'],
            channel_des=i['snippet']['description'],
            channel_publishAt=i['snippet']['publishedAt'],
            channel_playlist=i['contentDetails']['relatedPlaylists']['uploads'],
            channel_viewcount=i['statistics']['viewCount'],
            channel_sub=i['statistics']['subscriberCount'],
            channel_video=i['statistics']['videoCount']
        )
        channel_data_list.append(data)
    return channel_data_list
    


#get video_ids

def get_videos_ids(channel_id):
    
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                    part= 'contentDetails').execute()
    Playist_ID = response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None

    while True:
        response1 = youtube.playlistItems().list(part = 'snippet',
                                                 playlistId = Playist_ID,
                                                 maxResults = 50,
                                                 pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    

# get video_info 
def get_video_info(Video_Ids):
    video_data =[]
    for video_id in Video_Ids :
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id = video_id
        )
        response= request.execute()

        for item in response['items']:
            data= dict(Channel_title = item['snippet']['channelTitle'],
                       Channel_Id = item['snippet']['channelId'],
                       video_title = item['snippet']['title'],
                       video_id =item['id'],
                       publish_date = item['snippet']['publishedAt'],
                       video_des = item['snippet']['description'],
                       duration = item['contentDetails']['duration'],
                       view_count = item[ 'statistics']['viewCount'],
                       Fav_count = item['statistics'].get('favoriteCount',0),
                       like_count = item[ 'statistics'].get('likeCount',0),
                       commentCount = item[ 'statistics'].get('commentCount',0))
        video_data.append(data)
    return video_data


# Get Comments information
def get_comment_info(Video_Ids):
    comment_data = []
    try:
        for video_id in Video_Ids:    
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults = 50
            )

            response = request.execute()

            for item in response['items']:
                data = dict(comment_ID = item['snippet']['topLevelComment']['id'],
                        video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                        commment_text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_publishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                comment_data.append(data)
    except:
        pass
    return comment_data


def get_playlist_Details(channel_id):
    try:
        response = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50
        ).execute()

        playlists = []
        for item in response.get('items', []):
            playlists.append({
                'Playist_ID': item['id'],
                'playlist_title': item['snippet']['title'],
                'channelId': item['snippet']['channelId'],
                'channel_title': item['snippet']['channelTitle'],
                'published_date': item['snippet']['publishedAt'],
                'video_cont': item['contentDetails']['itemCount']
            })

        return playlists

    except HttpError as e:
        if e.resp.status == 404:
            print(f"Channel not found for channel ID: {channel_id}")
        else:
            print(f"Error retrieving playlists: {e}")
        return []




# Upload to MongoDb
client = pymongo.MongoClient("mongodb+srv://saravanan31198:saravanan@cluster0.81qggai.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]


def channel_details(channel_id): 
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_Details (channel_id)
    vi_ids =  get_videos_ids(channel_id)    
    vi_details = get_video_info(vi_ids)
    comnt_details = get_comment_info(vi_ids)

    col1 = db["channel_details"]
    col1.insert_one({
                    "channel_information":ch_details,
                    "playlist_information":pl_details,
                    "video_informaion":vi_details, 
                    "comment_information":comnt_details})
    return "upload completed :)"



def channels_table():        
        mydb = psycopg2.connect(host= "localhost",
                                user = "postgres",
                                password = "saravanan",
                                database = "Youtube_data",
                                port = "5432")
        cursor = mydb.cursor()

        drop_query = '''drop table if exists channel'''
        cursor.execute(drop_query)
        mydb.commit()
        try:
                create_query= '''create table if not exists channel( channel_name varchar(100),
                                                                channel_des text,
                                                                channel_publishAt timestamp,
                                                                channel_playlist varchar(80),
                                                                channel_viewcount bigint,
                                                                channel_sub bigint,
                                                                channel_video int)'''
                cursor.execute(create_query)
                mydb.commit()

        except:
                print("Channel Table already created")

        ch_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for ch_data in col1.find({},{"_id":0}):
                ch_list.append(ch_data["channel_information"])
        ch_list_flat = [item for sublist in ch_list for item in sublist]
        df = pd.DataFrame(ch_list_flat)

        for index,row in df.iterrows(): 
                        insert_query = '''insert into channel(channel_name,         
                                                        channel_des,
                                                        channel_publishAt,
                                                        channel_playlist,
                                                        channel_viewcount,
                                                        channel_sub,
                                                        channel_video)
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                        
                        values=(row['channel_name'],   
                                row['channel_des'],
                                row['channel_publishAt'],
                                row['channel_playlist'],
                                row['channel_viewcount'],
                                row['channel_sub'],
                                row['channel_video'])
                        
                        cursor.execute(insert_query,values)
                        mydb.commit()


def playlist_table():
        mydb = psycopg2.connect(
                host= "localhost",
                user = "postgres",
                password = "saravanan",
                database = "Youtube_data",
                port = "5432")

        cursor = mydb.cursor()

        drop_query = '''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()



        create_query= '''create table if not exists playlists(Playist_ID VARCHAR(255) primary Key,
                                                        playlist_title VARCHAR(255),
                                                        channelId VARCHAR(255),
                                                        channel_title VARCHAR(255),
                                                        published_date timestamp,
                                                        video_cont int)'''
        cursor.execute(create_query)
        mydb.commit()

        pl_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for pl_data in col1.find({},{"_id":0,"playlist_information":1}):
                for i in range (len(pl_data['playlist_information'])):
                        pl_list.append(pl_data['playlist_information'][i])
        df1 = pd.DataFrame(pl_list)

        for index,row in df1.iterrows(): 
                insert_query = '''insert into playlists(Playist_ID,         
                                                playlist_title,
                                                channelId,
                                                channel_title,
                                                published_date,
                                                video_cont)
                                                values(%s,%s,%s,%s,%s,%s)'''
                
                values=(row['Playist_ID'],   
                        row['playlist_title'],
                        row['channelId'],
                        row['channel_title'],
                        row['published_date'],
                        row['video_cont']
                        )
                
                cursor.execute(insert_query,values)
                mydb.commit()


def videos_table():
        mydb = psycopg2.connect(host= "localhost",
                                user = "postgres",
                                password = "saravanan",
                                database = "Youtube_data",
                                port = "5432")

        cursor = mydb.cursor()

        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()



        create_query= '''create table if not exists videos(Channel_title VARCHAR(255),
                                                        Channel_Id VARCHAR(255),
                                                        video_title VARCHAR(255),
                                                        video_id VARCHAR(255),
                                                        publish_date timestamp,
                                                        video_des VARCHAR(500),
                                                        duration INTERVAL,
                                                        view_count int,
                                                        Fav_count int,
                                                        like_count int,
                                                        commentCount int)'''

        cursor.execute(create_query)
        mydb.commit()

        vi_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for vi_data in col1.find({},{"_id":0,"video_informaion":1}):
                for i in range (len(vi_data['video_informaion'])):
                        vi_list.append(vi_data['video_informaion'][i])
        df2 = pd.DataFrame(vi_list)

        try:
                for index, row in df2.iterrows(): 
                        insert_query = '''insert into videos(Channel_title,
                                                        Channel_Id,
                                                        video_title,
                                                        video_id,
                                                        publish_date,
                                                        video_des,
                                                        duration,
                                                        view_count,
                                                        Fav_count,
                                                        like_count,
                                                        commentCount)
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                        # Truncate 'video_des' column data to fit within 255 characters
                        truncated_video_des = row['video_des'][:255]

                        values = (row['Channel_title'],   
                                row['Channel_Id'],
                                row['video_title'],
                                row['video_id'],
                                row['publish_date'],
                                truncated_video_des,  # Insert truncated 'video_des' here
                                row['duration'],   
                                row['view_count'],
                                row['Fav_count'],
                                row['like_count'],
                                row['commentCount']
                                )
                        
                        cursor.execute(insert_query, values)
                        mydb.commit()
                
                print("Data inserted successfully!")

        except Exception as e:
                print("Error:", e)
        mydb.rollback()  # Rollback the transaction in case of an error



def comments_table():
        mydb = psycopg2.connect(host= "localhost",
                                user = "postgres",
                                password = "saravanan",
                                database = "Youtube_data",
                                port = "5432")

        cursor = mydb.cursor()

        drop_query = '''drop table if exists comments '''
        cursor.execute(drop_query)
        mydb.commit()


        create_query= '''create table if not exists comments(comment_ID VARCHAR(100),
                                                        video_id VARCHAR(100),
                                                        commment_text VARCHAR(255),
                                                        comment_author VARCHAR(250),
                                                        comment_publishedAt timestamp)'''


        cursor.execute(create_query)
        mydb.commit()

        Comment_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for comment_data in col1.find({},{"_id":0,"comment_information":1}):
                for i in range (len(comment_data['comment_information'])):
                        Comment_list.append(comment_data['comment_information'][i])
        df3 = pd.DataFrame(Comment_list)


        for index, row in df3.iterrows(): 
                insert_query = '''insert into comments(comment_ID,
                                        video_id,
                                        commment_text,
                                        comment_author,
                                        comment_publishedAt)
                                        values(%s,%s,%s,%s,%s)'''
                truncated_commment_text = row['commment_text'][:255]
                values = (row['comment_ID'],
                        row['video_id'],
                        truncated_commment_text,
                        row['comment_author'],
                        row['comment_publishedAt'],
                        )
                
                cursor.execute(insert_query, values)
                mydb.commit()


def Tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

def show_channel_table():
        ch_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for ch_data in col1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
        df = st.dataframe(ch_list)        
        return df

def show_playlist_table():   
    pl_list = []
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for pl_data in col1.find({},{"_id":0,"playlist_information":1}):
            for i in range (len(pl_data['playlist_information'])):
                    pl_list.append(pl_data['playlist_information'][i])
    df1 = st.dataframe(pl_list)

    return df1

def show_video_table():
    vi_list = []
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for vi_data in col1.find({},{"_id":0,"video_informaion":1}):
            for i in range (len(vi_data['video_informaion'])):
                    vi_list.append(vi_data['video_informaion'][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():    
        Comment_list = []
        db = client["Youtube_data"]
        col1 = db["channel_details"]
        for comment_data in col1.find({},{"_id":0,"comment_information":1}):
                for i in range (len(comment_data['comment_information'])):
                        Comment_list.append(comment_data['comment_information'][i])
        df3 = st.dataframe(Comment_list)
        return df3

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSNG]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id =st.text_input("Enter Channel ID")

if st.button("collect and Store Data"):
    insert = channel_details(channel_id)
    st.success(insert)

if st.button("Migrate to SQL"):
    Table = Tables()
    st.success(Table)

show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table =="CHANNELS":
    show_channel_table()

elif show_table =="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comments_table()
        

# SQL Connection

mydb = psycopg2.connect(host= "localhost",
                        user = "postgres",
                        password = "saravanan",
                        database = "Youtube_data",
                        port = "5432")
cursor = mydb.cursor()

questions = st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                                 "2.Which channels have the most number of videos, and how many videos do they have?",
                                                 "3.What are the top 10 most viewed videos and their respective channels?",
                                                 "4.How many comments were made on each video, and what are their corresponding video names?",
                                                 "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                 "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                 "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                 "8.What are the names of all the channels that have published videos in the year 2022?",
                                                 "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                 "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                 ))
if questions == "1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select video_title as videos,Channel_title as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif questions == "2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as channelname,channel_video as no_videos from channel
                order by channel_video desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["channel name","channel video"])
    st.write(df2)


elif questions == "3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select channel_title as videos,
                video_title as videos,
                view_count as videos from videos order by view_count desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["channel name","video title","view count"])
    st.write(df3)


elif questions == "4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select commentCount as no_of_comment,
                video_title as videos,
                channel_title as videos from videos where commentCount is not null order by commentCount desc'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4,columns=["no_of_comment","video title","channel name"])
    st.write(df4)


elif questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select like_count as likes,
                video_title as videoName,
                channel_title as channelName from videos where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=["total likes","video title","channel name"])
    st.write(df5)


elif questions == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select like_count as likes,
                video_title as videoName from videos where like_count is not null order by like_count desc'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6,columns=["total likes","video title"])
    st.write(df6)


elif questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_name as channelName,
                channel_viewcount as totalViews from channel order by channel_viewcount desc'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7,columns=["channelName","totalViews"])
    st.write(df7)


elif questions == "8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select channel_title as channelName,
                video_title as videoName,
                publish_date as publishedAT from videos where extract (year from publish_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=["channelName","VideoTitle","published date"])
    st.write(df8)

elif questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_title as channelName,
                AVG(duration) as averageduration from videos group by channel_title'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9,columns=["channelName","averageduration"])
    
    T9 = []
    for index,row in df9.iterrows():
        channel_title = row["channelName"]
        averag_duration = row["averageduration"]
        average_duartion_str = str(averag_duration)
        T9.append(dict(channaltitle =channel_title,averag_duration = average_duartion_str ))
    df1 = pd.DataFrame(T9)
    st.write(df1)


elif questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10 = '''select channel_title as channelName,
                video_title as videoName,
                commentCount as comments from videos order by commentCount desc'''
    cursor.execute(query10)
    mydb.commit()
    t10= cursor.fetchall()
    df10 = pd.DataFrame(t10,columns=["channelName","videoName","totalComments"])
    st.write(df10)          