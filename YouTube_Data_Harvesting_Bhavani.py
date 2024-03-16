#Importing all the required packages

import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Function to connect to API key

def api_connect():

    api_key = 'AIzaSyDZMG755hO_jkMYXwq5H2YaJJj87gAmlzo'
    api_version = "v3"
    api_service_name = "youtube"
    youtube = googleapiclient.discovery.build(api_service_name,
                                              api_version,
                                              developerKey = api_key)

    return youtube

youtube = api_connect()


#Function to get the details of a channel

def get_channel_details(channel_id):
    
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
                                      id = channel_id)
    response = request.execute()
    for item in response['items']:
                  
        c_details = dict(Channel_Id = item['id'],
                         Channel_Name = item['snippet']['title'],
                         Channel_Description = item['snippet']['description'],
                         Channel_Views = item['statistics']['viewCount'],
                         Subscription_Count = item['statistics']['subscriberCount'],
                         Playlist_Id = item['contentDetails']['relatedPlaylists']['uploads'],
                         Video_Count = item['statistics']['videoCount']
                        )
        return c_details
    

#Function to get the ids of all the videos in a channel

def get_video_ids(channel_id):
    video_ids_list = []
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
                                      id = channel_id)
    response = request.execute()
    upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list(part="snippet,contentDetails",
                                                playlistId = upload_id,
                                                pageToken = next_page_token, maxResults=50)
        response1 = request1.execute()
        
        for item in range(len(response1['items'])):
            video_ids_list.append(response1['items'][item]['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids_list



#Function to get the video details of each video

def get_video_details(video_ids_list):
    list_video_details = []
    for video_id in video_ids_list:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",
                                        id = video_id)
        response = request.execute()
        for item in response['items']:
            
            v_details = dict(Video_Id = item['id'],
                            Video_Name = item['snippet']['title'],
                            Video_Description = item['snippet'].get('description'),
                            Channel_Name = item['snippet']['channelTitle'],
                            PublishedAt = item['snippet']['publishedAt'],
                            View_Count = item['statistics'].get('viewCount'),
                            Like_Count = item['statistics'].get('likeCount'),
                            Dislike_Count = item['statistics'].get('dislikeCount'),
                            Favorite_Count = item['statistics']['favoriteCount'],
                            Comment_Count = item['statistics'].get('commentCount'),
                            Duration = item['contentDetails']['duration'],
                            Thumbnail = item['snippet']['thumbnails']['default']['url'],
                            Caption_Status = item['contentDetails']['caption'],
                            Tags = item['snippet'].get('tags')
                            )
                        
            list_video_details.append(v_details)
            
    return list_video_details


    
                                           
#Function to get the details of playlists

def get_playlists_details(channel_id):
    list_playlists = []
    next_page_token = None

    while True:
   
        request = youtube.playlists().list(part="snippet,contentDetails",
                                           channelId = channel_id, maxResults=50, 
                                           pageToken = next_page_token)
        response = request.execute()
        
        for item in response['items']:
            p_details = dict(Playlist_Id = item['id'],
                             Playlist_Name = item['snippet']['title'],
                             Channel_Id = item['snippet']['channelId'],
                             Channel_Name = item['snippet']['channelTitle'],
                             Video_Count = item['contentDetails']['itemCount']
                            )
            
            list_playlists.append(p_details)

        next_page_token = response.get('next_page_token')

        if next_page_token is None:
            break
            
    return list_playlists


#Function to get the comments of all the videos:

def get_comment_details(videos_ids_info):
    list_comment_details = []
    try:

        for video_id in videos_ids_info:
            request = youtube.commentThreads().list(part="snippet",
                                                    videoId = video_id, 
                                                    maxResults = 50)
            response = request.execute()
            
            for item in response['items']:
                
                c_details = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                                Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_Text = item['snippet']['topLevelComment']['snippet']['textOriginal'],
                                Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_Publisheddate = item['snippet']['topLevelComment']['snippet']['publishedAt']
                                )
                            
                list_comment_details.append(c_details)
    
    except:
        pass
    return list_comment_details


#Connection to the MongoDB

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Youtube_Data_Harvest"]
coll1 = db['Youtube_Channel_Data']
#coll1.create_index('Channel_Id', unique=True)


#Inserting Data to MongoDB

def Youtube_Data(channel_id):
    channel_info = get_channel_details(channel_id)
    videos_ids_info = get_video_ids(channel_id)
    videos_info = get_video_details(videos_ids_info)
    playlists_info = get_playlists_details(channel_id)
    comments_info = get_comment_details(videos_ids_info)
    
    coll1.insert_one({"channel_information" : channel_info,
                      "video_information" : videos_info,
                      "comments_information" : comments_info,
                      "playlists_information" : playlists_info
                     })
    return "Stored {} in MongoDB successfully".format(channel_id)


#Creating tables for Channels

def channel_table():
    #Connecting to a database in PostgreSQL
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()

    create_table_channel = '''CREATE TABLE IF NOT EXISTS Channels(Channel_Id varchar(50) primary key,
                                                                Channel_Name varchar(100),
                                                                Channel_Description text,
                                                                Channel_Views bigint,
                                                                Subscription_Count bigint,
                                                                Playlist_Id varchar(50),
                                                                Video_Count int
                                                                )'''
    mycursor.execute(create_table_channel)
    mydb.commit()

def insert_channelinfo(channels_name):
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()

    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]
    channel_list=[]
    for i in channels_names:
        for data in coll1.find({'channel_information.Channel_Name': i}, {'_id':0}):
            channel_list.append(data['channel_information'])

    df1 = pd.DataFrame(channel_list)

    for index,row in df1.iterrows():
        
        insert_query = '''insert into channels (Channel_Id, 
                                                Channel_Name, 
                                                Channel_Description, 
                                                Channel_Views, 
                                                Subscription_Count, 
                                                Playlist_Id, 
                                                Video_Count) 
                                                
                                                VALUES ((%s),(%s),(%s),(%s),(%s),(%s),(%s))'''
        
        value = (row.Channel_Id, 
                 row.Channel_Name, 
                 row.Channel_Description, 
                 row.Channel_Views, 
                 row.Subscription_Count, 
                 row.Playlist_Id, 
                 row.Video_Count)
        
        try:

            mycursor.execute(insert_query, value)
            mydb.commit()

        except:
            st.write("UniqueViolation Error")



#Creating table for Videos
            
def videos_table():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()


    create_table_video = '''CREATE TABLE IF NOT EXISTS videosinfo(Video_Id varchar(30) primary key,
                                                            Video_Name varchar(200),
                                                            Video_Description text,
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            View_Count bigint,
                                                            Like_Count bigint,
                                                            Dislike_Count bigint,
                                                            Favorite_Count int,
                                                            Comment_Count int,
                                                            Duration interval,
                                                            Thumbnail varchar(255),
                                                            Caption_Status varchar(50),
                                                            Tags text)'''
    
    mycursor.execute(create_table_video)
    mydb.commit()

def insert_videosinfo(channels_name):
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()

    videos_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]

    for i in channels_names:
        for data in coll1.find({'channel_information.Channel_Name': i}, {'_id':0}):
            
            for j in range(len(data['video_information'])):
                videos_list.append(data['video_information'][j])

    df2 = pd.DataFrame(videos_list)

    for index,row in df2.iterrows():
        
        insert_query = '''insert into videosinfo (Video_Id, 
                                                  Video_Name, 
                                                  Video_Description, 
                                                  Channel_Name, 
                                                  PublishedAt, 
                                                  View_Count, 
                                                  Like_Count, 
                                                  Dislike_Count, 
                                                  Favorite_Count, 
                                                  Comment_Count, 
                                                  Duration, 
                                                  Thumbnail, 
                                                  Caption_Status, 
                                                  Tags) 
                                                  
                                                  VALUES ((%s),(%s),(%s),(%s),(%s),(%s),(%s),
                                                  (%s),(%s),(%s),(%s),(%s),(%s),(%s))'''
        
        values = (row['Video_Id'], 
                  row['Video_Name'], 
                  row['Video_Description'], 
                  row['Channel_Name'], 
                  row['PublishedAt'], 
                  row['View_Count'], 
                  row['Like_Count'], 
                  row['Dislike_Count'], 
                  row['Favorite_Count'], 
                  row['Comment_Count'], 
                  row['Duration'], 
                  row.Thumbnail, 
                  row['Caption_Status'], 
                  row['Tags'])

        try:

            mycursor.execute(insert_query,values)
            mydb.commit()

        except:
            pass


#Creating tables for Playlists
            
def playlists_table():

    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )
    mycursor = mydb.cursor()

    create_table_playlist = '''CREATE TABLE IF NOT EXISTS playlists(Playlist_Id varchar(255) primary key,
                                                                    Playlist_Name varchar(255),
                                                                    Channel_Id varchar(255),
                                                                    Channel_Name varchar(255),
                                                                    Video_Count int)'''
    mycursor.execute(create_table_playlist)
    mydb.commit()

def insert_Playlistsinfo(channels_name):
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()   

    playlist_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]
    for i in channels_names:
        for data in coll1.find({'channel_information.Channel_Name': i}, {'_id':0}):
               
            for i in range(len(data['playlists_information'])):
                playlist_list.append(data['playlists_information'][i])

    df3 = pd.DataFrame(playlist_list)

    for index,row in df3.iterrows():
        insert_query = '''insert into playlists (Playlist_Id, 
                                                 Playlist_Name, 
                                                 Channel_Id, 
                                                 Channel_Name, 
                                                 Video_Count) 
                                                    
                                                 VALUES ((%s),(%s),(%s),(%s),(%s))'''
        
        values = (row.Playlist_Id, 
                    row.Playlist_Name, 
                    row.Channel_Id, 
                    row.Channel_Name, 
                    row.Video_Count)

        try:
            mycursor.execute(insert_query, values)
            mydb.commit()

        except:
            pass


#Creating tables for Comments

def comments_table():
    
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="12345678",
                            database="Youtube_Data_Harvest",
                            port = "5432"
                            )
    
    mycursor = mydb.cursor()

    create_table_comment = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(255) primary key,
                                                                Video_Id varchar(255),
                                                                Comment_Text text,
                                                                Comment_Author varchar(255),
                                                                Comment_Publisheddate timestamp)'''
    
    mycursor.execute(create_table_comment)
    mydb.commit()

def insert_commentinfo(channels_name):
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data_Harvest",
                        port = "5432"
                        )

    mycursor = mydb.cursor()

    comment_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]

    for i in channels_names:
        for data in coll1.find({'channel_information.Channel_Name': i}, {'_id':0}):
            for i in range(len(data['comments_information'])):
                comment_list.append(data['comments_information'][i])

    df4 = pd.DataFrame(comment_list)

    for index,row in df4.iterrows():
        insert_query = '''insert into comments (Comment_Id, 
                                                Video_Id, 
                                                Comment_Text, 
                                                Comment_Author, 
                                                Comment_Publisheddate) 
                                                
                                                VALUES ((%s),(%s),(%s),(%s),(%s))'''
        
        values = (row.Comment_Id, 
                  row.Video_Id, 
                  row.Comment_Text, 
                  row.Comment_Author, 
                  row.Comment_Publisheddate)
        try:

            mycursor.execute(insert_query, values)
            mydb.commit()

        except:
            pass


#Function to Create table for Channels, Videos, Playlists and Comments
        
def tables():
    channel_table()
    videos_table()
    playlists_table()
    comments_table()


#Function to migrate data to sql:

def insertion(channels_names):
    insert_channelinfo(channels_names)
    insert_videosinfo(channels_names)
    insert_Playlistsinfo(channels_names)
    insert_commentinfo(channels_names)
    
    return "Migrated {} successfully".format(channels_names)


#Function to show channel table in the streamlit application 
def show_channel_table():
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]
    channel_list=[]
    for data in coll1.find({}, {'_id':0, 'channel_information':1}):
        channel_list.append(data['channel_information'])
    df = st.dataframe(channel_list)

    return df


#Function to show videos table in the streamlit application 
def show_videos_table():
    videos_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]

    for data in coll1.find({}, {'_id':0, 'video_information':1}):
        for i in range(len(data['video_information'])):
            videos_list.append(data['video_information'][i])

    df1 = st.dataframe(videos_list)

    return df1


#Function to show playlists table in the streamlit application 
def show_playlists_table():
    playlist_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]

    for data in coll1.find({}, {'_id':0, 'playlists_information':1}):
        for i in range(len(data['playlists_information'])):
            playlist_list.append(data['playlists_information'][i])

    df2 = st.dataframe(playlist_list)

    return df2


#Function to show comments table in the streamlit application 
def show_comments_table():
    comment_list=[]
    db = client["Youtube_Data_Harvest"]
    coll1 = db["Youtube_Channel_Data"]

    for data in coll1.find({}, {'_id':0, 'comments_information':1}):
        for i in range(len(data['comments_information'])):
            comment_list.append(data['comments_information'][i])

    df3 = st.dataframe(comment_list)

    return df3
    

#Streamlit code

st.set_page_config(page_title="Youtube Data Harvesting", page_icon=":bar_chart:", layout="wide")
st.markdown("""<style>
                span[data-baseweb="tag"] {
                background-color: green !important;}
                </style>""",
                unsafe_allow_html=True,
            )


st.title(":rainbow[Youtube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]  :bar_chart:")

with st.container():
    st.header(":blue[Skills take away]")
    st.caption(":blue[Python scripting]")
    st.caption(":red[API Integration]")
    st.caption(":green[Data collection using youtube API]")
    st.caption(":green[MongoDB]")
    st.caption(":orange[Data Management using MongoDB and SQL]")

col1, col2 = st.columns(2)
with col1:
    channel_ids_list = st.text_input(f"Enter the channel ids:").split()

    ids_to_store = st.multiselect("Select the channels you want to insert", options= channel_ids_list)
     
    if st.button("Store data in MongoDB"):
        channel_ids=[]
        db = client["Youtube_Data_Harvest"]
        coll1 = db["Youtube_Channel_Data"]
    
        for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
            channel_ids.append(ch_data["channel_information"]["Channel_Id"])
        
        for i in ids_to_store:
            if i in channel_ids:
                st.success("Channel information of {} id already exists. Please enter different id .".format(i))

            else:
                store_DB = Youtube_Data(i)
                st.success(store_DB)

    channel_names_display =[]

    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
            channel_names_display.append(ch_data["channel_information"]["Channel_Name"])

    channels_names = st.multiselect("Select the channels you want to migrate", options = channel_names_display)
    
    mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345678",
                    database="Youtube_Data_Harvest",
                    port = "5432"
                    )
    mycursor = mydb.cursor()
            
    if st.button("Migrate data to SQL"):
        tables()

        Query_channels = '''select channel_name from channels'''

        mycursor.execute(Query_channels)
        mydb.commit()

        t1_names = mycursor.fetchall()
        df = pd.DataFrame(t1_names, columns = ['Channel_Name'])
        Channels_list = df['Channel_Name'].tolist()

        for i in channels_names:
            if i in Channels_list:
                st.success("Channel information of {} already exists.".format(i))
            else:
                insert = insertion(i)
                st.success(insert)
                    
show_table = st.radio("Select any of the below options to view thier corresponding information:", 
                    ("Channels", "Videos", "Playlists", "Comments"))

if show_table == "Channels":
    show_channel_table()

elif show_table == "Videos":
    show_videos_table()

elif show_table == "Playlists":
    show_playlists_table()

elif show_table == "Comments":
    show_comments_table()

mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345678",
                    database="Youtube_Data_Harvest",
                    port = "5432"
                    )
mycursor = mydb.cursor()



col3, col4 = st.columns(2)

with col3:

    Question = st.selectbox("Select the Question",
                            ("1. All the videos and their corresponding channels",
                            "2. Channels with most number of videos and their number of videos",
                            "3. Top 10 most viewed videos and their respective channels",
                            "4. Comments made on each video and their corresponding video names",
                            "5. Videos having the highest number of likes and their corresponding channel",
                            "6. Total number of likes and dislikes for each video and their corresponding video names",
                            "7. Total number of views for each channel and their corresponding channel",
                            "8. Names of all the channels that have published videos in the year 2022",
                            "9. Average duration of all videos in each channel and their corresponding channel",
                            "10. Videos having the highest number of comments and their corresponding channel") )


    if Question == "1. All the videos and their corresponding channels":

        Query1 = '''select video_name as videoname, channel_name as Channelname from videosinfo'''

        mycursor.execute(Query1)
        mydb.commit()

        t1 = mycursor.fetchall()
        df11 = pd.DataFrame(t1, columns=["video title","channel_name"])
        st.write(df11)


    elif Question == "2. Channels with most number of videos and their number of videos":

        Query2 = '''select channel_name as channelname, video_count as videocount from channels 
                    order by video_count desc '''
        
        mycursor.execute(Query2)
        mydb.commit()

        t2 = mycursor.fetchall()
        df12 = pd.DataFrame(t2, columns=["channel name","video count"])
        st.write(df12)


    elif Question == "3. Top 10 most viewed videos and their respective channels":

        Query3 = '''select view_count as viewcount, video_name as videoname, channel_name as channelname 
                    from videosinfo where view_count is not null 
                    order by view_count desc limit 10'''
        
        mycursor.execute(Query3)
        mydb.commit()

        t3 = mycursor.fetchall()
        df13 = pd.DataFrame(t3, columns=["view count","video name", "channel name"])
        st.write(df13)


    elif Question == "4. Comments made on each video and their corresponding video names":

        Query4 = '''select comment_count as commentcount, video_name as videoname, channel_name as channelname
                    from videosinfo where comment_count is not null 
                    order by comment_count desc'''
        
        mycursor.execute(Query4)
        mydb.commit()

        t4 = mycursor.fetchall()
        df14 = pd.DataFrame(t4, columns=["comment count","video name", "channel name"])
        st.write(df14)


    elif Question == "5. Videos having the highest number of likes and their corresponding channel":

        Query5 = '''select video_name as video_name,like_count as likecount, channel_name as channelname 
                    from videosinfo where like_count is not null 
                    order by like_count desc'''
        
        mycursor.execute(Query5)
        mydb.commit()

        t5 = mycursor.fetchall()
        df15 = pd.DataFrame(t5, columns=["video name","like count", "channel name"])
        st.write(df15)


    elif Question == "6. Total number of likes and dislikes for each video and their corresponding video names":
        Query6 = '''select video_name as videoname, like_count as likecount, dislike_count as dislikecount 
                    from videosinfo where like_count is not null'''
        
        mycursor.execute(Query6)
        mydb.commit()

        t6 = mycursor.fetchall()
        df16 = pd.DataFrame(t6, columns=["video name","like count", "dislike count"])
        st.write(df16)


    elif Question == "7. Total number of views for each channel and their corresponding channel":
        Query7 = '''select channel_name as channelname,  channel_views as channelviews 
                    from channels where channel_views is not null'''
        
        mycursor.execute(Query7)
        mydb.commit()

        t7 = mycursor.fetchall()
        df17 = pd.DataFrame(t7, columns=["channel name", "channel views"])
        st.write(df17)


    elif Question == "8. Names of all the channels that have published videos in the year 2022":

        Query8 = '''select channel_name as channelname, video_name as videoname, 
                    publishedat as releaseyear from videosinfo 
                    where extract(year from publishedat) = 2022'''
        
        mycursor.execute(Query8)
        mydb.commit()

        t8 = mycursor.fetchall()
        df18 = pd.DataFrame(t8, columns=["channel name", "video name", "published year"])
        st.write(df18)


    elif Question == "9. Average duration of all videos in each channel and their corresponding channel":

        Query9 = '''select channel_name as channelname, avg(duration) as averageduration 
                    from videosinfo group by channel_name'''
        
        mycursor.execute(Query9)
        mydb.commit()

        t9 = mycursor.fetchall()
        df5 = pd.DataFrame(t9, columns=["channelname", "AverageDuration"])

        T9 = []
        for index,row in df5.iterrows():
            channel_title = row["channelname"]
            average_duration = row["AverageDuration"]
            average_duration_str = str(average_duration)
            T9.append(dict(channeltitle= channel_title, avgduration = average_duration_str))

        df19 = pd.DataFrame(T9)
        st.write(df19)

        
    elif Question == "10. Videos having the highest number of comments and their corresponding channel":

        Query10 = '''select video_name as videoname, channel_name as channelname, comment_count as commentcount 
                    from videosinfo where comment_count is not null 
                    order by comment_count desc'''
        mycursor.execute(Query10)
        mydb.commit()

        t10 = mycursor.fetchall()
        df20 = pd.DataFrame(t10, columns=["video name", "channel name", "comment count"])
        st.write(df20)

with col4:

    st.write("View the Corresponding charts here:")
    if Question == "2. Channels with most number of videos and their number of videos":
        st.bar_chart(data=df12, x='channel name', y='video count', use_container_width=True)

    elif Question == "7. Total number of views for each channel and their corresponding channel":
        fig, ax = plt.subplots()
        ax.pie(df17['channel views'], labels=df17['channel name'], autopct='%1.1f%%')
        st.pyplot(fig)

    


        
        
    