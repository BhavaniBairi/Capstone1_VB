#Importing all the required packages

import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px


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
    
    return f"Stored :orange[{channel_info["Channel_Name"]}] in MongoDB successfully"

#Inserting channel information into SQL
def insert_channelinfo(channel_name):
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

    for data in coll1.find({'channel_information.Channel_Name': channel_name}, {'_id':0}):
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


#Inserting video information into SQL
def insert_videosinfo(channel_name):
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

    for data in coll1.find({'video_information.Channel_Name': channel_name}, {'_id':0}):
    
        for j in range(len(data['video_information'])):
            videos_list.append(data['video_information'][j])

    df2 = pd.DataFrame(videos_list)
    df2_new = df2.fillna(0)

    for index,row in df2_new.iterrows():
        
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


#Inserting Playlist information into SQL

def insert_Playlistsinfo(channel_name):
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
    for data in coll1.find({'channel_information.Channel_Name': channel_name}, {'_id':0}):
        
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


#Inserting Comment information into SQL

def insert_commentinfo(channel_name):
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

    for data in coll1.find({'channel_information.Channel_Name': channel_name}, {'_id':0}):
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


img = Image.open(r"C:\Users\rajub\OneDrive\Desktop\youtube.png")
st.set_page_config(page_title="Youtube Data Harvesting", page_icon=img, layout="wide")
st.markdown("""<style>
                span[data-baseweb="tag"] {
                background-color: blue !important;}
                </style>""",
                unsafe_allow_html=True,
            )


with st.sidebar:
    selected = option_menu(menu_title="YouTube", menu_icon='youtube',
                           options = ["Home", "Extract & Upload to MongoDB", "Migrate to SQL", "Insights"],
                           icons= ["house", "upload", "database", "bar-chart"])
    
cols = st.columns((0.3, 2), gap='medium')
img1 = Image.open(r"C:\Users\rajub\OneDrive\Desktop\Youtube_logo.png")

with cols[0]:
    st.image(img1)
with cols[1]:
    st.markdown("### :blue[Data Harvesting and Warehousing]")

img_d = Image.open(r"C:\Users\rajub\OneDrive\Desktop\images.jpeg")
img_d1 = Image.open(r"C:\Users\rajub\OneDrive\Desktop\like.jpg")
if selected == 'Home':

    cols = st.columns((3,0.5,3))

    with cols[0]:
        st.write("")
        st.write('''Welcome to the YouTube Data Harvesting and Warehousing project!
                  This Python-based tool is designed to efficiently gather comprehensive data from YouTube channels,
                  including channel details, playlists, videos, and comments.
                  The harvested data is then stored in both MongoDB and PostgreSQL databases for easy access and analysis.''')
        st.write("")
        st.write(''':blue[Data Harvesting:] Utilizes the YouTube Data API to fetch channel details, playlists, videos, and comments.''')
        st.write("")
        st.write(''':blue[Warehousing:] Stores the harvested data in MongoDB and PostgreSQL databases for efficient data management.''')
        st.write("")            
        st.write(''':blue[Streamlit Interface:] Provides a streamlined web interface powered by Streamlit for easy data fetching, migration, and analysis.''')
        st.write("")            
        st.write(''':blue[Analytical Queries:] Offers various analytical queries to explore YouTube channel analytics and video insights.''')
        st.write("")            
        st.write(''':blue[Scalable:] Can handle large volumes of data efficiently, making it suitable for both small-scale and large-scale data analysis projects.''')

    with cols[2]:
        st.write("")
        st.write("")
        st.image(img_d, width=400)
        st.image(img_d1, width=350)


if selected == 'Extract & Upload to MongoDB':
    
    col1, col2 = st.columns(2)

    with col1:
        channel = st.text_input("Enter the channel id:")
        extracted = st.button("Extract Data")

    if channel and extracted:
        try:
            ch_details = get_channel_details(channel)
            st.success(f'Extracted data from :orange["{ch_details["Channel_Name"]}"] channel')
            
        except:
            st.error("Please enter valid channel ID")
    
    if st.button("Upload to MongoDB"):
        channel_ids = []
        db = client["Youtube_Data_Harvest"]
        coll1 = db["Youtube_Channel_Data"]
    
        for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
            channel_ids.append(ch_data["channel_information"]["Channel_Id"])
        
        
        if channel in channel_ids:
            st.error("Channel information of this channel already exists.")

        else:
            store_DB = Youtube_Data(channel)
            st.success(store_DB)

    show_table = st.radio("Select any of the below options to view the information from MongoDB:", 
                    ("Channels", "Videos", "Playlists", "Comments"), horizontal=True)

    if show_table == "Channels":
        show_channel_table()

    elif show_table == "Videos":
        show_videos_table()

    elif show_table == "Playlists":
        show_playlists_table()

    elif show_table == "Comments":
        show_comments_table()

    


if selected == 'Migrate to SQL':

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

    Query_channels = '''select channel_name from channels'''

    mycursor.execute(Query_channels)
    mydb.commit()

    t1_names = mycursor.fetchall()
    df = pd.DataFrame(t1_names, columns = ['Channel_Name'])
    Channels_list = df['Channel_Name'].tolist()    

    if st.button("Migrate data to SQL"):
        
        for i in channels_names:
            if i in Channels_list:
                st.error("Channel information of {} already exists.".format(i))
            else:
                insert = insertion(i)
                st.success(insert)

    

    st.markdown("#### :orange[Information of below channels are already present in SQL] ")     
    st.write(df)
                


mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345678",
                    database="Youtube_Data_Harvest",
                    port = "5432"
                    )
mycursor = mydb.cursor()

if selected == 'Insights':

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
        df11 = pd.DataFrame(t1, columns=["Video title","Channel name"])
        #df11.reset_index(drop=True, inplace=True)
        st.write(df11)     

    

    elif Question == "2. Channels with most number of videos and their number of videos":
        cols = st.columns(2)

        with cols[0]:
            Query2 = '''select channel_name as channelname, video_count as videocount from channels 
                        order by video_count desc '''
            
            mycursor.execute(Query2)
            mydb.commit()

            t2 = mycursor.fetchall()
            df12 = pd.DataFrame(t2, columns=["Channel Name","Video Count"])
            st.write("")
            st.write(df12)
        
        with cols[1]:

            st.subheader(":blue[channels with most no. of videos]", divider='rainbow')
            st.bar_chart(data=df12, x='Channel Name', y='Video Count',use_container_width=True, height=500, color='Channel Name')


    elif Question == "3. Top 10 most viewed videos and their respective channels":
        cols = st.columns(2)

        with cols[0]:
            Query3 = '''select video_name as videoname, view_count as viewcount, channel_name as channelname 
                        from videosinfo where view_count is not null 
                        order by view_count desc limit 10'''
            
            mycursor.execute(Query3)
            mydb.commit()

            t3 = mycursor.fetchall()
            df13 = pd.DataFrame(t3, columns=["Video title", "View count", "Channel name"])
            st.write("")
            st.write(df13)
        
        with cols[1]:

            st.subheader(":orange[Videos with most views]", divider='rainbow')
            sorted_df13 = df13.sort_values(by='View count', ascending=True)
            fig13 = px.bar(sorted_df13, x='View count', y='Video title',
                    height=400, color_discrete_sequence=px.colors.sequential.Rainbow, hover_data="Channel name")
            st.plotly_chart(fig13)

    elif Question == "4. Comments made on each video and their corresponding video names":
        Query4 = '''select video_name, comment_text from comments Join videosinfo on comments.video_id = videosinfo.video_id'''
        
        mycursor.execute(Query4)
        mydb.commit()

        t4 = mycursor.fetchall()
        df14 = pd.DataFrame(t4, columns=["Video Title", "Comment Description"])
        st.write(df14)


    elif Question == "5. Videos having the highest number of likes and their corresponding channel":
        cols = st.columns(2)

        with cols[0]:
            
            Query5 = '''select video_name as video_name,like_count as likecount, channel_name as channelname 
                        from videosinfo where like_count is not null 
                        order by like_count desc'''
            
            mycursor.execute(Query5)
            mydb.commit()

            t5 = mycursor.fetchall()
            df15 = pd.DataFrame(t5, columns=["Video Title","Like count", "Channel name"])
            st.write("")
            st.write(df15)
        
        with cols[1]:

            st.subheader(":green[Videos with most likes]", divider='rainbow')
            sorted_df15 = df15.sort_values(by='Like count', ascending=False).head(10)
            srt_df15 = sorted_df15.sort_values(by='Like count', ascending=True)
            fig15 = px.bar(srt_df15, x='Like count', y='Video Title',
                           hover_data="Channel name", height=400,
                           color_discrete_sequence=px.colors.sequential.Magenta_r)
            st.plotly_chart(fig15)


    elif Question == "6. Total number of likes and dislikes for each video and their corresponding video names":
        
        Query6 = '''select video_name as videoname, like_count as likecount, dislike_count as dislikecount 
                    from videosinfo where like_count is not null'''
        
        mycursor.execute(Query6)
        mydb.commit()

        t6 = mycursor.fetchall()
        df16 = pd.DataFrame(t6, columns=["Video title","Like count", "Dislike count"])
        st.write(df16)



    elif Question == "7. Total number of views for each channel and their corresponding channel":
        cols = st.columns(2)

        with cols[0]:
            Query7 = '''select channel_name as channelname,  channel_views as channelviews 
                        from channels where channel_views is not null'''
            
            mycursor.execute(Query7)
            mydb.commit()

            t7 = mycursor.fetchall()
            df17 = pd.DataFrame(t7, columns=["Channel Name", "Channel Views"])
            st.write("")
            st.write(df17)

        with cols[1]:
            st.subheader(":blue[Total Views of channels]", divider='rainbow')
            
            fig17= px.pie(df17, values='Channel Views',names='Channel Name', hover_data=('Channel Name','Channel Views'),
                      hover_name='Channel Name', hole=0.5,
                      color_discrete_sequence=px.colors.sequential.Rainbow_r, height= 400,width= 500)
            st.plotly_chart(fig17)           


    elif Question == "8. Names of all the channels that have published videos in the year 2022":

        Query8 = '''select channel_name as channelname, video_name as videoname, 
                    publishedat as releaseyear from videosinfo 
                    where extract(year from publishedat) = 2022'''
        
        mycursor.execute(Query8)
        mydb.commit()

        t8 = mycursor.fetchall()
        df18 = pd.DataFrame(t8, columns=["Channel Name", "Video title", "Published year"])
        st.write("")
        st.write(df18)


    elif Question == "9. Average duration of all videos in each channel and their corresponding channel":
        cols = st.columns(2)

        with cols[0]:
            Query9 = '''select channel_name as channelname, avg(duration) as averageduration 
                        from videosinfo group by channel_name'''
            
            mycursor.execute(Query9)
            mydb.commit()

            t9 = mycursor.fetchall()
            df19 = pd.DataFrame(t9, columns=["Channel name", "Average Duration"])

            st.write("")
            st.write(df19)

        with cols[1]:
            st.subheader(":blue[Average duration of channels]", divider='rainbow')
            
            fig19= px.pie(df19, values='Average Duration',names='Channel name',
                          hover_data=('Channel name','Average Duration'),
                          hole=0.5,
                          color_discrete_sequence=px.colors.sequential.Rainbow_r, height= 400,width= 500)
            st.plotly_chart(fig19)

        
    elif Question == "10. Videos having the highest number of comments and their corresponding channel":
        cols = st.columns(2)

        with cols[0]:
            Query10 = '''select video_name as videoname, comment_count as commentcount, channel_name as channelname 
                        from videosinfo where comment_count is not null 
                        order by comment_count desc'''
            
            mycursor.execute(Query10)
            mydb.commit()

            t10 = mycursor.fetchall()
            df20 = pd.DataFrame(t10, columns=["Video Title", "Comment Count", "Channel Name"])
            st.write(df20)

        with cols[1]:

            st.subheader(":green[Videos with most comments]", divider='rainbow')
            sorted_df20 = df20.sort_values(by='Comment Count', ascending=False).head(10)
            srt_df20 = sorted_df20.sort_values(by='Comment Count', ascending=True)
            fig20 = px.bar(srt_df20, x='Comment Count', y='Video Title',
                    height=400, color_discrete_sequence=px.colors.sequential.Blues_r)
            st.plotly_chart(fig20)

        
            

               
