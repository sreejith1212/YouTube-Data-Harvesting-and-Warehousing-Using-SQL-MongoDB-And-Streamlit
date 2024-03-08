from googleapiclient.discovery import build
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from collections import OrderedDict
from pymongo import MongoClient
import pymysql

# Generated API Key for youtube channel data scrapping
api_key = "AIzaSyDqHoJ-feja59SSn0ja1M2AOf23dK0vXbM"
youtube = build('youtube', 'v3', developerKey=api_key)

# to retrieve channel info from youtube
def channel_info(channel_id):

    request = youtube.channels().list(part = "snippet,contentDetails,statistics", id = channel_id)
    response = request.execute()
    channel = OrderedDict()
    try:
        channel["Channel_Name"] = response["items"][0]["snippet"]["title"]
        channel["Channel_Id"] = response["items"][0]["id"]
        channel["Subscription_Count"] = response["items"][0]["statistics"]["subscriberCount"]
        channel["Channel_Views"] = response["items"][0]["statistics"]["viewCount"]
        channel["Channel_Video_Count"] = response["items"][0]["statistics"]["videoCount"]
        channel["Channel_Description"] = response["items"][0]["snippet"]["description"]
        channel["Playlist_Id"] = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except KeyError:
        print("A KeyError occurred while fetching channel info.")

    return channel

# to retrieve video ids of the channel
def video_id_info(playlist_id):

    page_token = None
    video_ids = []
    try:
        while True:
            request = youtube.playlistItems().list(part = "contentDetails", maxResults = 50, playlistId = playlist_id, pageToken = page_token)
            response = request.execute()
            video_ids += [item["contentDetails"]["videoId"] for item in response["items"]]
            page_token = response.get("nextPageToken")
            if page_token is None:
                break
    except Exception as e:
        print(e)
        
    return video_ids

# to retrieve video info
def video_info(video_id):

    request = youtube.videos().list(part = "snippet, contentDetails,statistics", id = video_id)
    response = request.execute()
    video_info = OrderedDict()
    try:
        video_info["Video_Id"] = response["items"][0]["id"]
        video_info["Video_Name"] = response["items"][0]["snippet"]["title"]
        video_info["Video_Description"] = response["items"][0]["snippet"]["description"]

        try:
            video_info["Tags"] = response["items"][0]["snippet"]["tags"]
        except KeyError:
            video_info["Tags"] = ""

        video_info["PublishedAt"] = response["items"][0]["snippet"]["publishedAt"]

        try:
            video_info["View_Count"] = response["items"][0]["statistics"]["viewCount"]
        except KeyError:
            video_info["View_Count"] = 0
        try:    
            video_info["Like_Count"] = response["items"][0]["statistics"]["likeCount"]
        except KeyError:
            video_info["Like_Count"] = 0
        try:
            video_info["Favorite_Count"] = response["items"][0]["statistics"]["favoriteCount"]
        except KeyError:
            video_info["Favorite_Count"] = 0
        try:
            video_info["Comment_Count"] = response["items"][0]["statistics"]["commentCount"]
        except KeyError:
            video_info["Comment_Count"] = 0

        video_info["Duration"] = response["items"][0]["contentDetails"]["duration"]

        try:
            video_info["Thumbnail"] = response["items"][0]["snippet"]["thumbnails"]["default"]["url"]
        except KeyError:
            video_info["Thumbnail"] = ""

        video_info["Caption_Status"] = response["items"][0]["contentDetails"]["caption"]

    except KeyError:
        print("A KeyError occurred while fetching video info.")
        
    return video_info

# to retrieve comment info of the video
def comment_info(video_id):

    page_token = None
    all_comments_info_list = []
    all_comments_data = {}
    try:
        while True:
            request = youtube.commentThreads().list(part = "snippet,replies", maxResults = 100, pageToken = page_token, videoId = video_id)
            response = request.execute()
    
            for i in range(len(response["items"])):
                comment_info = OrderedDict()
                comment_info["Comment_Id"] = response["items"][i]["snippet"]["topLevelComment"]["id"]
                comment_info["Comment_Text"] = response["items"][i]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                comment_info["Comment_Author"] = response["items"][i]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                comment_info["Comment_PublishedAt"] = response["items"][i]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                
                all_comments_info_list.append(comment_info)
                if response["items"][i].get("replies") is not None:
                    for com_num in range(len(response["items"][i]["replies"]["comments"])) :
                        comment_info = OrderedDict()
                        comment_info["Comment_Id"] = response["items"][i]["replies"]["comments"][com_num]["id"]
                        comment_info["Comment_Text"] = response["items"][i]["replies"]["comments"][com_num]["snippet"]["textDisplay"]
                        comment_info["Comment_Author"] = response["items"][i]["replies"]["comments"][com_num]["snippet"]["authorDisplayName"]
                        comment_info["Comment_PublishedAt"] = response["items"][i]["replies"]["comments"][com_num]["snippet"]["publishedAt"]
                        all_comments_info_list.append(comment_info)
    
            page_token = response.get("nextPageToken")
            if page_token is None:
                break

        for i in range(len(all_comments_info_list)):
            all_comments_data["{}".format(i)] = all_comments_info_list[i]

    except Exception as e:
        print(e)
    
    return all_comments_data

# to combine channel info, video info, comment info of a channel
def combine_channel_info(channel_id):

    channel_combine_data = {}
    channel_data = channel_info(channel_id)
    channel_combine_data["Channel_Info"] = channel_data
    playlist_id = channel_data["Playlist_Id"]
    channel_video_ids = video_id_info(playlist_id)
    channel_videos = {}
    for i in range(len(channel_video_ids)):
        video_data = video_info(channel_video_ids[i])
        channel_comments = comment_info(channel_video_ids[i])
        channel_videos["video_{}".format(i)] = video_data
        channel_videos["video_{}".format(i)]["Comments"] = channel_comments
    channel_combine_data["Video_Info"] = channel_videos

    return channel_combine_data

# delete all data corresponding to the input channel from the sql database
def delete_sql_data(channel_harvest_data, connection, cursor):

    channel_Id = channel_harvest_data["Channel_Info"]["Channel_Id"]

    cursor.execute("SELECT * FROM channel WHERE Channel_Id = %s", (channel_Id,))
    results = cursor.fetchall()
    if len(results) > 0:
        cursor.execute("DELETE FROM channel WHERE Channel_Id = %s", (channel_Id,))
        connection.commit()

# save channel info to sql database
def channel_info_to_sql(channel_harvest_data, connection, cursor):
    
    channel_Id = channel_harvest_data["Channel_Info"]["Channel_Id"]
    channel_name = channel_harvest_data["Channel_Info"]["Channel_Name"]
    subscription_Count = int(channel_harvest_data["Channel_Info"]["Subscription_Count"])
    channel_Description = channel_harvest_data["Channel_Info"]["Channel_Description"]
    playlist_Id = channel_harvest_data["Channel_Info"]["Playlist_Id"]

    sql = "INSERT INTO channel (Channel_Id, Channel_Name, Subscription_Count, Channel_Description) VALUES (%s, %s, %s, %s)"
    val = (channel_Id, channel_name, subscription_Count, channel_Description) 
    cursor.execute(sql, val)
    connection.commit()
    
    sql = "INSERT INTO playlist (Playlist_Id, Channel_Id) VALUES (%s, %s)"
    val = (playlist_Id, channel_Id) 
    cursor.execute(sql, val)
    connection.commit()

# save video info to sql database
def video_info_to_sql(channel_harvest_data, connection, cursor):
    
    video_list = []
    video_count = len(channel_harvest_data["Video_Info"])
    playlist_id = channel_harvest_data["Channel_Info"]["Playlist_Id"]
    for i in range(video_count):
        video_id = channel_harvest_data["Video_Info"][f"video_{i}"]["Video_Id"]                   
        video_name = channel_harvest_data["Video_Info"][f"video_{i}"]["Video_Name"]            
        video_description = channel_harvest_data["Video_Info"][f"video_{i}"]["Video_Description"]         
        tags = ",".join(channel_harvest_data["Video_Info"][f"video_{i}"]["Tags"])               
        publishedAt = channel_harvest_data["Video_Info"][f"video_{i}"]["PublishedAt"]                
        view_count = channel_harvest_data["Video_Info"][f"video_{i}"]["View_Count"]                 
        like_count = channel_harvest_data["Video_Info"][f"video_{i}"]["Like_Count"]                  
        favorite_count = channel_harvest_data["Video_Info"][f"video_{i}"]["Favorite_Count"]                                 
        duration = channel_harvest_data["Video_Info"][f"video_{i}"]["Duration"]                   
        thumbnail = channel_harvest_data["Video_Info"][f"video_{i}"]["Thumbnail"]                   
        caption_status = channel_harvest_data["Video_Info"][f"video_{i}"]["Caption_Status"]       
        video_list.append([video_id, playlist_id, video_name, video_description, tags, publishedAt, view_count, like_count, favorite_count, 
                            duration, thumbnail, caption_status])
        
    video_dataframe = pd.DataFrame(video_list, columns = ['video_id', 'playlist_id', 'video_name', 'video_description', 'tags',
                                              'publishedAt', 'view_count', 'like_count', 'favorite_count', 'duration', 'thumbnail', 'caption_status'])

    video_dataframe["publishedAt"] = pd.to_datetime(video_dataframe["publishedAt"]).dt.tz_localize(None)
    video_dataframe["view_count"] = video_dataframe["view_count"].astype(int)
    video_dataframe["like_count"] = video_dataframe["like_count"].astype(int)
    video_dataframe["favorite_count"] = video_dataframe["favorite_count"].astype(int)
    video_dataframe["duration"] = pd.to_timedelta(video_dataframe["duration"]).dt.total_seconds().astype(int)

    for i, row in video_dataframe.iterrows():
        sql = ("INSERT IGNORE INTO video (Video_Id, Playlist_Id, Video_Name, Video_Description, Tags, PublishedAt, View_Count, Like_Count, Favorite_Count," 
               " Duration_In_Seconds, Thumbnail, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = tuple(row)
        cursor.execute(sql, val)
        connection.commit()

# save comment info to sql database
def comment_info_to_sql(channel_harvest_data, connection, cursor):
    
    comment_list = []
    videos = len(channel_harvest_data["Video_Info"])
    for i in range(videos):
        video_id = channel_harvest_data["Video_Info"][f"video_{i}"]["Video_Id"]
        comments = len(channel_harvest_data["Video_Info"][f"video_{i}"]["Comments"])
        for j in range(comments):
            comment_id = channel_harvest_data["Video_Info"][f"video_{i}"]["Comments"][f"{j}"]["Comment_Id"]
            comment_text = channel_harvest_data["Video_Info"][f"video_{i}"]["Comments"][f"{j}"]["Comment_Text"]
            comment_author = channel_harvest_data["Video_Info"][f"video_{i}"]["Comments"][f"{j}"]["Comment_Author"]
            comment_published_at = channel_harvest_data["Video_Info"][f"video_{i}"]["Comments"][f"{j}"]["Comment_PublishedAt"]
            comment_list.append([comment_id, video_id, comment_text, comment_author, comment_published_at])
    
    comment_dataframe = pd.DataFrame(comment_list, columns = ['comment_id', 'video_id', 'comment_text', 'comment_author', 'comment_published_at'])

    comment_dataframe["comment_published_at"] = pd.to_datetime(comment_dataframe["comment_published_at"]).dt.tz_localize(None)

    for i, row in comment_dataframe.iterrows():
        sql = "INSERT IGNORE INTO comment (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_PublishedAt) VALUES (%s, %s, %s, %s, %s)"
        val = tuple(row)
        cursor.execute(sql, val)
        connection.commit()

# delete complete channel data from mongoDB, if any and save newly retrieved channel data from youtube to mongoDB 
def save_to_mongodb(search_channel_id, collection_name, data_harvest):

    required_channel = collection_name.find_one({"Channel_Info.Channel_Id" : {"$eq" : search_channel_id}}, {"_id" : 0})

    if required_channel == None:
        collection_name.insert_one(data_harvest)
    else :
        collection_name.delete_one({"Channel_Info.Channel_Id" : {"$eq" : search_channel_id}})
        collection_name.insert_one(data_harvest)

    return collection_name.find_one({"Channel_Info.Channel_Id" : {"$eq" : search_channel_id}}, {"_id" : 0})

# create required tables in sql database if they don't exist
def create_mysql_tables(cursor):

    cursor.execute("SHOW TABLES")
    tables_sql = []
    for table in cursor:
        tables_sql.append(table[0])

    if "channel" not in tables_sql:
        cursor.execute("CREATE TABLE channel ( "
                        "Channel_Id VARCHAR(100),"
                        "Channel_Name VARCHAR(200),"
                        "Subscription_Count INT,"
                        "Channel_Description TEXT,"
                        "PRIMARY KEY (Channel_Id) )")
    if "playlist" not in tables_sql:
        cursor.execute("CREATE TABLE playlist ( "
                        "Playlist_Id VARCHAR(100),"
                        "Channel_Id VARCHAR(100),"
                        "PRIMARY KEY (Playlist_Id),"
                        "FOREIGN KEY (Channel_Id) REFERENCES channel (channel_id) ON DELETE CASCADE )")
    if "video" not in tables_sql:
        cursor.execute("CREATE TABLE video ( "
                        "Video_Id VARCHAR(100),"
                        "Playlist_Id VARCHAR(100),"
                        "Video_Name VARCHAR(255),"
                        "Video_Description TEXT,"
                        "Tags TEXT,"
                        "PublishedAt DATETIME,"
                        "View_Count INT,"
                        "Like_Count INT,"
                        "Favorite_Count INT,"
                        "Duration_In_Seconds INT,"
                        "Thumbnail VARCHAR(255),"
                        "Caption_Status VARCHAR(50),"
                        "PRIMARY KEY (Video_Id),"
                        "FOREIGN KEY (Playlist_Id) REFERENCES playlist (Playlist_Id) ON DELETE CASCADE )")
    if "comment" not in tables_sql:
        cursor.execute("CREATE TABLE comment ( "
                        "Comment_Id VARCHAR(200),"
                        "Video_Id VARCHAR(100),"
                        "Comment_Text TEXT,"
                        "Comment_Author VARCHAR(200),"
                        "Comment_PublishedAt DATETIME,"
                        "PRIMARY KEY (Comment_Id),"
                        "FOREIGN KEY (Video_Id) REFERENCES video (Video_Id) ON DELETE CASCADE )")

# retrieve query result from sql database and save it to a dataframe
def question_answer(sql, result_columns, cursor, connection):

    cursor.execute(sql)
    connection.commit()
    result = cursor.fetchall()
    result_df = pd.DataFrame(result, columns = result_columns)
    result_df['Index'] = range(1, len(result_df) + 1)
    result_df.set_index('Index', inplace=True)
    st.dataframe(result_df, width=800)


if __name__ == "__main__":

    # connect to mongoDB database    
    client = MongoClient("mongodb://localhost:27017")

    # create/select database and collection
    mongo_database_name = client["youtube"]
    collection_name = mongo_database_name["data_harvest"]

    host = 'localhost' 
    user = 'root' 
    password = '12121995' 
    dbname = 'youtube_data_harvest'

    # connect to required sql database, if the database does not exist, create the database and connect to it
    try:
        connection = pymysql.connect(host=host, user=user, password=password, db=dbname)
        cursor = connection.cursor()
    except Exception as e:
        connection = pymysql.connect(host=host, user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE {}".format(dbname))
        connection = pymysql.connect(host=host, user=user, password=password, db=dbname)
        cursor = connection.cursor()
    
    create_mysql_tables(cursor)

    # set app page layout type
    st.set_page_config(layout="wide")

    # create sidebar
    with st.sidebar:        
        page = option_menu(
                            menu_title='YouTube',
                            options=['Data Collection And Migration','Q & A'],
                            icons=['person-circle','trophy-fill'],
                            default_index=1 ,
                            styles={"container": {"padding": "5!important"},
                                    "icon": {"color": "brown", "font-size": "23px"}, 
                                    "nav-link": {"color":"white","font-size": "20px", "text-align": "left", "margin":"0px", "--hover-color": "lightblue"},
                                    "nav-link-selected": {"background-color": "grey"},}  
                        )
   
    if page == "Data Collection And Migration":

        st.header(':green[_YouTube Data Collection and Warehousing_] :books:')
        st.header("")
        col1, col2, col3 = st.columns(3)
        search_channel_id = col1.text_input("Search Channel", placeholder = "Type Here ...")
        
        col4, col5 = st.columns([1,4])
        col6, col7, col8 = st.columns(3)
        if col4.button("Search"):
            if len(search_channel_id) != 0:         
                # retrieve channel info from youtube
                channel_data = channel_info(search_channel_id)
                if len(channel_data) == 0:
                    col6.error('Invalid Channel ID, Please Input Valid Channel ID', icon="🚨")
                else:
                    channel_data_df = pd.DataFrame([channel_data])
                    channel_data_df.drop(columns=["Channel_Id", "Playlist_Id"], inplace=True)
                    channel_data_df = channel_data_df.T
                    channel_data_df = channel_data_df.rename(columns={0:""})
                    st.dataframe(channel_data_df)
            else:
                col6.error('Please enter a valid Channel ID', icon="⚠️")
        if col5.button("Save to MongoDB"):

            if len(search_channel_id) != 0:
                channel_data = channel_info(search_channel_id)
                if len(channel_data) == 0:
                    col6.error('Invalid Channel ID, Please Input Valid Channel ID', icon="🚨")
                else:
                    # retrieve complete channel data from youtube
                    data_harvest = combine_channel_info(search_channel_id)
                    # save channel data to mongoDB
                    channel_data_mongo_db = save_to_mongodb(search_channel_id, collection_name, data_harvest)
                    st.write(f":violet[_Channel ID (':blue[{search_channel_id}]') data successfully saved to MongoDB_]")
                    container = st.container(border=True)
                    container.write("Expand to view extracted data")
                    container.json(channel_data_mongo_db, expanded=False)
            else:
                col6.error('Please enter a valid Channel ID', icon="⚠️")

        st.header("")
        all_channel_data_mongo_db = collection_name.find({}, {"_id" : 0})
        all_channel_mongo_db = []
        for channel in all_channel_data_mongo_db:
            channel_name = channel["Channel_Info"]["Channel_Name"]
            channel_id = channel["Channel_Info"]["Channel_Id"]
            all_channel_mongo_db.append(channel_name + " - " + channel_id)
        col9, col10, col11 = st.columns(3)
        # Dropdown displays all channels available in the MongoDB 
        option = col9.selectbox("Select Channel To Migrate To SQL database", all_channel_mongo_db, index=None, placeholder="Select a channel")
        
        if st.button("Migrate to SQL database"):
            col12, col13, col14 = st.columns(3)
            if option is not None:         
                selected_channel_id = option.split(" - ")[1].strip()
                required_channel = collection_name.find_one({"Channel_Info.Channel_Id" : {"$eq" : selected_channel_id}}, {"_id" : 0})
                delete_sql_data(required_channel, connection, cursor)
                channel_info_to_sql(required_channel, connection, cursor)
                video_info_to_sql(required_channel, connection, cursor)
                comment_info_to_sql(required_channel, connection, cursor)
                st.write(f":violet[_Channel (':blue[{option}]') data successfully migrated to SQL Database_]")
            else:
                col12.error('Please select a channel to migrate', icon="⚠️")
  

    if page == "Q & A":
        
        st.title(":green[_General Questions On Migrated Data_] :notebook:")
        col1,col2,col3 = st.columns([2,1,2])

        # display all questions amd the user can view the corresponding answers by exapanding each question 

        with st.expander("1\) What are the names of all the videos and their corresponding channels?"):
            sql = ("SELECT video.Video_Name, channel.Channel_Name FROM video INNER JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id "
                   "INNER JOIN channel ON playlist.Channel_id = channel.Channel_Id") 
            result_columns = ["Video Name", "Channel Name"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("2\) Which channels have the most number of videos, and how many videos do they have?"):
            sql = ("SELECT channel.Channel_Name, COUNT(video.Video_Id) AS no_of_videos FROM channel INNER JOIN playlist ON channel.Channel_Id = playlist.Channel_Id "
                   "LEFT JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP BY channel.Channel_Name ORDER BY no_of_videos DESC")
            result_columns = ["Channel Name", "Number Of Videos"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("3\) What are the top 10 most viewed videos and their respective channels?"):
            sql = ("SELECT video.Video_Name, video.View_Count, channel.Channel_Name FROM video INNER JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id "
                   "INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id ORDER BY video.View_Count DESC LIMIT 10")    
            result_columns = ["Video Name", "View Count", "Channel Name"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("4\) How many comments were made on each video, and what are their corresponding video names?"):
            sql = ("SELECT video.Video_Name, COUNT(comment.Comment_Id) AS no_of_comments FROM comment RIGHT JOIN video ON comment.Video_Id = video.Video_Id "
                   "GROUP BY video.Video_Name ORDER BY no_of_comments DESC")      
            result_columns = ["Video Name", "Number Of Comments"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("5\) Which videos have the highest number of likes, and what are their corresponding channel names?"):
            sql = ("SELECT video.Video_Name, video.Like_Count, channel.Channel_Name FROM video INNER JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id "
                   "INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id ORDER BY video.Like_Count DESC")         
            result_columns = ["Video Name", "Like Count", "Channel Name"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("6\) What is the total number of likes for each video, and what are their corresponding video names?"):
            sql = ("SELECT video.Video_Name, video.Like_Count FROM video ORDER BY video.Like_Count DESC")        
            result_columns = ["Video Name", "Like Count"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("7\) What is the total number of views for each channel, and what are their corresponding channel names?"):
            sql = ("SELECT channel.Channel_Name, COALESCE(sum(video.View_Count), 0) AS Total_Views FROM video RIGHT JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id "
                   "INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id GROUP BY channel.Channel_Name ORDER BY Total_Views DESC")   
            result_columns = ["Channel Name", "Total Views"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("8\) What are the names of all the channels that have published videos in the year 2022?"):
            sql = ("SELECT channel.Channel_Name, COUNT(video.Video_Id) AS no_of_videos_in_year_2022 FROM video INNER JOIN "
                   "playlist ON video.Playlist_id = playlist.Playlist_Id INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id "
                   "WHERE YEAR(video.PublishedAt) = 2022 GROUP BY channel.Channel_Name ORDER BY no_of_videos_in_year_2022 DESC")
            result_columns = ["Channel Name", "Number Of Videos In Year 2022"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("9\) What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
            sql = ("SELECT channel.Channel_Name, COALESCE(AVG(video.Duration_In_Seconds), 0) AS Average_Video_Duration_In_Seconds FROM video "
                   "RIGHT JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id "
                   "GROUP BY channel.Channel_Name ORDER BY Average_Video_Duration_In_Seconds DESC")           
            result_columns = ["Channel Name", "Average Video Duration In Seconds"]
            question_answer(sql, result_columns, cursor, connection)

        with st.expander("10\) Which videos have the highest number of comments, and what are their corresponding channel names?"):
            sql = ("SELECT video.Video_Name, COUNT(comment.Comment_Id) AS Comment_Count, channel.Channel_Name FROM comment "
                   "RIGHT JOIN video ON comment.Video_Id = video.Video_Id INNER JOIN playlist ON video.Playlist_Id = playlist.Playlist_Id "
                   "INNER JOIN channel ON playlist.Channel_Id = channel.Channel_Id GROUP BY video.Video_Name, channel.Channel_Name ORDER BY Comment_Count DESC")
            result_columns = ["Video Name", "Comment Count", "Channel Name"]
            question_answer(sql, result_columns, cursor, connection)