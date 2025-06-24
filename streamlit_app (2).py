import streamlit as st
import pandas as pd
import sqlite3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re

# --- YOUTUBE API SETUP ---
API_KEY = "AIzaSyAEBISh8DJZNggnU9WfuUPVAFNpENz5py0"
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

def get_youtube_service():
    return build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

# --- DB UTILS ---
def fetch_data(query):
    conn = sqlite3.connect('db1.db')
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- YOUTUBE HELPERS ---
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

def channel_info(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    if "items" not in response or not response["items"]:
        return None
    item = response["items"][0]
    data = {
        "channel_name": item["snippet"]["title"],
        "channel_id": item["id"],
        "channel_des": item["snippet"]["description"],
        "channel_playid": item["contentDetails"]["relatedPlaylists"]["uploads"],
        "channel_viewcount": item["statistics"].get("viewCount", 0),
        "channel_subcount": item["statistics"].get("subscriberCount", 0)
    }
    return pd.DataFrame(data, index=[0])

def playlist_videos_id(youtube, channel_id):
    video_ids = []
    try:
        response = youtube.channels().list(
            part="contentDetails", id=channel_id).execute()
        if 'items' not in response or not response['items']:
            return []
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        nextPageToken = None
        while True:
            pl_request = youtube.playlistItems().list(
                part="snippet", playlistId=playlist_id, maxResults=50, pageToken=nextPageToken)
            pl_response = pl_request.execute()
            for item in pl_response["items"]:
                video_ids.append(item["snippet"]["resourceId"]["videoId"])
            nextPageToken = pl_response.get("nextPageToken")
            if not nextPageToken:
                break
    except Exception as e:
        st.error(f"Error fetching playlist videos: {e}")
    return video_ids

def videos_data(youtube, video_ids):
    video_stats = []
    for video_id in video_ids:
        try:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics", id=video_id)
            response = request.execute()
            if response["items"]:
                video = response["items"][0]
                video_info = {
                    "Video_Id": video_id,
                    "Video_title": video["snippet"]["title"],
                    "Video_Description": video["snippet"].get("description", ""),
                    "channel_id": video["snippet"]["channelId"],
                    "video_Tags": ','.join(video["snippet"].get("tags", [])) if "tags" in video["snippet"] else "",
                    "Video_pubdate": video["snippet"]["publishedAt"],
                    "Video_viewcount": int(video["statistics"].get("viewCount", 0)),
                    "Video_likecount": int(video["statistics"].get("likeCount", 0)),
                    "Video_favoritecount": int(video["statistics"].get("favoriteCount", 0)),
                    "Video_commentcount": int(video["statistics"].get("commentCount", 0)),
                    "Video_duration": iso8601_duration_to_seconds(video["contentDetails"]["duration"]),
                    "Video_thumbnails": video["snippet"]["thumbnails"]['default']['url'],
                    "Video_caption": video["contentDetails"].get("caption", "")
                }
                video_stats.append(video_info)
        except Exception as e:
            st.warning(f"Error fetching video {video_id}: {e}")
    return pd.DataFrame(video_stats)

def comments_inf(youtube, video_ids):
    commentdata = []
    for video_id in video_ids:
        nextpagetoken = None
        while True:
            try:
                request = youtube.commentThreads().list(
                    part="snippet", videoId=video_id, maxResults=50, pageToken=nextpagetoken)
                response = request.execute()
                for item in response["items"]:
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]
                    commentdata.append({
                        "comment_id": item["snippet"]["topLevelComment"]["id"],
                        "Comment_Text": top_comment["textDisplay"],
                        "Comment_Authorname": top_comment["authorDisplayName"],
                        "published_date": top_comment["publishedAt"],
                        "video_id": top_comment["videoId"],
                        "channel_id": item["snippet"].get("channelId", ""),
                    })
                nextpagetoken = response.get('nextPageToken')
                if not nextpagetoken:
                    break
            except Exception:
                break
    return pd.DataFrame(commentdata)

# --- DB INSERTS ---
def insert_channels_into_sqlite(df):
    if df is not None and not df.empty:
        conn = sqlite3.connect('db1.db')
        df.to_sql('channels', conn, if_exists='append', index=False)
        conn.close()

def insert_videos_into_sqlite(df):
    if df is not None and not df.empty:
        conn = sqlite3.connect('db1.db')
        df.to_sql('videos', conn, if_exists='append', index=False)
        conn.close()

def insert_comments_into_sqlite(df):
    if df is not None and not df.empty:
        conn = sqlite3.connect('db1.db')
        df.to_sql('comments', conn, if_exists='append', index=False)
        conn.close()

# --- STREAMLIT APP ---
#Function to execute the predefined queries
def execute_query(question):
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
		         """SELECT Video_title,channel_name
                 FROM videos
                 JOIN channels ON channels.channel_id=videos.channel_id;""",
        "Which channels have the most number of videos, and how many videos do they have?":
		         """SELECT channel_name, COUNT(video_id) AS video_count
				 FROM videos
                 JOIN Channels ON channels.channel_id=videos.channel_id
                 GROUP BY channel_name
                 ORDER BY video_count DESC;""",
        "What are the top 10 most viewed videos and their respective channels?":
		         """SELECT video_title,channel_name
                 FROM videos
                 JOIN channels ON channels.channel_id =videos.channel_id
                 ORDER BY video_viewcount DESC
                 LIMIT 10;""",
        "How many comments were made on each video, and what are their corresponding video names?":
		         """SELECT video_title, COUNT(*) AS comment_counts
                 FROM videos
                 JOIN comments on videos.video_id=comments.video_id
                 GROUP BY video_title;""",
        "Which videos have the highest number of likes, and what are their corresponding channel names?":
		         """SELECT video_title,channel_name
                 FROM videos
                 JOIN channels ON channels.channel_id=videos.channel_id
                 ORDER BY video_likecount DESC
                 LIMIT 1;""",
        "What is the total number of likes for each video, and what are their corresponding video names?":
                """SELECT videos.Video_title, SUM(videos.Video_likecount) AS total_likes
                  FROM videos
                  GROUP BY videos.Video_title;""",
        "What is the total number of views for each channel, and what are their corresponding channel names?":
		          """SELECT channel_name, SUM(video_viewcount) AS Total_views
                  FROM videos
                  JOIN channels ON channels.channel_id=videos.channel_id
                  GROUP BY channel_name;""",
        "What are the names of all the channels that have published videos in the year 2022?":
		          """SELECT DISTINCT channels.channel_name
                  FROM channels
                  JOIN videos ON channels.channel_id = videos.channel_id
                  WHERE YEAR(videos.Video_pubdate) = 2022;""",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
		          """ SELECT channel_name,AVG(video_duration) AS Average_duration
                  FROM videos
                  JOIN channels ON videos.channel_id = channels.channel_id
                  GROUP BY channel_name;""",
        "Which videos have the highest number of comments, and what are their corresponding channel names?":
		          """ SELECT video_title,channel_name
                  FROM videos
                  JOIN channels ON videos.channel_id = channels.channel_id
                  ORDER BY Video_commentcount DESC
                  LIMIT 1;"""
    }

    query=query_mapping.get(question)
    if query:
        return fetch_data(query)
    else:
        return pd.DataFrame()


import sqlite3
import pandas as pd

def fetch_channel_data(newchannel_id):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to check if the channel exists
    query = "SELECT * FROM channels WHERE channel_id = ?"
    df = pd.read_sql_query(query, conn, params=(newchannel_id,))

    # Check if the channel exists
    if not df.empty:
        existing_channel = df.iloc[0]
        print("Channel already exists in the database.")
        return existing_channel

    # Close the connection
    conn.close()

    # If the channel doesn't exist, fetch data using the API key (implement your API logic here)
    # ... (Your API fetching logic)
    # ...
    # Assuming you've fetched the channel data into a DataFrame 'new_channel_data'
    new_channel_data.to_sql('channels', conn, if_exists='append', index=False)

    return new_channel_data


import sqlite3
import pandas as pd

def fetch_channel_data(newchannel_id):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Execute the query to check if the channel exists
    query = "SELECT * FROM channels WHERE channel_id = ?"
    df = pd.read_sql_query(query, conn, params=(newchannel_id,))

    # Check if the channel exists
    if not df.empty:
        existing_channel = df.iloc[0]
        print("Channel already exists in the database.")
        return existing_channel

    # Close the connection
    conn.close()

    # If the channel doesn't exist, fetch data using the API key (implement your API logic here)
    try:
        # Your API fetching logic here
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=newchannel_id
        )
        response = request.execute()

        if 'items' in response and len(response["items"]) > 0:
            data = {
                "channel_name": response["items"][0]["snippet"]["title"],
                "channel_id": newchannel_id,
                "channel_des": response["items"][0]["snippet"]["description"],
                "channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                "channel_viewcount": response["items"][0]["statistics"]["viewCount"],
                "channel_subcount": response["items"][0]["statistics"]["subscriberCount"]
            }

            # Insert the fetched data into the SQLite database
            new_channel_data = pd.DataFrame(data, index=[0])
            new_channel_data.to_sql('channels', conn, if_exists='append', index=False)

            return new_channel_data

        else:
            print("No items found in the response.")
            return pd.DataFrame()

    except HttpError as e:
        print(f"HTTP Error: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"KeyError: {e}. Please make sure the channel ID is correct.")
        return pd.DataFrame()

#Function to fetch the video ID using channel ID
def playlist_videos_id(channel_ids):
    all_video_ids=[]
    for newchannel_id in channel_ids:
        videos_ids=[]
        try:
            response = youtube.channels().list(part="contentDetails",id=newchannel_id).execute()
            if 'items' in response and len(response["items"]) > 0:
                playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                nextPageToken=None

                while True:
                    response2 = youtube.playlistItems().list(
                       part="snippet",
                       playlistId=playlist_Id,maxResults=50,
                       pageToken=nextPageToken).execute()
                    for i in range(len(response2.get("items",[]))):
                       videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
                    nextPageToken=response2.get("nextPageToken")
                    if nextPageToken is None:
                        break
                else:
                    st.error(f"No channels found for ID: {newchannel_id}")
        except HttpError as e:
            st.error(f"HTTP Error: {e}")
        except KeyError as e:
            st.error(f"KeyError: {e}")

        all_video_ids.extend(videos_ids)
    return all_video_ids


#Function to fetch the video datas from the video_IDs
def fetch_video_data(all_video_ids):
    video_info=[]
    for each in all_video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=each

        )
        response = request.execute()
        for i in response["items"]:
            given= {
                       "Video_Id":i["id"] ,
                       "Video_title":i["snippet"]["title"],
                       "Video_Description":i["snippet"]["description"],
                       "channel_id":i['snippet']['channelId'],
                       "video_Tags": i['snippet'].get("Tags",0),
                       "Video_pubdate":i["snippet"]["publishedAt"],
                       "Video_viewcount":i["statistics"]["viewCount"],
                       "Video_likecount":i["statistics"].get('likeCount',0) ,
                       "Video_favoritecount":i["statistics"]["favoriteCount"],
                       "Video_commentcount":i["statistics"].get("Comment_Count",0),
                       "Video_duration":iso8601_duration_to_seconds(i["contentDetails"]["duration"]),
                       "Video_thumbnails":i["snippet"]["thumbnails"]['default']['url'],
                       "Video_caption":i["contentDetails"]["caption"]
            }

            video_info.append(given)


import sqlite3
import pandas as pd

def insert_video_info(video_info):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the 'videos' table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            Video_Id TEXT,
            Video_title TEXT,
            Video_Description TEXT,
            channel_id TEXT,
            video_Tags TEXT,
            Video_pubdate TEXT,
            Video_viewcount INTEGER,
            Video_likecount INTEGER,
            Video_favoritecount INTEGER,
            Video_commentcount INTEGER,
            Video_duration INTEGER,
            Video_thumbnails TEXT,
            Video_caption TEXT
        )
    ''')

    # Insert the video data into the table
    for video in video_info:
        cursor.execute("""
            INSERT INTO videos (Video_Id, Video_title, Video_Description, channel_id, video_Tags, Video_pubdate, Video_viewcount,
            Video_likecount, Video_favoritecount, Video_commentcount, Video_duration, Video_thumbnails, Video_caption)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (video['Video_Id'], video['Video_title'], video['Video_Description'], video['channel_id'], video['video_Tags'],
               video['Video_pubdate'], video['Video_viewcount'], video['Video_likecount'], video['Video_favoritecount'],
               video['Video_commentcount'], video['Video_duration'], video['Video_thumbnails'], video['Video_caption']))

    # Commit the changes to the database
    conn.commit()

    # Close the connection
    conn.close()

    return pd.DataFrame(video_info)

#Function to fetch the comments from video IDs
def Fetch_comment_data(newchannel_id):
    commentdata=[]
    allvideo_ids=playlist_videos_id([newchannel_id])
    for video in allvideo_ids:

            try:
                request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video,
                        maxResults=50)
                response=request.execute()
                for all in response["items"]:
                    given={
                                        "comment_id":all["snippet"]["topLevelComment"]["id"],
                                        "Comment_Text":all["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                        "Comment_Authorname":all["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        "published_date":all["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                                        "video_id":all["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                        'channel_id': all['snippet']['channelId']}

                    commentdata.append(given)
                nextpagetoken= response.get('nextPageToken')
            except HttpError as e:
                pass

import sqlite3
import pandas as pd

def insert_comment_data(commentdata):
    # Connect to the SQLite database
    conn = sqlite3.connect('db1.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the 'comments' table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT,
            Comment_Text TEXT,
            Comment_Authorname TEXT,
            published_date TEXT,
            video_id TEXT,
            channel_id TEXT
        )
    ''')

    # Insert the comment data into the table
    for comment in commentdata:
        cursor.execute("""
            INSERT INTO comments (comment_id, Comment_Text, Comment_Authorname, published_date, video_id, channel_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (comment['comment_id'], comment['Comment_Text'], comment['Comment_Authorname'], comment['published_date'],
               comment['video_id'], comment['channel_id']))

    # Commit the changes to the database
    conn.commit()

    # Close the connection
    conn.close()

    return pd.DataFrame(commentdata)

#function to convert the duration from hours to seconds
def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds





#Streamlit coding part to showcase the output in streamlit
import streamlit as st
def main():
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")

    Options = st.sidebar.radio("Options", ("Channels", "Videos", "Comments", "Queries", "Enter YouTube Channel ID"))

    if  Options == "Channels":
        st.header("Channels")
        channels_df = fetch_data("SELECT * FROM Channels;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif Options == "Videos":
        st.header("Videos")
        videos_df = fetch_data("SELECT * FROM Videos;")
        videos_df.index += 1
        st.dataframe(videos_df)

    elif Options == "Comments":
        st.header("Comments")
        comments_df = fetch_data("SELECT * FROM Comments;")
        comments_df.index += 1
        st.dataframe(comments_df)

    elif Options == "Queries":
        st.header("Queries")
        query_question = st.selectbox("Select Query", [
            "What are the names of all the videos and their corresponding channels?",
            "Which channels have the most number of videos, and how many videos do they have?",
            "What are the top 10 most viewed videos and their respective channels?",
            "How many comments were made on each video, and what are their corresponding video names?",
            "Which videos have the highest number of likes, and what are their corresponding channel names?",
            "What is the total number of likes for each video, and what are their corresponding video names?",
            "What is the total number of views for each channel, and what are their corresponding channel names?",
            "What are the names of all the channels that have published videos in the year 2022?",
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "Which videos have the highest number of comments, and what are their corresponding channel names?"])

        if query_question:
            query_result_df = execute_query(query_question)
            query_result_df.index += 1
            st.dataframe(query_result_df)
    elif Options == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")
        if st.button("Fetch Channel Data"):
            channel_df = fetch_channel_data(channel_id)
            channel_df.index +=1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch Video Data"):
            all_video_ids = playlist_videos_id([channel_id])
            video_df = fetch_video_data(all_video_ids)
            video_df.index +=1
            st.subheader("Video Data")
            st.write(video_df)

        if st.button("Fetch Comment Data"):
            comment_df = Fetch_comment_data([channel_id])
            comment_df.index +=1
            st.subheader("Comment Data")
            st.write(comment_df)


if __name__ == "__main__":
    main()
