import csv
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

API_KEY = ''  # Put your API Key here

csv_lock = threading.Lock()  # Lock for writing to the CSV file safely

# Get the youtube client in each thread to avoid global state
def get_youtube_client():
    return build('youtube', 'v3', developerKey=API_KEY)

# Get the channel name and upload playlist ID
def get_channel_info(channel_id):
    youtube = get_youtube_client()
    response = youtube.channels().list(
        part='snippet,contentDetails',
        id=channel_id
    ).execute()
    if 'items' in response and len(response['items']) > 0:
        channel = response['items'][0]['snippet']['title']
        upload_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel, upload_playlist_id
    return None, None

# Fetch 100 video IDs from the uploads playlist
def get_video_ids_from_channel(upload_playlist_id, max_results=100):
    youtube = get_youtube_client()
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_results:
        response = youtube.playlistItems().list(
            part='snippet',
            playlistId=upload_playlist_id,
            maxResults=50,  # API allows max 50 results per request
            pageToken=next_page_token
        ).execute()

        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = response.get('nextPageToken')
        if not next_page_token or len(video_ids) >= max_results:
            break

    return video_ids[:max_results]

# Fetch the video title
def get_video_title(video_id):
    youtube = get_youtube_client()
    response = youtube.videos().list(
        part='snippet',
        id=video_id
    ).execute()
    if 'items' in response and len(response['items'])

# Fetch comments for a video
def get_comments(video_id, max_comments=100):
    comments = []
    next_page_token = None

    while len(comments) < max_comments:
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,  # Fetch up to 100 comments per request
            pageToken=next_page_token
        ).execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)

        next_page_token = response.get('nextPageToken')
        if not next_page_token or len(comments) >= max_comments:
            break

    return comments[:max_comments]


# Write the data to a CSV file
def write_to_csv(channel_name, video_title, comments, csv_file):
    with csv_lock:  # Ensure only one thread writes to the file at a time
        writer = csv.writer(csv_file)
        for comment in comments:
            writer.writerow([channel_name, video_title, comment])


# Fetch video details and comments concurrently
def fetch_video_data(video_id, channel_name, csv_file, max_comments_per_video):
    video_title = get_video_title(video_id)
    if video_title:
        print(f"Fetching comments for video: {video_title}")
        comments = get_comments(video_id, max_comments=max_comments_per_video)
        write_to_csv(channel_name, video_title, comments, csv_file)


# Main Function
def fetch_videos_and_comments(channel_id, max_videos=100, max_comments_per_video=100):
    # Get channel information
    channel_name, upload_playlist_id = get_channel_info(channel_id)

    if not channel_name or not upload_playlist_id:
        print("Invalid channel ID or channel information not found.")
        return

    print(f"Fetching data for channel: {channel_name}")

    # Get video IDs
    video_ids = get_video_ids_from_channel(upload_playlist_id, max_results=max_videos)

    # Open CSV file for writing
    with open(f'{channel_name}_video_comments.csv', mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Channel Name", "Video Title", "Comment"])

        # Use ThreadPoolExecutor for multithreaded video and comment fetching
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_video_data, video_id, channel_name, csv_file, max_comments_per_video)
                       for video_id in video_ids]

            # Wait for all threads to complete
            for future in as_completed(futures):
                try:
                    future.result()  # Ensure any exceptions are raised
                except Exception as e:
                    print(f"Error fetching video data: {e}")

    print(f"Data saved to {channel_name}_video_comments.csv")


# Replace with the actual YouTube channel ID
channel_id = input("Enter the YouTube Channel ID: ")
fetch_videos_and_comments(channel_id, max_videos=100, max_comments_per_video=100)