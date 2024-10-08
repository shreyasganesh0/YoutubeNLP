import asyncio
import aiohttp
import aiofiles
from aiocsv import AsyncWriter
import time
import nest_asyncio
from aiogoogle import Aiogoogle

nest_asyncio.apply()

API_KEY = ''

async def get_channel_info(youtube, channel_id):
    response = await youtube.channels().list(
        part='snippet,contentDetails',
        id=channel_id
    ).execute()
    if 'items' in response and len(response['items']) > 0:
        channel = response['items'][0]['snippet']['title']
        upload_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel, upload_playlist_id
    return None, None

async def get_video_ids(youtube, playlist_id, max_results=50):
    url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={playlist_id}&maxResults={max_results}&key={API_KEY}"
    async with youtube.get(url) as response:
        data = await response.json()
        return [item['snippet']['resourceId']['videoId'] for item in data.get('items', [])]

async def get_video_details(youtube, video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={API_KEY}"
    async with youtube.get(url) as response:
        data = await response.json()
        if 'items' in data and len(data['items']) > 0:
            item = data['items'][0]
            return {
                'title': item['snippet']['title'],
                'views': item['statistics']['viewCount'],
                'likes': item['statistics'].get('likeCount', '0'),
                'comments': item['statistics'].get('commentCount', '0')
            }
        return None

async def get_video_comments(youtube, video_id, max_results=100):
    url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&maxResults={max_results}&key={API_KEY}"
    async with youtube.get(url) as response:
        data = await response.json()
        return [
            {
                'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'likes': item['snippet']['topLevelComment']['snippet']['likeCount'],
                'replies': item['snippet']['totalReplyCount']
            }
            for item in data.get('items', [])
        ]

async def write_to_csv(filename, data):
    async with aiofiles.open(filename, mode='w', newline='', encoding='utf-8') as afp:
        writer = AsyncWriter(afp)
        await writer.writerow(['Video Title', 'Views', 'Likes', 'Comments', 'Author', 'Comment', 'Comment Likes', 'Replies'])
        for row in data:
            await writer.writerow(row)

async def process_video(youtube, video_id):
    video_details = await get_video_details(youtube, video_id)
    if video_details:
        comments = await get_video_comments(youtube, video_id)
        return [
            [
                video_details['title'],
                video_details['views'],
                video_details['likes'],
                video_details['comments'],
                comment['author'],
                comment['comment'],
                comment['likes'],
                comment['replies']
            ]
            for comment in comments
        ]
    return []

    

async def main():
    channel_id = "UCX6OQ3DkcsbYNE6H8uQQuVA"  # MrBeast channel ID
    async with Aiogoogle(api_key=API_KEY) as aiogoogle:
        youtube = await aiogoogle.discover('youtube', 'v3')
        channel_name, uploads_playlist_id = await get_channel_info(youtube, channel_id)
        if channel_name and uploads_playlist_id:
            print(f"Channel: {channel_name}")
            video_ids = await get_video_ids(youtube, uploads_playlist_id)
            
            # Create tasks for processing videos
            tasks = [asyncio.create_task(process_video(youtube, video_id)) for video_id in video_ids]
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)
            
            all_data = [item for sublist in results for item in sublist]
            
            await write_to_csv('mrbeast_comments_async_tasks.csv', all_data)
            print("Data has been written to mrbeast_comments_async_tasks.csv")
        else:
            print("Failed to retrieve channel information")

import asyncio

def run_main():
    asyncio.run(main())

# Call the function to execute the main method
if __name__ == "__main__":
    run_main()

print("Main method execution completed.")