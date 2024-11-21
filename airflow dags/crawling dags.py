# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# PostgreSQL
import psycopg2
from psycopg2 import errors
# Date and time
from datetime import datetime, timedelta, timezone  
import time
import pytz
from collections import deque
# Requesting API
import requests
import logging

import pandas as pd
import numpy as np
import os

connection_params = {
    'host': '<your_host>',          # Replace <your_host> with the database host (e.g., localhost or IP address)
    'dbname': '<your_dbname>',      # Replace <your_dbname> with the name of your database
    'user': '<your_user>',          # Replace <your_user> with your database username
    'password': '<your_password>',  # Replace <your_password> with your database password
    'port': 5432                    # Adjust the port if necessary (default PostgreSQL port is 5432)
}

client_credentials = [
    {'client_id': '<your_client_id_1>', 'client_secret': '<your_client_secret_1>'},  # First set of credentials
    {'client_id': '<your_client_id_2>', 'client_secret': '<your_client_secret_2>'},  # Second set of credentials
    {'client_id': '<your_client_id_3>', 'client_secret': '<your_client_secret_3>'},  # Third set of credentials
    # Add more credentials here as needed
]

current_credential_index = 0
request_timestamps = deque()
hochiminh_tz = pytz.timezone("Asia/Ho_Chi_Minh")

# Function to get access token using a given client ID and secret
def request_access_token(client_id, client_secret):
    import base64  
    
    auth_string = f'{client_id}:{client_secret}'
    auth_encoded = base64.b64encode(auth_string.encode()).decode()

    response = requests.post(
        'https://accounts.spotify.com/api/token',
        headers={
            'Authorization': f'Basic {auth_encoded}',
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        data={'grant_type': 'client_credentials'}
    )

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        return None
    
def refresh_access_token():
  global current_credential_index

  creds = client_credentials[current_credential_index]
  access_token = request_access_token(creds['client_id'], creds['client_secret'])

  if access_token:
    return access_token
  else:
    return None

def switch_client_credentials():
  global current_credential_index
  current_credential_index = (current_credential_index + 1) % len(client_credentials)
  logging.info('Switched to client ID: %s', client_credentials[current_credential_index]['client_id'])

def check_rate_limit(time_window=30, rate_limit=60):
    global request_timestamps

    # Current time
    now = time.time()

    # Remove timestamps older than the time_window
    while request_timestamps and request_timestamps[0] < now - time_window:
        request_timestamps.popleft()  # Remove the old timestamp

    # If the rate_limit was reached, sleep until the window resets
    if len(request_timestamps) >= rate_limit:
        wait_time = time_window - (now - request_timestamps[0])
        if wait_time > 0:  # Only sleep if there is actual wait time
            logging.info(f'\n\t\tRATE LIMIT\nRate limit reached. Waiting for {wait_time:.3f} seconds.')
            time.sleep(wait_time)
            logging.info('Sleep completed\n')
        # Clear any timestamps older than the time_window after sleeping
        while request_timestamps and request_timestamps[0] < now - time_window:
            request_timestamps.popleft()

    # Log the new request timestamp
    request_timestamps.append(now)

# Task 1: fetch playlists from the official top 50 for all Southeast Asian countries
def get_playlist_from_top50_country(country, market, headers):
    # Construct the URL for searching playlists
    url = 'https://api.spotify.com/v1/search'
    params = {
        'q': f'Top 50 - {country}',
        'type': 'playlist',
        'market': market,
        'limit': 1
    }
    retry_attempts = 3
    
    for attempt in range(retry_attempts):
        try:
            check_rate_limit()
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                result = response.json()
                playlist_info = result.get('playlists', {})
                playlist = playlist_info.get('items', [])

                if not playlist:
                    logging.info('No more playlists for query: %s', country)
                    continue

                playlist = playlist[0]  # Access the first playlist
                if playlist['owner']['id'] == 'spotify' and \
                   playlist['description'] == f'Your daily update of the most played tracks right now - {country}.':
                    # Create a dictionary with playlist information
                    playlist_details = {
                        'playlist_id': playlist.get('id', 'Unknown'),
                        'playlist_name': playlist.get('name', 'Unknown')
                    }
                    time.sleep(0.5)  # Sleep after processing the playlist
                    return playlist_details

            elif response.status_code == 401:
                logging.warning('Access token expired. Requesting a new one...\n')
                time.sleep(60)
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token. Exiting...\n')
                    break

            elif response.status_code == 429:
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Rate limit exceeded. Switching credentials...')
                time.sleep(60)
                switch_client_credentials()
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token after switching credentials. Exiting...\n')
                    break

            else:
                logging.error('Error searching %s: %s, %s\n', 
                              country, response.status_code, response.text)
                break

        except requests.exceptions.RequestException as e:
            logging.error('Network error occurred: %s', e)
            continue

    logging.error('Failed to fetch tracks after multiple attempts.')
    return None
    
def save_playlists_to_postgres(playlists_data):
    insert_query = """
    INSERT INTO playlists (
        country, 
        playlist_id, 
        playlist_name
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (playlist_id) DO NOTHING;
    """
    
    try:
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        
        for playlist in playlists_data:
            cursor.execute(insert_query, (playlist['country'], playlist['playlist_id'], playlist['playlist_name']))
        
        # Commit changes once after all insertions
        connection.commit()

    except errors.UniqueViolation:
        logging.warning("A duplicate entry was found. No action taken.")
    except Exception as e:
        logging.error('An unexpected error occurred: %s', e)
        return False
    finally:
        # Ensure resources are closed
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return True
    
def fetch_playlists(**kwargs):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        access_token = refresh_access_token()
        
        if access_token:
            break  # Exit the loop if we successfully get an access token
        else:
            logging.error(f'Failed to obtain access token on attempt {attempt + 1}/{retry_attempts}.')
            
            if attempt < retry_attempts - 1:
                time.sleep(60)
                switch_client_credentials()
    else:
        logging.error('Exhausted all attempts to obtain access token. Exiting...')
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    sea_countries_list = {
        'Indonesia': 'ID', 'Malaysia': 'MY', 'Philippines': 'PH',
        'Singapore': 'SG', 'Thailand': 'TH', 'Vietnam': 'VN'
    }
    all_playlists = []
    
    for country_name, country_code in sea_countries_list.items():
        now = datetime.now(hochiminh_tz)
        readable_time = now.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f'Search playlist with name: {country_name}\nStart time: {readable_time}')
        playlist = get_playlist_from_top50_country(country_name, country_code, headers)

        if playlist:  # Check if playlists_list is not None
            playlist['country'] = country_name
            all_playlists.append(playlist)
            logging.info(f'Playlist found: {playlist}\n')
        else:
            logging.info(f'No playlists found for: {country_name}\n')
    
    if all_playlists:
        playlists_df = pd.DataFrame(all_playlists)
        playlists_data = playlists_df.to_dict(orient='records')
        
        file_path = './data/playlists.csv'
        playlists_df.to_csv(file_path, mode='w', index=False, header=True)       
        
        kwargs['ti'].xcom_push(key='playlists_df', value=playlists_data)
        logging.info('Playlists dataframe saved successfully in XCom.')
        
        save_playlists_to_postgres(playlists_data)
        return True
    else:
        logging.info('No playlists to save.')
        return False

# Task 2: fetch tracks from specific playlists
def get_tracks_from_playlist(playlist_id, headers):
    tracks_list = []
    # Construct the URL for searching tracks
    url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
    current_track_position = 1
    retry_attempts = 3

    for attempt in range(retry_attempts):
        try:
            check_rate_limit()
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                results = response.json()  # Decode the response as JSON
                playlist_info = results
                items = playlist_info.get('tracks', {}).get('items', [])

                # Collect track information
                for item in items:
                    track = item.get('track', {})

                    if not track:
                        logging.warning('Missing track data in playlist: %s', playlist_id)
                        continue

                    # Safely access the 'artists' field within the 'track'
                    artists = track.get('artists', [])
                    if not artists:
                        logging.warning('No artists found for track: %s', track.get('id', 'Unknown'))

                    # Extract artist IDs, or provide a fallback in case of missing data
                    artists_id = [artist['id'] for artist in artists if artist.get('id') is not None]
                    artists_id_string = ', '.join(artists_id) if artists_id else 'Unknown'

                    # Create the track info dictionary
                    track_info = {
                        'artists_id': artists_id_string,                              # List of artist IDs
                        'album_id': track.get('album', {}).get('id', 'Unknown'),      # Album ID
                        'track_id': track.get('id', 'Unknown'),                       # Track ID
                        'track_uri': track.get('uri', 'Unknown'),                     # Track URI
                        'track_name': track.get('name', 'Unknown'),                   # Track name
                        'track_release_date': track.get('album', {})
                                                .get('release_date', 'Unknown'),   # Release date
                        'track_date_added': item.get('added_at', 'Unknown'),          # Track's date added in the playlist
                        'track_duration_ms': track.get('duration_ms', 0),             # Duration in milliseconds
                        'track_popularity': track.get('popularity', 0),               # Popularity (0-100)
                        'track_position': current_track_position,                                # The position of track in playlist
                        'is_explicit': track.get('explicit', False)                   # Whether the track is explicit
                    }

                    current_track_position += 1  # Increment the track count for each track
                    # Add the track info to the tracks list
                    tracks_list.append(track_info)

                return tracks_list

            elif response.status_code == 401:  # Access token expired
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Access token expired. Refreshing...')
                time.sleep(60)
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token. Exiting...\n')
                    break

            elif response.status_code == 429:  # Too many requests
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Rate limit exceeded. Switching credentials...')
                time.sleep(60)
                switch_client_credentials()
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token after switching credentials. Exiting...\n')
                    break

            else:  # Handle other errors
                logging.error('Error searching tracks for playlist ID %s: %s, %s\n', 
                              playlist_id, response.status_code, response.text)
                break
            
        except requests.exceptions.RequestException as e:
            logging.error('Network error occurred: %s', e)
            continue

    logging.error('Failed to fetch tracks after multiple attempts.')
    return None

def save_tracks_to_postgres(tracks_data):
    insert_query = """
    INSERT INTO tracks (
        artists_id,         album_id, 
        track_id,           track_uri, 
        track_name,         track_release_date, 
        track_date_added,   track_duration_ms, 
        track_popularity,   track_position, 
        is_explicit,        country, 
        date
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (track_id, country, date) DO NOTHING;
    """
    connection = None
    cursor = None
    
    try:
        insert_values = [
            (track['artists_id'],       track['album_id'], 
             track['track_id'],         track['track_uri'], 
             track['track_name'],       track['track_release_date'], 
             track['track_date_added'], track['track_duration_ms'], 
             track['track_popularity'], track['track_position'], 
             track['is_explicit'],      track['country'], 
             track['date'],)
            for track in tracks_data
        ]
        
        # Get the database connection
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        
        # Execute the batch insert
        cursor.executemany(insert_query, insert_values)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        logging.error('An error occurred: %s', e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
    return True

def fetch_tracks(**kwargs):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        access_token = refresh_access_token()
        
        if access_token:
            break  # Exit the loop if we successfully get an access token
        else:
            logging.error(f'Failed to obtain access token on attempt {attempt + 1}/{retry_attempts}.')
            
            if attempt < retry_attempts - 1:
                time.sleep(60)
                switch_client_credentials()
    else:
        logging.error('Exhausted all attempts to obtain access token. Exiting...')
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    all_tracks = []
    playlist_count = 1
    
    playlists_df = pd.DataFrame(kwargs['ti'].xcom_pull(key='playlists_df', task_ids='fetch_playlists'))
    
    for index, playlist in playlists_df.iterrows():
        now = datetime.now(hochiminh_tz)
        readable_time = now.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Search playlist number: {playlist_count}, playlist name: {playlist['playlist_name']}\nStart time: {readable_time}")
        tracks = get_tracks_from_playlist(playlist['playlist_id'], headers)
        
        if tracks:
            logging.info(f'Number of tracks found: {len(tracks)}\n')
            tracks = [{**track, 'country': playlist['country']} for track in tracks]
            
            for track in tracks:
                all_tracks.append(track)
        else:
            logging.info(f"No tracks found for: {playlist['playlist_id']}\n")
            
        playlist_count += 1
    
    if all_tracks:
        tracks_df = pd.DataFrame(all_tracks)
        tracks_df['track_release_date'] = pd.to_datetime(tracks_df['track_release_date'], errors='coerce')
        default_date = pd.to_datetime('2001-01-01')
        tracks_df['track_release_date'] = tracks_df['track_release_date'].fillna(default_date)
        tracks_df['track_release_date'] = tracks_df['track_release_date'].dt.strftime('%Y-%m-%d')
        
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        tracks_df['date'] = date_str
        tracks_data = tracks_df.to_dict(orient='records')
        
        file_path = './data/tracks.csv'
    
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            tracks_df.to_csv(file_path, mode='a', index=False, header=False)
        else:
            tracks_df.to_csv(file_path, mode='w', index=False, header=True)
        
        kwargs['ti'].xcom_push(key='tracks_df', value=tracks_data)
        logging.info('Tracks dataframe saved successfully.')
        
        save_tracks_to_postgres(tracks_data)
        return True
    else:
        logging.info('No tracks to save.')
        return False

# Task 3: fetch artist information from specific artists id
def get_artists_from_batch_artists_id(batch_artists_id, headers):
    url = 'https://api.spotify.com/v1/artists'
    params = {'ids': ','.join(batch_artists_id)}  # Join IDs into a comma-separated string
    retry_attempts = 3

    for attempt in range(retry_attempts):
        try:
            check_rate_limit()
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                artists = response.json().get('artists', [])
                artists_list = []

                for artist in artists:
                    if artist:
                        artist_info = {
                            'artist_id': artist.get('id', 'Unknown'),
                            'artist_uri': artist.get('uri', 'Unknown'),
                            'artist_name': artist.get('name', 'Unknown'),
                            'artist_genres': artist.get('genres', []),
                            'artist_popularity': artist.get('popularity', 0),
                            'artist_follower': artist.get('followers', {}).get('total', 0),
                            'artist_image_url': artist['images'][0]['url'] if artist['images'] else ''
                        }
                        
                        artists_list.append(artist_info)

                return artists_list

            elif response.status_code == 401:  # Access token expired
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Access token expired. Refreshing...')
                time.sleep(60)
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token. Exiting...\n')
                    break

            elif response.status_code == 429:  # Too many requests
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Rate limit exceeded. Switching credentials...')
                time.sleep(60)
                switch_client_credentials()
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token after switching credentials. Exiting...\n')
                    break

            else:
                logging.error('Error fetching artist: %s, %s\n',
                                response.status_code, response.text)
                break

        except requests.exceptions.RequestException as e:
            logging.error('Network error occurred: %s', e)
            continue

    logging.error('Failed to fetch artists after multiple attempts.')
    return None

def save_artists_to_postgres(artists_data):
    insert_query = """
    INSERT INTO artists (
        artist_id,         artist_uri, 
        artist_name,       artist_genres,  
        artist_popularity, artist_follower,
        artist_image_url
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
    """
    connection = None
    cursor = None

    try:
        insert_values = [
            (artist['artist_id'],           artist['artist_uri'], 
             artist['artist_name'],         artist['artist_genres'],  
             artist['artist_popularity'],   artist['artist_follower'],
             artist['artist_image_url'])
            for artist in artists_data
        ]
        
        # Get the database connection
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        
        # Execute the batch insert
        cursor.executemany(insert_query, insert_values)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        logging.error('An error occurred: %s', e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    
    return True

def fetch_artists(**kwargs):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        access_token = refresh_access_token()
        
        if access_token:
            break  # Exit the loop if we successfully get an access token
        else:
            logging.error(f'Failed to obtain access token on attempt {attempt + 1}/{retry_attempts}.')
            
            if attempt < retry_attempts - 1:
                time.sleep(60)
                switch_client_credentials()
    else:
        logging.error('Exhausted all attempts to obtain access token. Exiting...')
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    all_artists = []
    batch_size = 50
    
    tracks_df = pd.DataFrame(kwargs['ti'].xcom_pull(key='tracks_df', task_ids='fetch_tracks'))
    tracks_df_exploded = tracks_df.copy()
    
    tracks_df_exploded['artist_id'] = tracks_df_exploded['artists_id'].str.split(', ')
    tracks_df_exploded = tracks_df_exploded.explode('artist_id')
    tracks_df_exploded = tracks_df_exploded.reset_index(drop=True)
    tracks_df_exploded['artist_id'] = tracks_df_exploded['artist_id'].str.strip()
    
    unique_artist_ids = tracks_df_exploded['artist_id'].unique()
    
    for batch_start in range(0, len(unique_artist_ids), batch_size):
        batch_artists_id = unique_artist_ids[batch_start : batch_start + batch_size]
        now = datetime.now(hochiminh_tz)
        readable_time = now.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Processing batch starting at index {batch_start}...\nStart time: {readable_time}")

        batch_artists = get_artists_from_batch_artists_id(batch_artists_id, headers)

        if batch_artists:
            logging.info(f'Total number of unique artists processed in batch: {len(batch_artists)}')
            
            for artist in batch_artists:
                all_artists.append(artist)
        else:
            logging.info(f'No artists found for batch starting at index {batch_start}\n')
    
    if all_artists:
        artists_df = pd.DataFrame(all_artists)
        artists_data = artists_df.to_dict(orient='records')
        
        file_path = './data/artists.csv'
    
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            existing_artists_df = pd.read_csv(file_path)
            artists_df = pd.concat([existing_artists_df, artists_df], ignore_index=True)
            
        artists_df.drop_duplicates(subset='artist_id', inplace=True)
        artists_df.sort_values(by='artist_id', ascending=True, inplace=True)
        artists_df.to_csv(file_path, mode='w', index=False, header=True)
        
        kwargs['ti'].xcom_push(key='artists_df', value=artists_data)
        logging.info('Artists dataframe saved successfully.')
        
        save_artists_to_postgres(artists_data)
        return True
    else:
        logging.info('No artists to save.')
        return False

# Task 4: fetch tracks audio features from specific tracks
def get_tracks_audio_feature_from_batch(batch_tracks_id, headers):
    url = 'https://api.spotify.com/v1/audio-features'
    params = {'ids': ','.join(batch_tracks_id)}  # Join IDs into a comma-separated string
    retry_attempts = 3

    for attempt in range(retry_attempts):
        try:
            check_rate_limit()
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 200:
                audio_features = response.json().get('audio_features', [])
                audio_features_list = []

                for feature in audio_features:
                    if feature:  # Ensure the feature object isn't None
                        audio_feature_info = {
                            'track_id': feature.get('id', 'Unknown'),
                            'danceability': feature.get('danceability', 0.0),
                            'energy': feature.get('energy', 0.0),
                            'key': feature.get('key', 'Unknown'),
                            'loudness': feature.get('loudness', 0.0),
                            'mode': feature.get('mode', 0),
                            'speechiness': feature.get('speechiness', 0.0),
                            'acousticness': feature.get('acousticness', 0.0),
                            'instrumentalness': feature.get('instrumentalness', 0.0),
                            'liveness': feature.get('liveness', 0.0),
                            'valence': feature.get('valence', 0.0),
                            'tempo': feature.get('tempo', 0.0),
                            'duration_ms': feature.get('duration_ms', 0),
                            'time_signature': feature.get('time_signature', 4)
                        }
                        audio_features_list.append(audio_feature_info)

                return audio_features_list

            elif response.status_code == 401:  # Access token expired
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Access token expired. Refreshing...')
                time.sleep(60)
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token. Exiting...\n')
                    break

            elif response.status_code == 429:  # Too many requests
                logging.warning(f'Attempt {attempt + 1}/{retry_attempts}: Rate limit exceeded. Switching credentials...')
                time.sleep(60)
                switch_client_credentials()
                access_token = refresh_access_token()
                if access_token:
                    headers['Authorization'] = f'Bearer {access_token}'
                    continue
                else:
                    logging.error('Failed to refresh access token after switching credentials. Exiting...\n')
                    break

            else:
                logging.error('Error fetching audio features: %s, %s\n',
                              response.status_code, response.text)
                break

        except requests.exceptions.RequestException as e:
            logging.error('Network error occurred: %s', e)
            continue
        
    logging.error('Failed to fetch audio features after multiple attempts.')
    return None

def save_tracks_audio_feature_to_postgres(tracks_audio_feature_data):
    insert_query = """
    INSERT INTO tracks_audio_feature (
        artists_id,         album_id, 
        track_id,           track_uri, 
        track_name,         track_release_date, 
        track_date_added,   track_duration_ms, 
        track_popularity,   track_position, 
        is_explicit,        country, 
        date,               danceability, 
        energy,             key, 
        loudness,           mode, 
        speechiness,        acousticness, 
        instrumentalness,   liveness, 
        valence,            tempo, 
        duration_ms,        time_signature
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (track_id, country, date) DO NOTHING;
    """
    connection = None
    cursor = None
    
    try:
        insert_values = [
            (track['artists_id'],       track['album_id'], 
             track['track_id'],         track['track_uri'], 
             track['track_name'],       track['track_release_date'], 
             track['track_date_added'], track['track_duration_ms'], 
             track['track_popularity'], track['track_position'], 
             track['is_explicit'],      track['country'], 
             track['date'],        track['danceability'],
             track['energy'],           track['key'], 
             track['loudness'],         track['mode'], 
             track['speechiness'],      track['acousticness'], 
             track['instrumentalness'], track['liveness'], 
             track['valence'],          track['tempo'], 
             track['duration_ms'],      track['time_signature'])
            for track in tracks_audio_feature_data
        ]
        
        # Get the database connection
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        
        # Execute the batch insert
        cursor.executemany(insert_query, insert_values)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        logging.error('An error occurred: %s', e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
    return True

def fetch_tracks_audio_feature(**kwargs):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        access_token = refresh_access_token()
        
        if access_token:
            break  # Exit the loop if we successfully get an access token
        else:
            logging.error(f'Failed to obtain access token on attempt {attempt + 1}/{retry_attempts}.')
            
            if attempt < retry_attempts - 1:
                time.sleep(60)
                switch_client_credentials()
    else:
        logging.error('Exhausted all attempts to obtain access token. Exiting...')
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    all_tracks_audio_features = []
    batch_size = 100
    
    tracks_df = pd.DataFrame(kwargs['ti'].xcom_pull(key='tracks_df', task_ids='fetch_tracks'))
    unique_tracks_id = tracks_df['track_id'].unique()
    
    for batch_start in range(0, len(unique_tracks_id), batch_size):
        batch_tracks_id = unique_tracks_id[batch_start : batch_start + batch_size]
        now = datetime.now(hochiminh_tz)
        readable_time = now.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Processing batch starting at index {batch_start}...\nStart time: {readable_time}")

        batch_audio_features = get_tracks_audio_feature_from_batch(batch_tracks_id, headers)

        if batch_audio_features:
            logging.info(f'Total number of unique audio features processed in batch {len(batch_audio_features)}')
            
            for track_audio_feature in batch_audio_features:
                all_tracks_audio_features.append(track_audio_feature)
        else:
            logging.info(f'No audio features found for batch starting at index {batch_start}\n')
    
    if all_tracks_audio_features:
        tracks_audio_features_df = pd.DataFrame(all_tracks_audio_features)
        tracks_audio_feature_df = pd.merge(tracks_df, tracks_audio_features_df, on='track_id', how='left')
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        tracks_audio_feature_df['date'] = date_str
        tracks_audio_feature_data = tracks_audio_feature_df.to_dict(orient='records')
        
        file_path = './data/tracks_audio_feature.csv'
    
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            tracks_audio_feature_df.to_csv(file_path, mode='a', index=False, header=False)
        else:
            tracks_audio_feature_df.to_csv(file_path, mode='w', index=False, header=True)
        
        kwargs['ti'].xcom_push(key='full_tracks_df', value=tracks_audio_feature_data)
        logging.info('Audio features tracks dataframe saved successfully.')
        
        save_tracks_audio_feature_to_postgres(tracks_audio_feature_data)
        return True
    else:
        logging.info('No tracks to save.')
        return False

# Task 5: fetch tracks stream counts in the previous day from specific tracks
def get_stream_track_count(track_id, start_date, end_date):
    url = f'https://www.mystreamcount.com/api/track/{track_id}/streams'
    retry_attempts = 3
    interval = 30

    for attempt in range(retry_attempts):
        try:
            check_rate_limit(60, 20)
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                json_response = response.json()

                if json_response.get('error') or (json_response.get('status') and json_response['status'] == 'processing'):
                    logging.warning(f"Attempt {attempt + 1}/{retry_attempts}: Status is 'processing' for track_id: {track_id}. Retrying after waiting.")
                    time.sleep(interval)
                    interval *= 2
                    continue
                else:
                    dates = json_response.get('data', {})

                    if not dates:
                        logging.warning("No stream data available.")
                        return []

                    stream_list = []
                    specific_date = start_date

                    while specific_date <= end_date:
                        date_str = specific_date.strftime('%Y-%m-%d')
                        stream_info = dates.get(date_str)

                        if stream_info:
                            stream_entry = {
                                'track_id': track_id,
                                'date': date_str,
                                'stream_daily': stream_info.get('daily') if not pd.isna(stream_info.get('daily')) else 0,
                                'stream_total': stream_info.get('total') if not pd.isna(stream_info.get('total')) else 0
                            }
                            
                            stream_list.append(stream_entry)
                            
                        specific_date += timedelta(days=1)

                    return stream_list

            else:
                logging.error('Error fetching stream data: %s, %s', 
                              response.status_code, response.text)

        except requests.exceptions.RequestException as e:
            logging.error('Request failed: %s', e)

    logging.error('Failed to fetch stream data after multiple attempts.')
    return None

def save_stream_to_postgres(tracks_stream_data):
    insert_query = """
    INSERT INTO tracks_stream (
        artists_id,     album_id,
        track_id,       track_uri,
        track_name,     date,
        stream_daily,   stream_total
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (track_id, date) DO NOTHING;
    """
    connection = None
    cursor = None

    try:
        insert_values = [
            (stream['artists_id'],      stream['album_id'], 
             stream['track_id'],        stream['track_uri'],  
             stream['track_name'],      stream['date'],
             stream['stream_daily'],    stream['stream_total'])
            for stream in tracks_stream_data
        ]
        
        # Get the database connection
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        
        # Execute the batch insert
        cursor.executemany(insert_query, insert_values)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        logging.warning(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
    return True

def fetch_tracks_stream():
    in_file_path = './data/tracks.csv'
    tracks_df = pd.read_csv(in_file_path)
    
    tracks_df['date'] = pd.to_datetime(tracks_df['date'])
    tracks_df['track_release_date'] = pd.to_datetime(tracks_df['track_release_date'])
    
    specific_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    tracks_specific_date_df = tracks_df[tracks_df['date'].dt.date == specific_date].copy()
    
    tracks = tracks_specific_date_df[['track_id', 'track_release_date']].drop_duplicates()
    tracks = tracks.sort_values(by='track_id', ascending=True)
    
    all_tracks_stream = []
    track_count = 1

    for index, track in tracks.iterrows():
        now = datetime.now(hochiminh_tz)
        readable_time = now.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Search stream track number: {track_count}, track_id: {track['track_id']} \nStart time: {readable_time}")

        release_date = track['track_release_date'].date()
        limit_date = specific_date - timedelta(days=7)
        nearer_date = min(release_date, limit_date, key=lambda d: abs(specific_date - d))

        if nearer_date <= specific_date:
            start_date = nearer_date
            end_date = nearer_date + timedelta(days=6)
            track_stream_count = get_stream_track_count(track['track_id'], start_date, end_date)

            if track_stream_count:
                logging.info(f'Number of stream date data collected: {len(track_stream_count)}')
                all_tracks_stream.extend(track_stream_count)
            else:
                logging.info(f'No stream date found')

        else:
            logging.info(f'No stream date found')

        track_count += 1

    if all_tracks_stream:
        tracks_stream_df = pd.DataFrame(all_tracks_stream)
        tracks_df_unique = tracks_df[['track_id', 'artists_id', 'album_id', 'track_uri', 'track_name']].drop_duplicates(subset='track_id')
        tracks_stream_df = pd.merge(
            tracks_stream_df, 
            tracks_df_unique, 
            how='left',
            on='track_id'
        )
        tracks_stream_data = tracks_stream_df.to_dict(orient='records')
        
        out_file_path = './data/tracks_stream.csv'
        
        if os.path.exists(out_file_path) and os.path.getsize(out_file_path) > 0:
            existing_tracks_stream_df = pd.read_csv(out_file_path)
            tracks_stream_df = pd.concat([existing_tracks_stream_df, tracks_stream_df], ignore_index=True)
            
        tracks_stream_df.drop_duplicates(subset=['track_id', 'date'], inplace=True)
        tracks_stream_df.sort_values(by=['track_id', 'date'], ascending=True, inplace=True)
        tracks_stream_df.to_csv(out_file_path, mode='w', index=False, header=True)
        
        logging.info('Tracks stream dataframe saved successfully.')
        
        save_stream_to_postgres(tracks_stream_data)
        return True
    else:
        logging.info('No stream to save.')
        return False

# Set up DAG
dag = DAG(
    dag_id='Spotify_Data_Collection_ETL_Pipeline',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='An ETL process for crawling Spotify data using the API',
    schedule_interval='30 13 * * *',
    start_date=datetime(2024, 11, 6),
    tags=['Spotify', 'ETL'],
)

# Task 1
fetch_playlists_task = PythonOperator(
    task_id='fetch_playlists',
    python_callable=fetch_playlists,
    dag=dag
)
# Task 2
fetch_tracks_task = PythonOperator(
    task_id='fetch_tracks',
    python_callable=fetch_tracks,
    dag=dag
)
# Task 3
fetch_artists_task = PythonOperator(
    task_id='fetch_artists',
    python_callable=fetch_artists,
    dag=dag
)
# Task 4
fetch_tracks_audio_feature_task = PythonOperator(
    task_id='fetch_tracks_audio_features',
    python_callable=fetch_tracks_audio_feature,
    dag=dag
)
# Task 5
fetch_tracks_stream_task = PythonOperator(
    task_id='fetch_tracks_stream',
    python_callable=fetch_tracks_stream,
    dag=dag
)

fetch_playlists_task >> fetch_tracks_task >> fetch_artists_task >> fetch_tracks_audio_feature_task >> fetch_tracks_stream_task