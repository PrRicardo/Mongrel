import os
import pymongo
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import tqdm


def fetch_playlists(client: spotipy.Spotify):
    playlists = client.current_user_playlists()
    return playlists


def fetch_songs_of_playlists(client: spotipy.Spotify, playlists: list[dict]):
    tracks = []
    for playlist in playlists:
        response = client.playlist_items(playlist_id=playlist['id'])
        tracks.extend(response['items'])
    return tracks


def throw_client_id_error(e: KeyError):
    print("Please add a CLIENT_ID to your environment variables. "
          "Create a client_id at https://developer.spotify.com/")
    raise e


def throw_secret_error(e: KeyError):
    print("Please add a CLIENT_SECRET to your environment variables. "
          "Lookup the CLIENT_SECRET at https://developer.spotify.com/")
    raise e


def aggregate_spotify_data():
    try:
        client_id = os.getenv("CLIENT_ID")
    except KeyError as e:
        throw_client_id_error(e)
    if client_id is None:
        throw_client_id_error(KeyError())
    try:
        client_secret = os.getenv("CLIENT_SECRET")
    except KeyError as e:
        throw_secret_error(e)
    if client_secret is None:
        throw_secret_error(KeyError())

    client = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret),
        auth_manager=SpotifyOAuth(client_id=client_id, client_secret=client_secret,
                                  scope="user-read-currently-playing,user-read-playback-state,playlist-read-private,"
                                        "playlist-read-collaborative,user-follow-read,user-top-read,"
                                        "user-read-recently-played,user-library-read,user-read-email,"
                                        "user-read-private",
                                  redirect_uri="localhost:8080"))
    playlists = fetch_playlists(client)
    tracks = fetch_songs_of_playlists(client, playlists['items'])
    return tracks


def insert_tracks(tracks: list):
    mongo_client = pymongo.MongoClient("localhost", 27017)
    db = mongo_client.hierarchical_relational_test
    for track in tqdm.tqdm(tracks):
        db.test_tracks.insert_one(track)


if __name__ == "__main__":
    tracks = aggregate_spotify_data()
    insert_tracks(tracks)
