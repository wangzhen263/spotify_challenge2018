import json
from collections import defaultdict
from typing import Dict, List
import re
import time

############## CONFIGURABLES... #######################

# Since playlist IDs go from 0 - 999,999, by default track IDs start from 1,000,000. Change this function if you want 
# to change this behaviour.
def id_curr(i, uri2id):
    return i + len(uri2id) + 1000000

# Change the path in this function to match the path of your dataset
def get_slice_filename(i: int):
    return "mpd.v1/data/mpd.slice.{}-{}.json".format(i*1000, i*1000+999)

# Change which adjacency list types to collect. 
# Decreasing the number of adjacency lists will improve speed and memory usage.
ADJ_TYPES = {
    'playlists', 
    'tracks', 
    'albums', 
    'artists'
}

# Change which playlist features to collect. Names of features correspond directly with those defined in mpd.v1 schema.
# Decreasing the number of features or setting to None will improve speed and memory usage.
PLAYLIST_FEAT_NAMES = (
    'name', 
    'collaborative', 
    'pid', 
    'modified_at', 
    'num_albums', 
    'num_tracks', 
    'num_followers', 
    'num_edits',
    'duration_ms', 
    'num_artists'
)

# Change which track features to collect. Names of features correspond directly with those defined in mpd.v1 schema.
# Decreasing the number of features or setting to None will improve speed and memory usage.
#
# NOTE: (album_name, album_uri) and (artist_name, artist_uri) need to be provided together since the names will 
# actually be used as album and artist features. (i.e. don't include artist_name without including artist_uri, and 
# vice versa)
TRACK_FEAT_NAMES = (
    'track_name', 
    'duration_ms', 
    'album_name', 
    'artist_name', 
    'album_uri', 
    'artist_uri'
)

############## SKIM-OVER/IGNORE - ABLES ###############

### Slice reader
def get_slice(i: int):
    """
    Read JSON slice data into dict.
    """
    with open(get_slice_filename(i), 'r') as f:
        data = json.load(f)
    return data


### adj helper functions
def match_uri(uri: str, uri_type: str) -> str:
    return re.match(r'spotify:' + uri_type + r':(.*)', uri)[1]

def track_uri(track: Dict[str, any]) -> str:
    return match_uri(track['track_uri'], 'track')

def album_uri(track: Dict[str, any]) -> str:
    return match_uri(track['album_uri'], 'album')

def artist_uri(track: Dict[str, any]) -> str:
    return match_uri(track['artist_uri'], 'artist')

def get_uris(playlists_slice):
    return {(p['pid'], track_uri(t), album_uri(t), artist_uri(t)) for p in playlists_slice for t in p['tracks']}

def update_tracks_adj(uris, tracks, albums, artists, uri2id):
    """
    Update tracks/albums/artists adjacency lists from slice
    """
    for pid, t, alb, art in uris:
        if t in uri2id:
            pids_t, _, _ = tracks[uri2id[t]]
            tracks[uri2id[t]] = (pids_t + [pid], alb, art)
        else:
            uri2id[t] = id_curr(0, uri2id)
            tracks[uri2id[t]] = ([pid], alb, art)
            if 'albums' in ADJ_TYPES:
                albums[alb] += [uri2id[t]]
            if 'artists' in ADJ_TYPES:
                artists[art] += [uri2id[t]]

def get_playlists_adj(playlists_slice, uri2id):
    """
    Get playlists data structure from slice
    """
    return {p['pid']: [uri2id[track_uri(t)] for t in p['tracks']] for p in playlists_slice}
    
def update_adj(playlists_slice, playlists, tracks, albums, artists, uri2id):
    """
    Update adjacency data structures with slice data
    """
    if 'tracks' in ADJ_TYPES:
        update_tracks_adj(get_uris(playlists_slice), tracks, albums, artists, uri2id)
    if 'playlists' in ADJ_TYPES:
        playlists.update(get_playlists_adj(playlists_slice, uri2id))


### feats helper functions
def get_track_feats(playlists_slice):
    return {track_uri(t): {k:v for k,v in t.items() if k in TRACK_FEAT_NAMES} 
            for p in playlists_slice for t in p['tracks']} if TRACK_FEAT_NAMES is not None else None

def get_playlist_feats(playlists_slice):
    return {p['pid']: {k:v for k,v in p.items() if k in PLAYLIST_FEAT_NAMES}
            for p in playlists_slice} if PLAYLIST_FEAT_NAMES is not None else None

def update_feats(playlists_slice, playlists, tracks, albums, artists, uri2id):
    """
    Update feature data structures with slice data
    """
    
    playlist_feats = get_playlist_feats(playlists_slice)
    if playlist_feats is not None:
        playlists.update(playlist_feats)
        
    track_feats = get_track_feats(playlists_slice)
    if track_feats is not None:
        for t, feats in track_feats.items():
            if not uri2id[t] in tracks:
                tracks[uri2id[t]] = {'name': feats['track_name'], 'duration_ms': feats['duration_ms']}
            if not feats['album_uri'] in albums:
                albums[feats['album_uri']] = {'name': feats['album_name']}
            if not feats['artist_uri'] in artists:
                artists[feats['artist_uri']] = {'name': feats['artist_name']}


############## PARSE FUNCTIONS - USE THESE! #################

def parse_slice(i, adj=None, feats=None, uri2id=None):
    """
    Parse a slice file into playlists, tracks, albums, and artists. Tracks are indexed by incremental IDs rather 
    than URI. Both adj and feat are organised as a tuple of (playlists, tracks, albums, artists)
    
    :return Updated tuple of (adj, feats, uri2id)
    """
    adj = adj if adj is not None else ({}, {}, defaultdict(lambda: []), defaultdict(lambda: []))
    feats = feats if feats is not None else ({}, {}, {}, {})
    uri2id = uri2id if uri2id is not None else {}
    
    data = get_slice(i)
    update_adj(data['playlists'], *adj, uri2id)
    update_feats(data['playlists'], *feats, uri2id)
    return (adj, feats, uri2id)
            
def parse_range(start, end):
    """
    Parse mutiple slice files in range. Tracks are indexed by incremental IDs rather than URI.
    
    :return adj, feats, uri2id
    """
    adj, feats, uri2id = None, None, None
    for i in range(start, end):
        adj, feats, uri2id = parse_slice(i, adj, feats, uri2id)
    return adj, feats, uri2id