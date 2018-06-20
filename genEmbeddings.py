import fastText
import re
import dataslice as ds
import pandas as pd
import itertools

def normalize_name(name):
    name = name.lower()
    name = re.sub(r"[.,\/#!$%\^\*;:{}=\_`~()@]", ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def getTracksFeats(tracksDict):
    for track in tracksDict:
        for k,v in track.items():
            track_names.append([normalize_name(v)]) if k == 'track_name' else None
            album_names.append([normalize_name(v)]) if k == 'album_name' else None
            artist_names.append([normalize_name(v)]) if k == 'artist_name' else None

def getPlaylistFeats(playlist):
    for k,v in playlist.items():
        playlist_names.append([normalize_name(v)]) if k == 'name' else getTracksFeats(v) if k == 'tracks' else None

def wordEmbeddings(nodes, fastTextSession):
    assert fastTextSession is not None
    try:
        output = []
        for n in nodes:
            output.append([fastTextSession.get_word_vector(' '.join(n))])
        return output
    except Exception as e:
        raise e

print('Loading fastText model...')
ftSession = fastText.load_model('fastTextModel.bin')

print('Collecting words from dataset...')
n_slices = 1

playlist_names = []
track_names = []
album_names = []
artist_names = []

for i in range(0, n_slices):
    sliceJson = ds.get_slice(i)
    for pj in sliceJson['playlists']:
        getPlaylistFeats(pj)

playlist_names.sort()
playlist_names = list(playlist_names for playlist_names,_ in itertools.groupby(playlist_names))

track_names.sort()
track_names = list(track_names for track_names,_ in itertools.groupby(track_names))

album_names.sort()
album_names = list(album_names for album_names,_ in itertools.groupby(album_names))

artist_names.sort()
artist_names = list(artist_names for artist_names,_ in itertools.groupby(artist_names))

print('Collected playlist words: ', len(playlist_names))
print('Collected track words: ', len(track_names))
print('Collected album words: ', len(album_names))
print('Collected artist words: ', len(artist_names))

with open('playlist_names.txt', 'w') as f:
    for p in playlist_names:
        f.write(' '.join(p) + '\n')
    
with open('track_names.txt', 'w') as f:
    for p in track_names:
        f.write(' '.join(p) + '\n')

with open('album_names.txt', 'w') as f:
    for p in album_names:
        f.write(' '.join(p) + '\n')

with open('artist_names.txt', 'w') as f:
    for p in artist_names:
        f.write(' '.join(p) + '\n')


print('Generating w2v - playlists...')
playlist_names_output = wordEmbeddings(playlist_names, ftSession)
df_playlists = pd.DataFrame(
    {'name': playlist_names,
     'feat': playlist_names_output
    })
print('dumping playlists w2v to csv...')
df_playlists.to_csv('playlists_w2v.csv')
del df_playlists
del playlist_names_output

print('Generating w2v - tracks...')
track_names_output = wordEmbeddings(track_names, ftSession)
df_tracks = pd.DataFrame(
    {'name': track_names,
     'feat': track_names_output
    })
print('dumping tracks w2v to csv...')
df_tracks.to_csv('tracks_w2v.csv')
del df_tracks
del track_names_output

print('Generating w2v - albums...')
album_names_output = wordEmbeddings(album_names, ftSession)
df_albums = pd.DataFrame(
    {'name': album_names,
     'feat': album_names_output
    })
print('dumping albums w2v to csv...')
df_albums.to_csv('albums_w2v.csv')
del df_albums
del album_names_output

print('Generating w2v - artists...')
artist_names_output = wordEmbeddings(artist_names, ftSession)
df_artists = pd.DataFrame(
    {'name': artist_names,
     'feat': artist_names_output
    })
print('dumping artists w2v to csv...')
df_artists.to_csv('artists_w2v.csv')
del df_artists
del artist_names_output