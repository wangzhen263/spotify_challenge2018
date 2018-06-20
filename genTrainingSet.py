import pandas as pd
import re
import dataslice as ds

def normalize_name(name):
    name = name.lower()
    name = re.sub(r"[.,\/#!$%\^\*;:{}=\_`~()@]", ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

TRACK_FEAT_NAMES = (
    'track_name',
    'album_name',
    'artist_name',
)

def getTracksFeats(tracksDict, storage):
    for track in tracksDict:
        for k,v in track.items():
            storage.append(normalize_name(v)) if k in TRACK_FEAT_NAMES else None

def getPlaylistFeats(playlist, storage):
    for k,v in playlist.items():
        storage.append(normalize_name(v)) if k == 'name' else getTracksFeats(v, storage) if k == 'tracks' else None

n_slices = 1000

ffile = open('fastTextTrain.txt', 'a')
for i in range(0, n_slices):
    textsStorage = []
    sliceJson = ds.get_slice(i)
    for pj in sliceJson['playlists']:
        getPlaylistFeats(pj, textsStorage)
    ffile.write(' '.join(textsStorage) + ' ')

ffile.write('\n')
ffile.close() 