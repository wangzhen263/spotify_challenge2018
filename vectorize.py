import pandas as pd
import fastText
import re
import multiprocessing
from threading import Thread
from threading import Lock
import queue
import time


def normalize_name(name):
    name = name.lower()
    name = re.sub(r"[.,\/#!$%\^\*;:{}=\_`~()@]", ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def process_data(q, results):
    while not exitFlag:
        if not workQueue.empty():
            data = q.get()
            feat = [ftSession.get_word_vector(normalize_name(data['data']['name']))]
            epgm_id = data['id']
            results.append({'epgm_id':epgm_id, 'feat':feat})
        else:
            time.sleep(1)

def fastTextize(data, q, csvName):
    if len(pThreads) == 0:
        print ('pThreads ', len(pThreads))
        for i in range(numThreads):
            worker = Thread(target=process_data, args=(workQueue, results_list))
            worker.setDaemon(True)
            pThreads.append(worker)
            worker.start()

    for index, row in data.iterrows():
        q.put(row)

    while q.qsize() > 0:
        print('sleep 5')
        time.sleep(5)

    df_results = pd.DataFrame(results_list)
    df_results.to_csv(csvName, index=False)
    print('df_results ', df_results.shape)
    results_list.clear()


exitFlag = 0
workQueue = queue.Queue()
numThreads = multiprocessing.cpu_count()

pThreads = []
results_list = []
vertices = pd.read_json('mpd.epgm/vertices.json', lines=True)
playlists = vertices.loc[vertices['meta'].apply(lambda r:r['label']=='playlist')]
tracks = vertices.loc[vertices['meta'].apply(lambda r:r['label']=='track')]
albums = vertices.loc[vertices['meta'].apply(lambda r:r['label']=='album')]
artists = vertices.loc[vertices['meta'].apply(lambda r:r['label']=='artist')]

print('Loading fastText model...')
ftSession = fastText.load_model('fastTextModel.bin')

print('Generating fastText vectors on playlists...')
fastTextize(playlists, workQueue, 'playlists_w2v.csv')

print('Generating fastText vectors on tracks...')
fastTextize(tracks, workQueue, 'tracks_w2v.csv')

print('Generating fastText vectors on albums...')
fastTextize(albums, workQueue, 'albums_w2v.csv')

print('Generating fastText vectors on artists...')
fastTextize(artists, workQueue, 'artists_w2v.csv')

exitFlag = 1