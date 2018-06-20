###### EPGM WRITER #######
import json
import uuid
import csv
import dataslice as ds

def write_graph(fn, graph_label):
    def make_graph(graph_id, graph_label):
        return {'id': graph_id, 'data': {}, 'meta': {'label': graph_label}}
    
    graph_id = uuid.uuid4().hex
    with open(fn, 'w', encoding='utf8') as graph_file:
        json.dump(make_graph(graph_id, graph_label), graph_file, ensure_ascii=False)
    
    return graph_id

def write_vertices(feat, fn, graph_id):
    def make_vertex(vertex_id, vertex_label, feats, graphs):
        return {'id': vertex_id, 'data': feats, 'meta': {'label': vertex_label, 'graphs': graphs}}
    def write_vertex(vertex_file, vertex_id, vertex_label, feats, graphs):
        json.dump(make_vertex(vertex_id, vertex_label, feats, graphs), vertex_file, ensure_ascii=False)
        vertex_file.write('\n')
    
    with open(fn, 'w', encoding='utf8') as vertex_file:
        for vertices, label in zip(feat, ['playlist', 'track', 'album', 'artist']):
            for vid, feats in vertices.items():
                write_vertex(vertex_file, vid, label, feats, [graph_id])


def write_edges(adj, fn, graph_id):
    def make_edge(src, dst, label, graphs):
        return {'id': uuid.uuid4().hex, 
                'source': src, 
                'target': dst, 
                'data': {}, 
                'meta': {'label': label, 'graphs': graphs}}
    def write_edge(src, dst, label, graphs, edge_file):
        json.dump(make_edge(src, dst, label, graphs), edge_file, ensure_ascii=False)
        edge_file.write('\n')
    
    with open(fn, 'w', encoding='utf8') as edge_file:
        for t, (pls, alb, art) in adj[1].items():
            # comment out things in here to omit edge types
            for pl in pls:
                write_edge(t, pl, 'track_to_playlist', [graph_id], edge_file)
                write_edge(pl, t, 'playlist_to_track', [graph_id], edge_file)
            write_edge(t, alb, 'track_to_album', [graph_id], edge_file)
            write_edge(alb, t, 'album_to_track', [graph_id], edge_file)
            write_edge(t, art, 'track_to_artist', [graph_id], edge_file)
            write_edge(art, t, 'artist_to_track', [graph_id], edge_file)

def write_uri2id(uri2id, fn):
    with open(fn, 'w', encoding='utf8') as uri2id_file:
        writer = csv.writer(uri2id_file)
        for key, value in uri2id.items():
            writer.writerow([key, value])




n_slices = 1
graph_label = 'spotify'
epgm_loc = 'mpd.epgm/'

adj_types_saved = ds.ADJ_TYPES
ADJ_TYPES = {'tracks'}  # only requires tracks adj to build epgm
adj, feat, uri2id = ds.parse_range(0, n_slices)
ADJ_TYPES = adj_types_saved
print("writing graph...")
gid = write_graph(epgm_loc + 'graphs.json', 'spotify')
print("writing vertices...")
write_vertices(feat, epgm_loc + 'vertices.json', gid)
print("writing edges...")
write_edges(adj, epgm_loc + 'edges.json', gid)
print("writing uri2id...")
write_uri2id(uri2id, epgm_loc + 'uri2id.csv')
print("done")