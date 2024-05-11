import glob
import time
import numpy as np
import pandas as pd
from multiprocessing import Process, freeze_support

SOCIAL_TIES_PATH = 'social_ties/LFM-1b_social_ties.txt'
USERS_PATH = 'runs/'
DESTINATION_PATH = 'files_multi/'

def read_users_file(users):
    users_files = []

    for user in users: 
        file = read_user_file(user)
        users_files.append(file)
    
    return users_files

def find_user_relations(user_id, social_ties):
    relations = []

    relations.extend(social_ties.loc[social_ties['user1'] == user_id, 'user2'])

    return relations

def load_all_user_files(path): 
    dfs = []
    files = glob.glob(path + '*.txt')

    for file in files:
        df = pd.read_csv(file, sep='\t')
        df.columns = ['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']
        dfs.append(df)

    merged_files = pd.concat(dfs)
    merged_files.columns = ['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']

    return merged_files.sort_values(by='user_id')

def read_user_file(filename):
    user = []
    files = glob.glob(filename)
    for line in files:
        df = pd.read_csv(line, sep='\t')
        df.columns = ['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']
        user.append(df)
    
    if not user:
        return pd.DataFrame()
    
    return pd.concat(user)

def get_unique_ids(social_ties):
    all_ids = pd.concat([social_ties['user1'], social_ties['user2']])
    all_ids = all_ids.rename('id')
    unique_ids = all_ids.drop_duplicates()
    return unique_ids

def read_social_ties(filename):
    data = pd.read_csv(filename, sep='\t', header=None, skiprows=1, dtype=str, engine='python')
    data.columns = ['user1', 'user2']
    return data

def create_user_file(user_id, social_ties):
    relations = find_user_relations(user_id, social_ties)
    relations_info = [] 
    
    user_info = read_user_file(USERS_PATH + user_id + '.txt')

    if(user_info.empty):
       return
    
    for relation in relations:
        info = read_user_file(USERS_PATH + relation + '.txt')
        
        if info.empty:
            continue
        
        relations_info.append(info)

    if len(relations_info) == 0:
        return
    
    relations_info.append(user_info)

    combined_df = pd.concat(relations_info, ignore_index=True)

    sorted_df = combined_df.sort_values(by='timestamp')

    filename = DESTINATION_PATH + user_id + '.txt'

    sorted_df.to_csv(filename, sep='\t', index=False)

def divide_ids(ids):
    len_shape = len(ids) // 4 # 4 Threads
    shapes = []
    start = 0

    for i in range(4):
        if i == 3:
            shapes.append(ids[start:])
        else:
            shapes.append(ids[start:start+len_shape])
            start += len_shape

    return shapes

def divide(ids):
    N_PROC = 4
    SIZE = ids.size
    lenght = np.int64(np.ceil(SIZE / N_PROC))
    shares = [i * lenght for i in range(N_PROC)]
    index_slices = []
    for i in range(N_PROC):
        if i < N_PROC-1:
            arr=ids[shares[i]:shares[i+1]]
            if arr.size != 0:
                index_slices.append(arr)
        else:
            arr=ids[shares[i]:]
            if arr.size != 0:
                index_slices.append(arr)

def merge_shape(social_ties, ids):
    for id in ids:
        create_user_file(id, social_ties)

def multiprocessing(social_ties, index_slices):
    block = []

    for slices in index_slices:
        freeze_support()
        blk = Process(target=merge_shape, args=(social_ties, slices,))
        blk.start()
        print('%d,%s' % (blk.pid, " starting..."))
        block.append(blk)

    for blk in block:
        print('%d,%s' % (blk.pid, " waiting..."))
        blk.join()

def merge_all():
    # Grava o tempo inicial
    inicio = time.time()
    
    social_ties = read_social_ties(SOCIAL_TIES_PATH)

    ids = get_unique_ids(social_ties)

    shapes = divide_ids(ids)

    multiprocessing(social_ties, shapes)
     
    fim = time.time()

    # Calcula o tempo total decorrido
    tempo_total = fim - inicio

    print(f"Tempo de execução: {tempo_total} segundos")

if __name__ == '__main__':
    merge_all()