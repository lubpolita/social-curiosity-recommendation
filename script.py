import glob
import time
import pandas as pd

SOCIAL_TIES_PATH = 'social_ties/LFM-1b_social_ties.txt'
USERS_PATH = 'runs/'
DESTINATION_PATH = 'files/'

def relations_bt_users(user1, users):
    same_track = []

    for user2 in users:
        df_user1 = pd.merge(user1[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user2[['track_id']], on='track_id', how='inner')
        df_user2 = pd.merge(user2[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user1[['track_id']], on='track_id', how='inner')

        same_track.append(df_user1)
        same_track.append(df_user2)

        df_user1 = pd.merge(user1[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user2[['artist_id']], on='artist_id', how='inner')
        df_user2 = pd.merge(user2[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user1[['artist_id']], on='artist_id', how='inner')

        same_track.append(df_user1)
        same_track.append(df_user2)

        df_user1 = pd.merge(user1[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user2[['album_id']], on='album_id', how='inner')
        df_user2 = pd.merge(user2[['user_id', 'artist_id', 'album_id', 'track_id', 'timestamp']], user1[['album_id']], on='album_id', how='inner')

        same_track.append(df_user1)
        same_track.append(df_user2)

    return pd.concat(same_track).drop_duplicates().sort_values(by='timestamp')

def read_users_file(users):
    users_files = []

    for user in users: 
        file = read_user_file(user)
        users_files.append(file)
    
    return users_files

def find_user_relations(user_id, social_ties):
    relations = [] 

    for tie in social_ties:
      if (tie[0] == user_id):
        relations.append(tie[1])

    return relations

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

def get_ids(matrix): 
    ids = []
    for line in matrix:
        ids.append(line[0])
    return set(ids)

def read_social_ties(filename):
    matrix = []

    with open(filename, 'r') as arquivo:
        lines = arquivo.readlines()

        for line in lines:
            if(not line):
               continue

            colunas = line.strip().split("\t")

            try: 
                user_ids = [colunas[0], colunas[1]]
            except Exception as e: 
                colunas = line.strip().split(" ")
                user_ids = [colunas[0], colunas[1]]
            matrix.append(user_ids)

    return matrix

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
    
    tracks = relations_bt_users(user_info, relations_info)

    if(tracks.empty):
        return
    
    filename = DESTINATION_PATH + user_id + '.txt'

    tracks.to_csv(filename, sep='\t', index=False)
    

def merge_all():
    # Grava o tempo inicial
    inicio = time.time()
    
    social_ties = read_social_ties(SOCIAL_TIES_PATH)
    ids = get_ids(social_ties)

    for id in ids:
        create_user_file(id, social_ties)

    fim = time.time()

    # Calcula o tempo total decorrido
    tempo_total = fim - inicio

    print(f"Tempo de execução: {tempo_total} segundos")

merge_all()