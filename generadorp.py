import os
import json
import bz2
import sys
import time
import networkx as nx
from datetime import datetime
from collections import defaultdict
import argparse
from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


def procesar_tweets(archivo_bz2, fecha_inicial=None, fecha_final=None, hashtags=None):
    tweets = []

    with bz2.BZ2File(archivo_bz2, 'rb') as f_in:
        for line in f_in:
            tweet_data = json.loads(line.decode('utf-8'))

            if 'created_at' in tweet_data:
                created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                # Verifica si el tweet está dentro del rango de fechas
                if (fecha_inicial is None or created_at >= fecha_inicial) and (fecha_final is None or created_at <= fecha_final):
                    # Verifica si el tweet contiene al menos uno de los hashtags especificados
                    if hashtags is None or tiene_hashtags(tweet_data, hashtags):
                        tweets.append(tweet_data)

    return tweets

def tiene_hashtags(tweet, hashtags):
    tweet_hashtags = set(hashtag['text'].lower() for hashtag in tweet['entities']['hashtags'])
    return bool(hashtags.intersection(tweet_hashtags))

def procesar_directorio(directorio, fecha_inicial=None, fecha_final=None, archivo_hashtags=None):
    num_tweets_comprimidos = 0
    tweets = []

    hashtags = None
    if archivo_hashtags is not None:
        with open(archivo_hashtags, 'r') as file:
            hashtags = set(line.strip().lower() for line in file)

    for i, (root, _, files) in enumerate(os.walk(directorio)):
        if i % size == rank:
            for archivo in files:
                if archivo.endswith('.json.bz2'):
                    archivo_bz2 = os.path.join(root, archivo)
                    num_tweets_comprimidos += 1
                    tweets += procesar_tweets(archivo_bz2, fecha_inicial, fecha_final, hashtags)

    return tweets, num_tweets_comprimidos

def json_retweets(tweets):
    retweet_dict = {}

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']

            if user_original not in retweet_dict:
                retweet_dict[user_original] = {'receivedRetweets': 1, 'tweets': {tweet['retweeted_status']['id_str']: {'retweetedBy': [user_retweeter]}}}
            else:
                retweet_dict[user_original]['receivedRetweets'] += 1
                tweet_id = tweet['retweeted_status']['id_str']
                if tweet_id not in retweet_dict[user_original]['tweets']:
                    retweet_dict[user_original]['tweets'][tweet_id] = {'retweetedBy': [user_retweeter]}
                else:
                    retweet_dict[user_original]['tweets'][tweet_id]['retweetedBy'].append(user_retweeter)

    # Ordenar el JSON por número total de retweets al usuario de mayor a menor
    sorted_retweet_list = sorted(retweet_dict.items(), key=lambda item: item[1]['receivedRetweets'], reverse=True)

    result_json = {'retweets': []}
    for user, data in sorted_retweet_list:
        result_json['retweets'].append({'username': user, 'receivedRetweets': data['receivedRetweets'], 'tweets': data['tweets']})
    
    with open('rtp.json', 'w') as json_file:
            json.dump(result_json, json_file, indent=4)

    #print("JSON de retweets generado")


def json_menciones(tweets):
    mention_dict = {}

    for tweet in tweets:
        # Verificar si es un retweet
        if 'retweeted_status' in tweet:
            tweet = tweet['retweeted_status']  # Utilizar el tweet original en caso de retweet

        user_mentions = tweet['entities']['user_mentions']
        if user_mentions:
            user_source = tweet['user']['screen_name']
            for mention in user_mentions:
                user_target = mention['screen_name']
                if user_target not in mention_dict:
                    mention_dict[user_target] = {'receivedMentions': 1, 'mentions': [{'mentionBy': user_source, 'tweets': [tweet['id_str']]}]}
                else:
                    mention_dict[user_target]['receivedMentions'] += 1
                    tweet_id = tweet['id_str']
                    found = False
                    for mention_data in mention_dict[user_target]['mentions']:
                        if mention_data['mentionBy'] == user_source:
                            mention_data['tweets'].append(tweet_id)
                            found = True
                            break
                    if not found:
                        mention_dict[user_target]['mentions'].append({'mentionBy': user_source, 'tweets': [tweet_id]})

    # Ordenar el JSON por número total de menciones al usuario de mayor a menor
    sorted_mention_list = sorted(mention_dict.items(), key=lambda item: item[1]['receivedMentions'], reverse=True)

    result_json = {'mentions': []}
    for user, data in sorted_mention_list:
        result_json['mentions'].append({'username': user, 'receivedMentions': data['receivedMentions'], 'mentions': data['mentions']})

    with open('mencionp.json', 'w') as json_file:
            json.dump(result_json, json_file, indent=4)
    #print("JSON menciones generado")


def json_corretweets(tweets):
    corrtweets_json = {"coretweets": []}
    authors_retweeters = defaultdict(set)

    # Recopilar información sobre quién retuiteó a cada autor
    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']
            authors_retweeters[user_original].add(user_retweeter)

    # Generar corrtweets a partir de la información recopilada
    authors = list(authors_retweeters.keys())
    for i, author1 in enumerate(authors):
        for author2 in authors[i + 1:]:
            common_retweeters = authors_retweeters[author1].intersection(authors_retweeters[author2])
            if common_retweeters:
                coretweet_data = {
                    "authors": {"u1": author1, "u2": author2},
                    "totalCoretweets": len(common_retweeters),
                    "retweeters": list(common_retweeters)
                }
                corrtweets_json["coretweets"].append(coretweet_data)

    corrtweets_json["coretweets"] = sorted(corrtweets_json["coretweets"], key=lambda x: x["totalCoretweets"], reverse=True)

    '''with open("corrtw.json", "w", encoding="utf-8") as json_file:
        json.dump(corrtweets_json, json_file, ensure_ascii=False, indent=2)'''

    with open('corrtwp.json', 'w') as json_file:
            json.dump(corrtweets_json, json_file, indent=4)
    #print("JSON corretweets generado")

def generar_grafo_retweets(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']

            G.add_node(user_original)
            G.add_node(user_retweeter)
            G.add_edge(user_retweeter, user_original)

    return G

def generar_grafo_menciones(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        # Verificar si es un retweet
        if 'retweeted_status' in tweet:
            tweet = tweet['retweeted_status']  # Utilizar el tweet original en caso de retweet

        user_mentions = tweet['entities']['user_mentions']
        if user_mentions:
            user_source = tweet['user']['screen_name']
            for mention in user_mentions:
                user_target = mention['screen_name']
                G.add_node(user_source)
                G.add_node(user_target)
                G.add_edge(user_source, user_target)

    return G

def generar_grafo_corretweets(tweets):
    G = nx.Graph()

    authors_retweeters = defaultdict(set)

    # Recopilar información sobre quién retuiteó a cada autor
    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']
            authors_retweeters[user_original].add(user_retweeter)

    # Generar corrtweets a partir de la información recopilada
    authors = list(authors_retweeters.keys())
    for i, author1 in enumerate(authors):
        for author2 in authors[i + 1:]:
            common_retweeters = authors_retweeters[author1].intersection(authors_retweeters[author2])
            if common_retweeters:
                G.add_node(author1)
                G.add_node(author2)
                G.add_edge(author1, author2, weight=len(common_retweeters))

    return G

def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Procesador de tweets', add_help = False)
    parser.add_argument('-d', '--dir', type=str, default='data', help='Directorio de entrada')
    parser.add_argument('-fi', '--fecha_inicial', type=lambda s: datetime.strptime(s, '%d-%m-%y'), help='Fecha inicial (dd-mm-aa) para filtrar tweets')
    parser.add_argument('-ff', '--fecha_final', type=lambda s: datetime.strptime(s, '%d-%m-%y'), help='Fecha final (dd-mm-aa) para filtrar tweets')
    parser.add_argument('-h', '--archivo_hashtags', type=str, help='Archivo de texto con hashtags para filtrar tweets')
    parser.add_argument('-grt', '--grafo_retweets', action='store_true', help='Generar grafo de retweets (rt.gexf)')
    parser.add_argument('-gm', '--grafo_menciones', action='store_true', help='Generar grafo de menciones (mencion.gexf)')
    parser.add_argument('-gcrt', '--grafo_corretweets', action='store_true', help='Generar grafo de corretweets (corrtw.gexf)')
    parser.add_argument('-jrt', '--json_retweets', action='store_true', help='Generar JSON de retweets (rt.json)')
    parser.add_argument('-jm', '--json_menciones', action='store_true', help='Generar JSON de menciones (menciones.json)')
    parser.add_argument('-jcrt', '--json_corretweets', action='store_true', help='Generar JSON de co-retweets (corrtw.json)')
    args = parser.parse_args()

    
    directorio_completo = os.path.abspath(args.dir)
    #tweets, num_tweets_comprimidos = procesar_directorio(directorio_completo, args.fecha_inicial, args.fecha_final, args.archivo_hashtags)

    tweets_local, num_tweets_local = procesar_directorio(directorio_completo, args.fecha_inicial, args.fecha_final, args.archivo_hashtags)
    tweets_globales = comm.gather(tweets_local, root=0)
    num_tweets_globales = comm.gather(num_tweets_local, root=0)

    if rank == 0:
        tweets = [tweet for sublist in tweets_globales for tweet in sublist]
        num_tweets_comprimidos = sum(num_tweets_globales)

    if rank == 0:
        if args.json_retweets:
            json_retweets(tweets)
    
        if args.json_menciones:
            json_menciones(tweets)

        if args.json_corretweets:
            json_corretweets(tweets)

        if args.grafo_retweets:
            grafo_retweets = generar_grafo_retweets(tweets)
            nx.write_gexf(grafo_retweets, 'rtp.gexf')
    
        if args.grafo_menciones:
            grafo_menciones = generar_grafo_menciones(tweets)
            nx.write_gexf(grafo_menciones, 'mencionp.gexf')
    
        if args.grafo_corretweets:
            grafo_corretweets = generar_grafo_corretweets(tweets)
            nx.write_gexf(grafo_corretweets, 'corrtwp.gexf')

    print("Tiempo de ejecución total:", time.time() - start_time, "segundos")
if __name__ == "__main__":
    main()