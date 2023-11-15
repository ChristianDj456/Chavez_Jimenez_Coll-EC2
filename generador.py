import os
import json
import bz2
import getopt
import sys
import time
import networkx as nx
from datetime import datetime
from collections import defaultdict

def procesar_tweets(archivo_bz2, fecha_inicial=None, fecha_final=None, hashtags_file=None):
    tweets = []  # Lista para almacenar los tweets procesados

    with bz2.BZ2File(archivo_bz2, 'rb') as f_in:
        for line in f_in:
            tweet_data = json.loads(line.decode('utf-8'))  # Lee el archivo JSON línea por línea

            # Verifica la presencia del campo 'created_at' en el tweet antes de intentar acceder a él
            if 'created_at' in tweet_data:
                created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                # Comprueba si la fecha del tweet está dentro del rango especificado
                if (fecha_inicial is None or created_at >= fecha_inicial) and (fecha_final is None or created_at <= fecha_final):
                    # Comprueba si el tweet contiene alguno de los hashtags especificados
                    if hashtags_file is None or tiene_hashtags(tweet_data, hashtags_file):
                        tweets.append(tweet_data)  # Agrega el tweet a la lista si cumple con las condiciones

    return tweets


def tiene_hashtags(tweet, hashtags_file):
    with open(hashtags_file, 'r') as file:
        hashtags = set(line.strip() for line in file)

    tweet_hashtags = set(hashtag['text'].lower() for hashtag in tweet['entities']['hashtags'])

    return bool(hashtags.intersection(tweet_hashtags))


def procesar_directorio(directorio, fecha_inicial=None, fecha_final=None, hashtags_file=None):
    num_tweets_comprimidos = 0  # Contador para el número de tweets comprimidos encontrados
    tweets = []  # Lista para almacenar todos los tweets procesados

    for root, _, files in os.walk(directorio):
       # print("Recorriendo directorio:", root)  # Imprime el directorio actual
        for archivo in files:
            if archivo.endswith('.json.bz2'):
                archivo_bz2 = os.path.join(root, archivo)
                num_tweets_comprimidos += 1  # Incrementa el contador de tweets comprimidos encontrados
                tweets.extend(procesar_tweets(archivo_bz2, fecha_inicial))  # Extiende la lista de tweets con los tweets del archivo

    return tweets, num_tweets_comprimidos


def generar_grafo_retweets(tweets):
    G = nx.Graph()

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']

            G.add_edge(user_retweeter, user_original)

    return G


def generar_json_retweets(tweets):
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

    return result_json


def generar_grafo_menciones(tweets):
    G = nx.Graph()

    for tweet in tweets:
        user_mentions = tweet['entities']['user_mentions']
        if user_mentions:
            user_source = tweet['user']['screen_name']
            for mention in user_mentions:
                user_target = mention['screen_name']
                G.add_edge(user_source, user_target)

    return G


def generar_json_menciones(tweets):
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

    return result_json



def generar_grafo_corretweets(tweets):
    G = nx.Graph()

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            user_original = tweet['retweeted_status']['user']['screen_name']
            user_retweeter = tweet['user']['screen_name']

            # Agregar la relación de corretweet al grafo
            G.add_edge(user_retweeter, user_original)

    return G


def generar_json_corretweets(tweets):
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

    with open("corrtw.json", "w", encoding="utf-8") as json_file:
        json.dump(corrtweets_json, json_file, ensure_ascii=False, indent=2)

    return corrtweets_json



def main(argv):
    start_time = time.time()

    directorio = 'data'  # Valor por defecto para el directorio
    fecha_inicial = None  # Valor por defecto para la fecha inicial
    fecha_final = None  # Valor por defecto para la fecha final
    hashtags_file = None  # Valor por defecto para el archivo de hashtags

    try:
        opts, _ = getopt.getopt(argv, "hd:fi:ff:h:", ["dir=", "fecha_inicial=", "fecha_final=", "hashtags="])
    except getopt.GetoptError:
        print("Uso: generador.py -d <path relativo> -fi <fecha inicial (dd-mm-aa)> -ff <fecha final (dd-mm-aa)> -h <nombre de archivo>")
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print("Uso: generador.py -d <path relativo> -fi <fecha inicial (dd-mm-aa)> -ff <fecha final (dd-mm-aa)> -h <nombre de archivo>")
            sys.exit()
        elif opt in ("-d", "--dir"):
            directorio = arg
        elif opt in ("-fi", "--fecha_inicial"):
            fecha_inicial = datetime.strptime(arg, '%d-%m-%y')
        elif opt in ("-ff", "--fecha_final"):
            fecha_final = datetime.strptime(arg, '%d-%m-%y')
        elif opt in ("-h", "--hashtags"):
            hashtags_file = arg

    directorio_completo = os.path.abspath(directorio)
    tweets, num_tweets_comprimidos = procesar_directorio(directorio_completo, fecha_inicial, fecha_final, hashtags_file)

    # Generar grafo de retweets y guardar en formato GEXF
    grafo_retweets = generar_grafo_retweets(tweets)
    nx.write_gexf(grafo_retweets, 'rt.gexf')

    # Generar JSON de retweets ordenado y guardar en formato JSON
    json_retweets = generar_json_retweets(tweets)
    with open('rt.json', 'w') as json_file:
        json.dump(json_retweets, json_file, indent=4)

    # Generar grafo de menciones y guardar en formato GEXF
    grafo_menciones = generar_grafo_menciones(tweets)
    nx.write_gexf(grafo_menciones, 'mencion.gexf')

    # Generar JSON de menciones ordenado y guardar en formato JSON
    json_menciones = generar_json_menciones(tweets)
    with open('mencion.json', 'w') as json_file:
        json.dump(json_menciones, json_file, indent=4)

    # Generar JSON de corretweets y guardar en formato JSON
    json_corretweets = generar_json_corretweets(tweets)
    with open('corrtw.json', 'w') as json_file:
        json.dump(json_corretweets, json_file, indent=4)

    # Generar grafo de corretweets y guardar en formato GEXF
    grafo_corretweets = generar_grafo_corretweets(tweets)
    nx.write_gexf(grafo_corretweets, 'corrtw.gexf')


    # Imprimir el tiempo de ejecución total en segundos
    print("Tiempo de ejecución total:", time.time() - start_time, "segundos")

if __name__ == "__main__":
    main(sys.argv[1:])
