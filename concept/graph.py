
# graph.py

import networkx as nx
import random
import numpy
from collections import deque
# import tweepy as twpy

# CONSUMER_KEY = 'raDhcojjcOIjpoGkpwdbJ8aj7'
# CONSUMER_SECRET = 'mhNUiGOCU7z7VfdggMWq4gusnIakcr0xG06PWNoYJjeGwxL3vd'

# ACCESS_TOKEN = '975769632702435329-Xny7aBp75bJjbWK2LQ76CygeEXyPPLo'
# ACCESS_TOKEN_SECRET = 'SeCcyqMeiHCcuRo4xQi62ckMNqZMmT2OEpFixkxKVjY9s'

# auth = twpy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
# auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# api = twpy.api(auth)

NODES = 'nodes'
EDGES = 'edges'

def gen_friend_graph(n, thres=0.5):
    nodes = set(range(n))
    edges = { i:[] for i in nodes }
    for i in nodes:
        for j in nodes:
            if (i != j) and \
                (j not in edges[i] or i not in edges[j]) and \
                (random.random() >= thres):
                    edges[i].append(j)

    return { NODES: nodes, EDGES: edges }

# graph model:
# nodes = { key=user: val=retweet content sentiment }
# edges = { key=user: val=list(retweeters) }
def gen_retweet_graph(friend_graph, start=-1, thres=0.5, bias=0):
    # if no specified starter, choose the person with most followers
    if start == -1:
        start = max(friend_graph[NODES], key=lambda x: len(friend_graph[EDGES][x]))
    nodes = { start: random.random() } # map usr ID to sentiment
    edges = dict()
    queue = deque()
    seen = set()
    queue.append(start)

    while len(queue) > 0:
        user = queue.popleft()
        # sentiment 0..1 where 0 is neg, 1 is pos
        sentiment = min((random.random() + bias), 1)
        nodes[user] = sentiment
        followers = friend_graph[EDGES][user]
        seen.add(user)
        for follower in followers:
            will_retweet = random.random() >= thres
            if will_retweet and (follower not in nodes.keys()) and (follower not in seen):
                if user not in edges:
                    edges[user] = [follower]
                else:
                    edges[user].append(follower)
                queue.append(follower)
                seen.add(follower)

    return { NODES: nodes, EDGES: edges }

# MAIN

G = gen_friend_graph(10)
print(G[EDGES])
print(gen_retweet_graph(G))
