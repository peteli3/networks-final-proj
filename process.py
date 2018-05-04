import json
import pprint
from math import log
from threading import Lock
from multiprocessing import Pool, current_process
import os
import pickle

# CONSTANTS 
ID = 'id'
BODY = 'body'
SCORE = 'score'
PARENT = 'parent_id'
LINK = 'link_id'
SUBREDDIT = 'subreddit'

TRAIN_DATA = 'reddit_2006june.txt'
# TEST_DATA = 'reddit_2005dec.json'
# LIST OF SUBREDDITS WE CARE ABOUT
QUERY_SUBREDDITS = ['reddit.com', 'nsfw']

# Number of pools to start
cores = 2

# Dict of subreddit -> dict of link id to RedditThread
subredditThreads = {}
for s in QUERY_SUBREDDITS:
    subredditThreads[s] = {}

class RedditThread():
    def __init__(self, link):
        self.body = ""
        self.netScore = 0
        self.link = link
        self.numberOfComments = 0
        self.lock = Lock()

    def update(self, body, score):
        with self.lock:
            self.netScore += score
            self.body = self.body + " " + body
            self.numberOfComments += 1

def process_wrapper(lineByte):
    with open(TRAIN_DATA) as f:
        f.seek(lineByte)
        line = f.readline()
        process(line)

def process(line):
    try:
        post = json.loads(line)
        if post[SUBREDDIT] in QUERY_SUBREDDITS:
            linkIdToThreadMap = subredditThreads[post[SUBREDDIT]]
            if post[LINK] in linkIdToThreadMap:
                linkIdToThreadMap[post[LINK]].update(post[BODY], post[SCORE])
            else:
                linkIdToThreadMap[post[LINK]] = RedditThread(post[LINK])
                linkIdToThreadMap[post[LINK]].update(post[BODY], post[SCORE])
    except:
        pass

def main():
    pool = Pool(cores)
    jobs = []

    with open(TRAIN_DATA) as f:
        line = f.readline()
        while line:
            nextLineByte = f.tell()
            jobs.append( pool.apply_async(process_wrapper,args=(nextLineByte,)) )
            line = f.readline()
            nextLineByte = f.tell()

    l = len(jobs)
    for index, job in enumerate(jobs):
        if index % 1000 == 0:
            print('%s/%s lines processed.' % (index, l))
        job.get()

    pool.close()

    for k, v in subredditThreads.items():
        print('%s:%s' % (k, len(v)))

main()    
    