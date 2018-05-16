####################################################################################
 # python process.py <number of workers> <file> <lines per batch> <subreddits>*
####################################################################################
import json
import pprint
from math import log
from multiprocessing import Pool, current_process
from multiprocessing.pool import ThreadPool
import os
import pickle
import sys
import urllib.request
import urllib.parse
import urllib.error
import praw
import time

# CONSTANTS
ID = 'id'
BODY = 'body'
SCORE = 'score'
PARENT = 'parent_id'
LINK = 'link_id'
SUBREDDIT = 'subreddit'

TRAIN_DATA = ""

# LIST OF SUBREDDITS WE CARE ABOUT
QUERY_SUBREDDITS = []

TEMP_DIR = "temp"
OUTPUT_DIR = "output"

# Number of pools to start
NUM_WORKERS = 0
NUM_LINES = 0

reddit = praw.Reddit(client_id='TT8HtZxXLYvEvA', client_secret="xUPNH9fNS-Z_I9yWcLXeTuXCWWU", user_agent='Rehydrater v1')

class RedditThread():
    def __init__(self, link):
        self.body = ""
        self.netScore = 0
        self.link = link
        self.numberOfComments = 0
        self.post = {}

    def update(self, body, score):
        self.netScore += score
        self.body = self.body + " " + body
        self.numberOfComments += 1

    def concat(self, reddit_thread):
        self.netScore += reddit_thread.netScore
        self.body = self.body + " " + reddit_thread.body
        self.numberOfComments += reddit_thread.numberOfComments

    def addOriginalPost(self, postBody, postScore):
        self.post = { 'body': postBody, 'score': postScore }
        self.body += postBody
        self.netScore += postScore

def concatSubredditDicts(d1, d2):
    for link in d2.keys():
        if link in d1:
            d1[link].concat(d2[link])
        else:
            d1[link] = d2[link]

def getPostsById(keys):
    if len(keys) > 0:
        # get in batches of < 100
        CHUNK_SIZE = 99
        start = 0
        end = CHUNK_SIZE
        total = []
        count = 0
        print(str(len(keys)) + 'posts to be rehydrated')
        while end < len(keys):
            print('Fetching chunk ' + str(count))
            count += 1
            path = "by_id/" + ",".join(keys[start:end]) + ".json"
            try:
                contents = reddit.request('GET', path)
                total.extend(contents['data']['children'])

                start += CHUNK_SIZE
                end += CHUNK_SIZE
                time.sleep(1)

            except urllib.error.HTTPError as e:
                error_message = e.read()
                print(error_message)
                break

        print('Fetching chunk ' + str(count))
        count += 1
        path = "by_id/" + ",".join(keys[start:]) + ".json"

        try:
            contents = reddit.request('GET', path)
            total.extend(contents['data']['children'])
        except urllib.error.HTTPError as e:
            error_message = e.read()
            print(error_message)

        return total
    return []

def rehydrate(subreddit_dict, subreddit_obj):
    body = subreddit_obj['data']['selftext']
    score = subreddit_obj['data']['score']
    link_id = subreddit_obj['data']['name']
    subreddit_dict[link_id].addOriginalPost(body, score)

def process_wrapper(file):
    subredditThreads = {}
    for s in QUERY_SUBREDDITS:
        subredditThreads[s] = {}
    with open(TEMP_DIR + "/" + file) as f:
        for line in f:
            process(subredditThreads, line)
    for subreddit in QUERY_SUBREDDITS:
        if len(subredditThreads[subreddit]) > 0:
            with open(OUTPUT_DIR + "/" + subreddit + "_" + file + ".pkl", 'wb') as pickle_file:
                pickle.dump(subredditThreads[subreddit], pickle_file)

def process(subredditThreads, line):
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

def init_directory(dir_name):
    directory = './' + dir_name
    try:
        os.makedirs(directory)
    except OSError:
        if not os.path.isdir(directory):
            raise

def main(args):
    global NUM_LINES, NUM_WORKERS, TRAIN_DATA, QUERY_SUBREDDITS

    NUM_WORKERS = int(args[1])
    TRAIN_DATA = args[2]
    NUM_LINES = int(args[3])
    QUERY_SUBREDDITS = args[4:]

    print("Looking at subreddits: " + str(QUERY_SUBREDDITS))

    pool = Pool(NUM_WORKERS)
    jobs = []

    init_directory(TEMP_DIR)
    init_directory(OUTPUT_DIR)
    print("Splitting data into batches of " + str(NUM_LINES))
    os.system("split -l " + str(NUM_LINES) + " " + TRAIN_DATA + " " + TEMP_DIR + "/" + TRAIN_DATA.replace(".txt", ""))
    print("Sending data to workers to parse")
    for file in os.listdir("./" + TEMP_DIR):
        jobs.append( pool.apply_async(process_wrapper, args=(file, )) )

    for index, job in enumerate(jobs):
        job.get()
        print(str((index + 1) * NUM_LINES) + " parsed out of approx. " + str(len(jobs) * NUM_LINES))

    pool.close()

    threadPool = ThreadPool(NUM_WORKERS)

    print("Accumulating pickle files into <subreddit>.pkl")
    for subreddit in QUERY_SUBREDDITS:
        subreddit_dict = {}
        for file in os.listdir("./" + OUTPUT_DIR):
            if file.startswith(subreddit):
                with open(OUTPUT_DIR + "/" + file, 'rb') as subreddit_pickle:
                    concatSubredditDicts(subreddit_dict, pickle.load(subreddit_pickle))
        print("Rehydrating the original posts for subreddit " + subreddit)

        j = getPostsById(list(subreddit_dict.keys()))
        rehydrateJobs = []
        for obj in j:
            rehydrateJobs.append(threadPool.apply_async(rehydrate, (subreddit_dict, obj, ) ))

        for job in rehydrateJobs:
            job.get()

        with open(OUTPUT_DIR + "/" + "master_" + subreddit + ".pkl", 'wb') as sub_pkl:
            pickle.dump(subreddit_dict, sub_pkl)

        os.system("rm -rf " + TEMP_DIR)




if __name__ == '__main__':
    if (len(sys.argv) < 5):
        raise Exception('Not Enough Arguments')
    main(sys.argv)
