####################################################################################
 # python process.py <number of workers> <file> <lines per batch> <subreddits>*
####################################################################################
import json
import pprint
from math import log
from multiprocessing import Pool, current_process
import os
import pickle
import sys

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
subredditThreads = {}

TEMP_DIR = "temp"
OUTPUT_DIR = "output"

# Number of pools to start
NUM_WORKERS = 0
NUM_LINES = 0

class RedditThread():
    def __init__(self, link):
        self.body = ""
        self.netScore = 0
        self.link = link
        self.numberOfComments = 0

    def update(self, body, score):
        self.netScore += score
        self.body = self.body + " " + body
        self.numberOfComments += 1

    def concat(self, reddit_thread):
        self.netScore += reddit_thread.netScore
        self.body = self.body + " " + reddit_thread.body
        self.numberOfComments += reddit_thread.numberOfComments

def concatSubredditDicts(d1, d2):
    for link in d2.keys():
        if link in d1:
            d1[link].concat(d2[link])
        else:
            d1[link] = d2[link]

def process_wrapper(file):
    global subredditThreads
    subredditThreads = {}
    for s in QUERY_SUBREDDITS:
        subredditThreads[s] = {}
    with open(TEMP_DIR + "/" + file) as f:
        for line in f:
            process(line)
    for subreddit in subredditThreads.keys():
        if len(subredditThreads[subreddit]) > 0:
            with open(OUTPUT_DIR + "/" + subreddit + "_" + file + ".pkl", 'wb') as pickle_file:
                pickle.dump(subredditThreads[subreddit], pickle_file)

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

def init_directory(dir_name):
    directory = './' + dir_name
    try:
        os.makedirs(directory)
    except OSError:
        if not os.path.isdir(directory):
            raise

def main(args):
    global NUM_LINES, NUM_WORKERS, TRAIN_DATA, QUERY_SUBREDDITS, subredditThreads

    NUM_WORKERS = int(args[1])
    TRAIN_DATA = args[2]
    NUM_LINES = int(args[3])
    QUERY_SUBREDDITS = args[4:]

    for s in QUERY_SUBREDDITS:
        subredditThreads[s] = {}

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

    print("Accumulating pickle files into <subreddit>.pkl")
    for subreddit in QUERY_SUBREDDITS:
        subreddit_dict = {}
        for file in os.listdir("./" + OUTPUT_DIR):
            if file.startswith(subreddit):
                with open(OUTPUT_DIR + "/" + file, 'rb') as subreddit_pickle:
                    concatSubredditDicts(subreddit_dict, pickle.load(subreddit_pickle))
        with open(OUTPUT_DIR + "/" + subreddit + ".pkl", 'wb') as sub_pkl:
            pickle.dump(subreddit_dict, sub_pkl)

        os.system("rm -rf " + TEMP_DIR)

if __name__ == '__main__':
    if (len(sys.argv) < 5):
        raise Exception('Not Enough Arguments')
    main(sys.argv)
