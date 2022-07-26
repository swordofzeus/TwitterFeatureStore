import os
from numpy import extract
import pandas as pd
from dotenv import load_dotenv
import csv
import re
import warnings
import os
import logging
from utils.elastic_utils import bulk_index_csv_documents, bulk_index_json_documents,create_index
from config.elastic_mapping import tweets_mapping,conversations_mapping
from elasticsearch7 import Elasticsearch, helpers

class TweetIngest:
    def __init__(self,tweet_url,staging_path):
        self.tweet_url = tweet_url
        self._create_output_dir(staging_path)
        self.staging_path = staging_path

    def _create_output_dir(self,path="output"):
        try:
            os.mkdir(path) if(not os.path.exists(path)) else None
            os.mkdir(f'{path}"/tweets') if(not os.path.exists(f'{path}/tweets')) else None
            os.mkdir(f'{path}/conversations') if(not os.path.exists(f'{path}/conversations')) else None
        except OSError as e:
            logging.error("OS error while creating staging directory %s ",path)
            raise e
        except Exception as e:
            logging.error("Exception creating staging directory %s",e)
            raise e

    def extract(self):
        '''@return Pandas DF with tweets from tweet_url'''
        raw_data = None
        try:
            raw_data = pd.read_csv(self.tweet_url,parse_dates=['created_at'])
        except FileNotFoundError as fnfe:
            logging.error("File with name: %s not found",self.tweet_url)
            raise fnfe
        except IOError as ioe:
            logging.error("I/O issue reading file with name: %s",self.tweet_url)
            raise ioe
        return raw_data

    def clean(self,extract_df):
        '''@return pandas df with date parsed from timestamp, removed new line characters,
                    and generated hash value column from original message'''
        logging.info("Cleaning raw tweet data...")
        logging.info("Generating date from created_at timestamp...")
        '''create date from timestamp'''
        extract_df['created_at'] = extract_df['created_at'].dt.date
        logging.info("Removing @ handle from message text...")
        '''remove @ from begining of a string'''
        extract_df['text'] = extract_df['text'].str.replace(r'^@\w*', '').str.lower()
        '''replace new line characters'''
        logging.info("Removing newline chars from text....")
        extract_df['text'] = extract_df['text'].str.replace('\n','')
        '''generate hash for quick exact text match. TODO: replace with stronger SHA256/MD5/etc. with lower probability of collisions '''
        logging.info("Generating hash from message....")
        extract_df['text_hash'] = extract_df.apply(lambda x: hash(x['text']), axis=1)
        logging.info("Cleaning raw tweet data...OK.")
        return extract_df

    def clean_list(self,row):
        '''@arg: string vector csv of a conversation flow
           @return vector of lists in a conversation flow'''
        row = re.sub(' ','',row)
        row = row.split(",")
        row = list(filter(None, row))
        row = list(map(lambda x: int(x) ,row)  )
        return row

    def partition_conversation(self,df):
        df = df.drop(["latest_tweet_id","next_tweet_id"],axis=1)
        df['conversation_flow'] = df['conversation_flow'].apply(self.clean_list)
        df['conversation_length'] = df['conversation_flow'].apply(lambda x: len(x))
        return df


    def transform(self,extract_df):
        extract_df = self.clean(extract_df)
        staging_tweet_filepath ="{}/tweets/{}.csv".format(self.staging_path,"cleaned_tweets")
        self._load_to_staging(extract_df,staging_tweet_filepath)
        tweet_lineage = extract_df[['tweet_id','response_tweet_id','in_response_to_tweet_id']]


        tweet_lineage['response_tweet_id'] = tweet_lineage['response_tweet_id'].astype(str)
        tweet_lineage['tweet_id'] = tweet_lineage['tweet_id'].astype(str)
        initial = tweet_lineage[tweet_lineage['in_response_to_tweet_id'].isnull()]
        tweet_lineage = tweet_lineage[~tweet_lineage['tweet_id'].isin(initial['tweet_id'])]

        initial['response_tweet_id'] = initial['response_tweet_id'].apply(lambda x: x.split(","))
        initial = initial.explode("response_tweet_id")

        initial = initial.merge(tweet_lineage, left_on='response_tweet_id', right_on='tweet_id')
        initial['conversation_flow'] = initial['tweet_id_x'] + "," + initial['response_tweet_id_x']
        initial = initial[['tweet_id_x','tweet_id_y','conversation_flow','response_tweet_id_y']]
        initial = initial.rename(columns={'tweet_id_x':'first_tweet_id',
                                  'tweet_id_y': 'latest_tweet_id',
                                  'response_tweet_id_y': 'next_tweet_id'})
        prev_cnt = len(tweet_lineage)
        first_run = True
        conversation_length = 1

        '''merge tweet ids that belong in a conversation together into a conversation array
            using a linked list type traversal joining current tweet -> response tweets
            and aggregating into a list'''
        while(first_run or prev_cnt != len(tweet_lineage)):
            first_run = False
            prev_cnt = len(tweet_lineage)
            '''split next tweets into array'''
            initial['next_tweet_id'] = initial['next_tweet_id'].apply(lambda x: x.split(","))
            '''break multiple response tweets into other rows'''
            initial = initial.explode("next_tweet_id")
            '''filter out tweets that aren't going to match with repsonse tweets (to speed up merge)'''
            tweet_lineage = tweet_lineage[~tweet_lineage['tweet_id'].isin(initial['latest_tweet_id'])]

            '''merge tweets on prev tweet to response tweet, filter out nan's'''
            initial = initial.merge(tweet_lineage, left_on='next_tweet_id', right_on='tweet_id',how='left') #
            initial = initial.replace('nan' ,'')

            '''update conversation flow by including the latest tweet'''
            initial['conversation_flow'] = initial['conversation_flow'] + "," + initial['next_tweet_id']
            '''move the current val of the linked list, by updating the current node to the next pointer'''
            initial['latest_tweet_id'] = initial['next_tweet_id']
            initial['next_tweet_id'] = initial['response_tweet_id'].astype(str)
            initial = initial.drop(['in_response_to_tweet_id','tweet_id','response_tweet_id'],axis=1)
            '''filter out tweets that dont have any more responses (end of linkedlist) '''
            end_of_conversation = initial[initial['next_tweet_id'] == 'nan']
            end_of_conversation = self.partition_conversation(end_of_conversation)
            '''write out grouped conversation features to staging directory'''
            if(not end_of_conversation.empty):
                logging.info("Writing conversation with partition %i to staging dir",conversation_length)
                end_of_conversation.to_json("{}/conversations/conv_{}.json".format(self.staging_path,conversation_length),orient='records',lines=True)
            conversation_length+=1
            '''keep traversing linked list'''
            initial = initial[initial['next_tweet_id'] != 'nan']

    def _create_indexes(self,es):
        create_index(es,"conversations",conversations_mapping)
        create_index(es,"tweets",tweets_mapping)

    def index_directory(self,es,path,index_name,index_fn):
        '''
        @es: elastic client -> ElasticSearchClient
        @path: path to index documents -> str
        @index_name: name of index -> str
        @index_fn: function to perform the indexing on a given file -> function
        '''
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                file = os.path.join(root, name)
                logging.info("indexing file %s into conversation index",file)
                index_fn(es,file,index_name)



    def load(self):
        logging.info("Loading data from staging into ElasticSearch")
        es = Elasticsearch([os.getenv("ELASTIC_URI")])
        logging.info("Creating indexes conversations & tweets")
        self._create_indexes(es)
        '''index conversations into ElasticSearch'''
        tweet_path = "{}/{}".format(self.staging_path,"tweets")
        self.index_directory(es,tweet_path,"tweets",bulk_index_csv_documents)
        conversation_path = "{}/{}".format(self.staging_path,"conversations",)
        self.index_directory(es,conversation_path,"conversations",bulk_index_json_documents)


    def _load_to_staging(self,df,filename,index=False):
        df.to_csv(filename, quoting=csv.QUOTE_NONNUMERIC,index=index)

if __name__ == "__main__":
    load_dotenv()
    logging.getLogger().setLevel(os.getenv("LOG_LEVEL"))
    logging.info("Starting ingestion...")
    ti = TweetIngest(os.getenv("TWEET_SRC_URL"),staging_path=os.getenv("STAGING_PATH"))
    extract_df = ti.extract()
    logging.info("Finished extracting %i tweets",len(extract_df))
    transform_df = ti.transform(extract_df)
    logging.info("Finished transforming tweets")
    ti.load()
    logging.info("Finished loading tweets into staging directory %s",ti.staging_dir)