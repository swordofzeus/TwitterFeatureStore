import graphene
import os
from graphene import ObjectType, String, Field, Int, Boolean
from elasticsearch7 import Elasticsearch, helpers
from dotenv import load_dotenv


#TODO: Need to create a reusable connection so that a new connection doesnt occur on each resolver invocation.
# This requires waiting for ElasticSearch to be spun up before GraphQL attempts a connect on start and requires
# a more involved docker-compose file then I had time to create. I just created a new connection each request just for
# this demo. Logic to wait for ES would go in app.py before app.run() is invoked and the connection stored in the GraphQL Context object.
load_dotenv()

class MostFrequentMessage(ObjectType):
    message = String()
    #TODO: add > 5 authors in custom grapql type


class Query(ObjectType):
    mostFrequentMessage = Field(
        MostFrequentMessage, date=String(required=True))
    numberOfTurns = Field(Int, turns=Int(required=True))
    isPartOfConversation = Field(Boolean, tweetId1=Int(
        required=True), tweetId2=Int(required=True))

    def resolve_isPartOfConversation(parent, info, tweetId1, tweetId2):
        es = Elasticsearch([os.getenv('ELASTIC_URI')])
        query_body = {
                        "query": {
                            "bool": {
                                "filter": [
                                    {"term": {"conversation_flow": tweetId1}},
                                    {"term": {"conversation_flow": tweetId2}}
                                ]
                            }
                        }
                    }

        result = es.search(index="conversations", body=query_body)
        is_part_of_conversation = result['hits']['total']['value'] > 0
        return is_part_of_conversation

    def resolve_numberOfTurns(parent, info, turns):
        es = Elasticsearch([os.getenv('ELASTIC_URI')])
        query_body = {
            "query": {
                "match": {
                    "conversation_length": turns
                }
            }
        }
        '''size=0 prevents fetching the actual documents and just grabs the count'''
        result = es.search(index="conversations", body=query_body,size=0,track_total_hits=True)
        return result['hits']['total']['value']

    def resolve_mostFrequentMessage(parent, info, date):
        es = Elasticsearch([os.getenv('ELASTIC_URI')])
        most_freq_message = {}
        query_body = {
            "query": {
                "match": {
                    "created_at": date
                }
            },

            "aggs": {
                "top-terms-aggregation": {
                    "terms": {"field": "text_hash"}
                }
            }
        }
        result = es.search(index="tweets", body=query_body)
        document_hits = result['hits']['total']['value']
        '''if no document hits, no messages exist for that date. if greater then 1 take the most popular message'''
        most_frequent_msg_count = result['aggregations'][
            'top-terms-aggregation']['buckets'][0]['doc_count'] if document_hits > 0 else 0

        '''if most popular message has count 1, return empty json otherwise return message'''
        most_freq_message = {} if (most_frequent_msg_count < 2) else {
            'message': result['hits']['hits'][0]['_source']['text']}
        return most_freq_message


schema = graphene.Schema(query=Query)
