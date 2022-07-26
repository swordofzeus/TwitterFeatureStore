
conversations_mapping = {
    "mappings": {
        "properties": {
            "tweet_id": {
                "type": "integer"
            },
            "conversation_flow": {
                "type": "integer"
            }
        }
    }
}

tweets_mapping = {
    "mappings": {
        "properties": {
            "tweet_id": {
                "type": "integer"
            },
            "text_hash": {
                "type": "keyword"
            }
        }
    }
}
