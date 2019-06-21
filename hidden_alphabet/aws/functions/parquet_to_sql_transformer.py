from multiprocessing.pool import Pool
from s3fs import S3FileSystem
import pyarrow.parquet as pq
import multiprocessing as mp
import pyarrow as pa
import psycopg2
import boto3
import io
import os
from dotenv import load_dotenv

load_dotenv()

# S3 = S3FileSystem(key=os.environ['ACCESS_KEY_ID'],
#                   secret=os.environ['SECRET_ACCESS_KEY']
# )
# CLIENT = boto3.client('s3')

query = """
        INSERT INTO twitter(
        user_id,
        user_name,
        user_href,
        user_handle,
        user_avatar_href,

        tweet_id,
        tweet_item_id,
        tweet_conversation_id,
        tweet_text_html,
        tweet_time,
        tweet_nonce,
        tweet_language,
        tweet_timestamp_ms,
        tweet_permalink,

        mentions_count,
        retweets_count,
        favorites_count,

        is_reply,
        is_retweet,
        has_media,
        has_mentions,
        has_quote_tweet
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    );
"""

def create_query(filepath):
    db = psycopg2.connect(
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT'],
        dbname=os.environ['PG_DBNAME'],
        user=os.environ['PG_USERNAME'],
        password=os.environ['PG_PASSWORD']
    )

    parquet = pq.read_table(filepath).to_pydict()
    rows = list(zip(*parquet.values()))

    cursor = db.cursor()
    cursor.executemany(query, rows)

    db.commit()

    cursor.close()
    db.close()

def handler(event=None, context=None):
    """
      Lambda function
    """
    status = 'error'

    if len(event['Records']) > 0:
        len(event['Records'])

        objects = [(record['s3']['bucket']['name'], record['s3']['object']['key']) for record in event['Records']]
        files = ["s3://{}/{}".format(bucket, key) for bucket, key in objects]

        pool = Pool(min(mp.cpu_count(), len(event['Records'])))

        queries = pool.map(create_query, files)

        pool.close()
        pool.join()

        status = 'ok'

    return { 'status': status }