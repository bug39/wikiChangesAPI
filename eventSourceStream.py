import json, os, boto3, gzip
from requests_sse import EventSource
import datetime as dt

bucket = "wikichangesbucket"
s3 = boto3.client("s3")

url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def object_key(ts: dt.datetime, chunk_id: int) -> str:
    return f"date={ts:%Y-%m-%d}/hour={ts:%H}/rc_{ts:%Y%m%dT%H%M%S}_{chunk_id:04}.json.gz"

CHUNK_ROWS, chunk, chunk_id = 20_000, [], 0
with EventSource(url) as stream:
    for event in stream:
        if event.type == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                pass
            else:
                # discard canary events
                if change['meta']['domain'] == 'canary':
                    continue            
                
                #start processing through
                chunk.append(json.dumps(change, ensure_ascii=False))
                if len(chunk) >= CHUNK_ROWS:
                    curr_time = dt.datetime.now
                    object_key = object_key(curr_time, chunk_id)
                    body = gzip.compress("\n".join(chunk).encode())
                    s3.put_object(Bucket = bucket, Key = object_key, Body = body)
                    chunk, chunk_id = [], 0