import json
from requests_sse import EventSource

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
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
                '''

                ['$schema', 'meta', 'type', 'namespace', 'title', 'title_url', 'comment', 'timestamp', 'user', 'bot', 'log_id', 
                           'log_type', 'log_action', 'log_params', 'log_action_comment', 'server_url', 'server_name', 'server_script_path',
                           'wiki', 'parsedcomment']
                '''
                # print('{user} edited {title}'.format(**change))
                print(change['title'])

