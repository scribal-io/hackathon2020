import os
import slack
#import slackclient
import json
from time import sleep
from kafka import KafkaProducer #new

#pagination - set values high enough to avoid this extra complexity. 
MESSAGES_PER_PAGE = 500
MAX_MESSAGES = 1000

# init web client
client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
# init kafka producer
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

#return a list of channels
response = client.conversations_list()
assert response["ok"]
channels_all = response['channels']
reduced_list = [d for d in channels_all if d['is_member'] is True]
filtered_list = [i["id"] for i in reduced_list] #channels_all]

#loop through list of channel ids and obtain messages

messages_all = []
for n in filtered_list:
    print(n)
    #get first page ( the only page ...)
    page = 1
    print("retreiving page {}".format(page))
    response = client.conversations_history(
        channel=n,
        limit=MESSAGES_PER_PAGE,
    )
    assert response["ok"]
    for r in response["messages"]: #new
        encoded_msg = json.dumps(r).encode('utf-8')
        print(encoded_msg)
        producer.send('message_history', encoded_msg)
        producer.flush(30)
'''
    messages_all = messages_all + response["messages"]
    with open('channel_messages.json', 'w', encoding='utf-8') as f:
        json.dump(
            messages_all,
            f,
            sort_keys=True,
            indent=4,
            ensure_ascii=False
        )
'''


