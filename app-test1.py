#uses the events api to listen for new messages in channel
import os
import logging
from flask import Flask
from slack import WebClient
from slackeventsapi import SlackEventAdapter
from kafka import KafkaProducer #new


# Initialize a Flask app to host the events adapter
app = Flask(__name__)
# Create an events adapter and register it to an endpoint in the slack app for event ingestion.
slack_events_adapter = SlackEventAdapter(os.environ.get("SLACK_EVENTS_TOKEN"), "/slack/events", app)

# Initialize a Web API client
slack_web_client = WebClient(token=os.environ.get("SLACK_TOKEN"))
producer = KafkaProducer(bootstrap_servers = '164.90.150.240:9092', api_version=(0, 10, 1))#new

# When a 'message' event is detected by the events adapter, forward that payload
# to this function.
@slack_events_adapter.on("message")
def message(payload):

    #parse the message event and obtain the channel id and the user id for passage to kafka
    #before:

    # Get the event data from the payload
    event = payload.get("event", {})

    channel_id = event.get("channel")
    user_id = event.get("user")
    text = event.get("text")
    ts = event.get("ts")
    ack = producer.send('random', text.encode()) #check this

#   producer.flush() this should not be called in this way, makes it synchronous.
    print(user_id, channel_id, text, ts)
#    metadata = ack.get()
#    print(metadata.topic)
#    print(metadata.partition)
    return #channel_id, user_id





if __name__ == "__main__":
    # Create the logging object
    logger = logging.getLogger()

    # Set the log level to DEBUG. This will increase verbosity of logging messages
    logger.setLevel(logging.DEBUG)

    # Add the StreamHandler as a logging handler
    logger.addHandler(logging.StreamHandler())

    # Run our app on our externally facing IP address on port 3000 instead of
    # running it on localhost, which is traditional for development.
    app.run(host='164.90.150.240', port=3000)
