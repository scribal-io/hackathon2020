#test to check auth is functioning

from slack import WebClient
from slackmetrics import SlackMetrics
import os

# Create a slack client
slack_web_client = WebClient(token=os.environ.get("SLACK_TOKEN"))

# Get a new StatsBot
stats_bot = SlackMetrics("#random")

# Get the onboarding message payload
message = stats_bot.get_message_payload()

# Post the onboarding message in Slack
slack_web_client.chat_postMessage(**message)