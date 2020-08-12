#initial test to ensure that the slcakbot can receive & process data 

import random

class SlackMetrics:

    STATS_BLOCK = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "Sure! Flipping a coin....\n\n"
            ),
        },
    }
    
    # The constructor for the class. It takes the channel name as the a
    # parameter and sets it as an instance variable.
    def __init__(self, channel):
        self.channel = channel

    #replace this method with kafka connector...

    # Generate a random number to simulate flipping a coin. Then return the
    # crafted slack payload with the coin flip message.
    def _flip_coin(self):
        rand_int =  random.randint(0,1)
        if rand_int == 0:
            results = "Heads"
        else:
            results = "Tails"

        text = f"The result is {results}"

        return {"type": "section", "text": {"type": "mrkdwn", "text": text}},

    # Craft and return the entire message payload as a dictionary.
    def get_message_payload(self):
        return {
            "channel": self.channel,
            "blocks": [
                self.STATS_BLOCK,
                *self._flip_coin(),
            ],
        }