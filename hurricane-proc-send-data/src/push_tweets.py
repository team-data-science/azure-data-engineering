import fire
import os
from dotenv import load_dotenv
import pandas as pd
import requests
import time

load_dotenv()

HURRICANE_API_URL = os.getenv("HURRICANE_API_URL")
AUTH_VALUE = os.getenv("AUTH_VALUE")
OCP_APIM_SUBSCRIPTION_VALUE = os.getenv("OCP_APIM_SUBSCRIPTION_VALUE")

URL = f"{HURRICANE_API_URL}"
# URL = "http://localhost:7071/api/HttpTrigger1"

headers = {
    'Host': 'noreur-dev-dataeng-apim.azure-api.net',
    'Authorization': AUTH_VALUE,
    'Ocp-Apim-Subscription-Key': OCP_APIM_SUBSCRIPTION_VALUE,
    'Ocp-Apim-Trace': 'true'
}


def send_tweets_to_rest_api(num_tweets):
    """[summary]

    Args:
        number_of_tweet_messages ([type]): [description]
    """
    tweets_stream_sample = pd.read_json(
        "./data/preprocessed/merged/merged_tweets_image_metadata_stream.json",
        orient="records")

    if num_tweets < 1000:  # tweets_stream_sample.shape[0]:
        tweets_stream_sample = tweets_stream_sample.iloc[:num_tweets, ]
        for i in tweets_stream_sample.index:

            print(tweets_stream_sample.iloc[i])

            records_for_export = tweets_stream_sample.iloc[i].to_json()
            request = requests.post(
                url=URL, data=records_for_export, headers=headers)

            if request.status_code == 200:
                print(f"Request number {i} is succesfullly posted.")
                print(records_for_export)
                time.sleep(10)
                print(request.content)
                print(request.status_code)

            else:
                print(
                    f"Request number {i} NOT succeded! Status code {request.status_code}")
    else:
        print(
            f"Maximum number of tweets is {tweets_stream_sample.shape[0]}, and you have chosen: {num_tweets}. Please choose a smaller number!")


if __name__ == '__main__':
    fire.Fire()
