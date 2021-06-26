import pandas as pd
import base64


tweets = pd.read_json(
    "data/preprocessed/stream/merged_tweets_image_metadata_stream.json")


tweets.iloc[0:2].to_json("mock_api.json", orient="records")


# with open("./data/original/images/test/damage/-93.6141_30.754263.jpeg", "rb") as image_file:
#     encoded_string = base64.b64encode(image_file.read())
