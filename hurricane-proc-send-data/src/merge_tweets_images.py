import random
import pandas as pd


if __name__ == "__main__":

    tweets_stream = pd.read_json(
        "./data/preprocessed/stream/tweets_stream.json")

    # Make a relation between images and tweets
    tweet_ids = tweets_stream["tweet_id"].to_list()
    image_metadata = pd.read_csv(
        "./data/preprocessed/images/images_metadata.csv")

    random.seed(30)
    random_tweet_ids = random.sample(tweet_ids, len(image_metadata))
    image_metadata["tweet_id"] = random_tweet_ids

    # merge two datasets

    merged_tweets_image_metadata = tweets_stream.merge(
        image_metadata, on="tweet_id", how="left")

    merged_tweets_image_metadata.columns

    new_column_names = ['tweet_id', 'account_id', 'likes', 'replies', 'retweets', 'tweet',
                        'time', 'year_month_day', 'damage_flag', 'image_base64', 'latitude',
                        'longitude']

    merged_tweets_image_metadata.columns = new_column_names

    # small susbset for cosmos db

    merged_tweets_image_metadata.to_json(
        "./data/preprocessed/merged/merged_tweets_image_metadata_stream.json", orient="records")

    merged_tweets_image_metadata

    # export 100 sample messages

    # merged_tweets_image_metadata.iloc[0:100, :].to_json(
    #     "./data/sample/merged_tweets_image_metadata_stream_sample.json", orient="records")
