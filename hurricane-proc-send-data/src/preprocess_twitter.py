from pathlib import Path
import itertools
import pandas as pd


# load and shuffle
tweeter_data_filepath = "./data/original/tweeter/tweets_hurricane_harvey.csv"


def read_preprocess_tweeter_data(tweeter_data_filepath):
    """[summary]

    Returns:
        [type]: [description]
    """
    tweets_df = (pd.read_csv(tweeter_data_filepath, encoding='latin1')
                 .drop(labels=['Unnamed: 0'], axis="columns")
                 .dropna()
                 .drop(['ID'], axis="columns")
                 .sample(frac=1).reset_index(drop=True)

                 )
    tweets_df = tweets_df.assign(
        Time=pd.to_datetime(tweets_df["Time"]),
    )

    tweets_df = tweets_df.assign(
        Year=tweets_df["Time"].dt.year,
        Month=tweets_df["Time"].dt.month,
        Day=tweets_df["Time"].dt.day
    )

    tweets_df = tweets_df.assign(
        YearMonthDay=tweets_df['Year'].astype(
            str) + "-" + tweets_df['Month'] .astype(str) + "-" + tweets_df['Day'] .astype(str)
    )

    return tweets_df


def create_artificial_groups(group_size, df):
    """Create artificial ides from the row count and group choice

    Args:
        group_size ([type]): [description]
        df ([type]): [description]

    Returns:
        [type]: [description]
    """
    # split to groups
    number_of_groups = len(df) // group_size
    remainder_from_groups = len(df) % group_size
    assert (number_of_groups * group_size + remainder_from_groups) == len(df)

    # create artificial ids
    twitter_accounts = []
    for id in range(1, number_of_groups + 1):
        twitter_accounts.append(
            [number for number in itertools.repeat(id, group_size)])

    for id in range(number_of_groups, remainder_from_groups + 1):
        twitter_accounts.append(
            [number for number in itertools.repeat(id, remainder_from_groups)])

    remainder_ids = [number for number in itertools.repeat(
        number_of_groups + 1, remainder_from_groups)]

    twitter_accounts.append(remainder_ids)

    # flatten list of lists
    flattened_ids = []
    for sublist in twitter_accounts:
        for element in sublist:
            flattened_ids.append(element)

    return flattened_ids


def assign_id_columns(tweets_df, group_size, df):
    """[summary]

    Args:
        tweets_df ([type]): [description]
    """
    tweets_df["account_id"] = create_artificial_groups(group_size, df)
    tweets_df["tweet_id"] = range(len(tweets_df))

    tweets_df.columns = ['likes', 'replies', 'retweets', 'time', 'tweet', 'year', 'month', 'day',
                         'year_month_day', 'account_id', 'tweet_id']

    column_names = ["tweet_id", "account_id", "likes",
                    "replies", "retweets", "tweet", "time", "year_month_day"]

    tweets_df = tweets_df[column_names]

    return tweets_df


def split_to_batch_and_streaming(tweets_df):
    """[summary]

    Args:
        tweets_df ([type]): [description]
    """
    tweets_stream = ['2017-8-25']

    tweets_batch = ['2017-8-26', '2017-8-27', '2017-8-24', '2017-8-28',
                    '2017-8-29', '2017-8-23', '2017-6-1', '2017-6-21', '2017-8-17',
                    '2017-8-18', '2017-8-14', '2017-8-22', '2017-2-19', '2017-3-28',
                    '2017-8-19', '2017-8-13', '2017-5-21', '2017-5-26', '2017-4-19',
                    '2017-8-16', '2017-8-15', '2017-6-9', '2017-8-21', '2017-6-2',
                    '2017-2-21', '2017-5-25', '2017-4-20', '2017-8-4', '2017-3-21',
                    '2017-8-20', '2017-1-11', '2017-2-10']

    tweets_stream = tweets_df.query("year_month_day in @tweets_stream")
    tweets_batch = tweets_df.query("year_month_day in @tweets_batch")

    tweets_stream.to_csv(
        "./data/preprocessed/stream/tweets_stream.csv", index="False")

    (tweets_batch
        .groupby("year_month_day")
        .apply(
            lambda x: x.to_csv(
                f"./data/preprocessed/batch/{x.name}.csv", index=False)
        )
     )

    tweets_stream.to_json(
        "./data/preprocessed/stream/tweets_stream.json", orient="records")

    return tweets_stream


if __name__ == "__main__":

    tweets_df = read_preprocess_tweeter_data(tweeter_data_filepath)

    flattened_ids = create_artificial_groups(group_size=100, df=tweets_df)

    tweets_df = assign_id_columns(tweets_df, group_size=100, df=tweets_df)

    tweets_stream = split_to_batch_and_streaming(tweets_df)

    tweets_stream.to_json(
        "./data/preprocessed/stream/tweets_stream.json", orient="records")
