import pandas as pd

image_metadata = pd.read_json(
    "./data/preprocessed/stream/merged_tweets_image_metadata_stream.json")

image_metadata.columns

image_metadata['year_month_day'].unique()


def split_to_batch_and_streaming(tweets_df):
    """[summary]

    Args:
        tweets_df ([type]): [description]
    """

    tweets_batch = ['2017-8-26', '2017-8-29', '2017-8-27', '2017-8-24', '2017-8-28',
                    '2017-8-23', '2017-8-21', '2017-8-22', '2017-8-18', '2017-6-1',
                    '2017-8-14', '2017-8-17', '2017-6-21', '2017-8-13', '2017-8-19',
                    '2017-6-9', '2017-3-28', '2017-5-26', '2017-5-21', '2017-8-15',
                    '2017-5-25', '2017-8-20', '2017-8-16', '2017-2-19', '2017-1-11',
                    '2017-4-20', '2017-2-21', '2017-4-19', '2017-8-4', '2017-6-2',
                    '2017-3-21', '2017-2-10']

    tweets_batch = tweets_df.query("year_month_day in @tweets_batch")

    (tweets_batch
        .groupby("year_month_day")
        .apply(
            lambda x: x.to_csv(
                f'./data/preprocessed/batch/{x.name}.csv',
                index=False,

            )
        )
     )


split_to_batch_and_streaming(image_metadata)
