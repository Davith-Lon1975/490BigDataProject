import pandas as pd
from pmaw import PushshiftAPI
import datetime as dt


api = PushshiftAPI()
before = int(dt.datetime(2021,4,1,0,0).timestamp())
afterD = int(dt.datetime(2009,1,1,0,0).timestamp()) # The created Date for r/depression
afterSW = int(dt.datetime(2008,12,16,0,0).timestamp()) # the created Date for r/SuicideWatch

subreddit1 = 'SuicideWatch'
subreddit2 = 'depression'

limit = 800000

postsSW = api.search_submissions(subreddit = subreddit1, limit = limit, before = before, after = afterSW)

postsSW_df = pd.DataFrame(postsSW)

postsSW_df = postsSW_df[["selftext", "subreddit"]]

postsD = api.search_submissions(subreddit = subreddit2, limit = limit, before = before, after = afterD)

postsD_df = pd.DataFrame(postsD)

postsD_df = postsD_df[["selftext", "subreddit"]]

concat_df = pd.concat([postsD_df, postsSW_df])

concat_df.to_csv('concat_df.csv', encoding='utf-8', index=False)