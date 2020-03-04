import json
import re
import os
import time
import tweepy
import shutil

import datetime as dt
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from textblob import TextBlob

load_dotenv(find_dotenv())


class TwitterCron:
    file_directory = ''
    json_file = ''
    json_file_root = ''
    result = []
    file = ''
    name = ''

    @staticmethod
    def connect_api():
        """
        LOAD API
        """
        consumer_key = os.getenv('CONSUMER_KEY')
        consumer_secret = os.getenv('CONSUMER_SECRET')
        access_token = os.getenv('ACCESS_TOKEN')
        access_secret = os.getenv('ACCESS_SECRET')
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        return tweepy.API(auth)


    @staticmethod
    def tweet_search(api, query, max_tweets, max_id, since_id, geocode):
        """ Function that takes in a search string 'query', the maximum
        number of tweets 'max_tweets', and the minimum (i.e., starting)
        tweet id. It returns a list of tweepy.models.Status objects. """

        searched_tweets = []
        while len(searched_tweets) < max_tweets:
            remaining_tweets = max_tweets - len(searched_tweets)
            try:
                new_tweets = api.search(q=query, count=remaining_tweets,
                                        since_id=str(since_id),
                                        max_id=str(max_id - 1), geocode=geocode)
                twee = len(new_tweets)
                print(f'found {twee} tweets')
                if not new_tweets:
                    print('no tweets found')
                    break
                searched_tweets.extend(new_tweets)
                max_id = new_tweets[-1].id
            except tweepy.TweepError:
                delta = dt.datetime.now() + dt.timedelta(minutes=10)
                print(f'exception raised, waiting 10 minutes\n(until: {delta})')
                time.sleep(1 * 10 * 60)
                break  # stop the loop
        return searched_tweets, max_id


    @staticmethod
    def write_tweets(tweets, filename):
        """ Function that appends tweets to a file.
        @param tweets:
        @param filename:
        """

        with open(filename, 'a') as f:
            for tweet in tweets:
                json.dump(tweet._json, f)
                f.write('\n')
    

    @staticmethod
    def create_table(tweets):
        """
        Function creates table from a list of tweet information
        """
        df = pd.DataFrame()

        df['createdTime'] = [tweets[i]['created_at'] for i in
                            range(len(tweets))]
        df['hashtags'] = [len(tweets[i]['entities']['hashtags']) for i in
                        range(len(tweets))]
        df['userMentions'] = [len(tweets[i]['entities']['user_mentions']) for i
                            in range(len(tweets))]
        df['retweetCount'] = [tweets[i]['retweet_count'] for i in
                            range(len(tweets))]
        df['favoriteCount'] = [tweets[i]['favorite_count'] for i in
                            range(len(tweets))]
        df['isReply'] = [True if tweets[i]['in_reply_to_status_id'] else False
                        for i in range(len(tweets))]
        df['UserCreatedDate'] = [tweets[i]['user']['created_at'] for i in
                                range(len(tweets))]
        df['UserLikesNo'] = [tweets[i]['user']['favourites_count'] for i in
                            range(len(tweets))]
        df['UserFollowerNo'] = [tweets[i]['user']['followers_count'] for i in
                                range(len(tweets))]
        df['UserFriendsNo'] = [tweets[i]['user']['friends_count'] for i in
                            range(len(tweets))]
        df['UserListNo'] = [tweets[i]['user']['listed_count'] for i in
                            range(len(tweets))]
        df['UserTotalTweet'] = [tweets[i]['user']['statuses_count'] for i in
                                range(len(tweets))]
        df['UserIsVerified'] = [tweets[i]['user']['verified'] for i in
                                range(len(tweets))]
        df['tweet'] = [tweets[i]['text'] for i in range(len(tweets))]
        print("Dataframe contains : {}".format(df.shape))
        return df

    @staticmethod
    def clean_app(directory):
        if os.path.isdir(directory):
            shutil.rmtree(directory)
        else:
            raise ValueError(f'The path {directory} is not a directory')

    @staticmethod
    def clean_tweet(tweet):
        """
        Utility function to clean text by removing links and special characters
        """
        return ' '.join(
            re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ",
                   tweet).split())

    @staticmethod
    def get_tweet_id(api, date='', days_ago=10, query='a'):
        """ Function that gets the ID of a tweet. This ID can then be
        used as a 'starting point' from which to search. The query is
        required and has been set to a commonly used word by default.
        The variable 'days_ago' has been initialized to the maximum
        amount we are able to search back in time (9).
        @rtype: """

        if date:
            # return an ID from the start of the given day
            td = date + dt.timedelta(days=1)
            tweet_date = '{0}-{1:0>2}-{2:0>2}'.format(
                td.year, td.month, td.day)
            tweet = api.search(q=query, count=1, until=tweet_date)
        else:
            # return an ID from __ days ago
            td = dt.datetime.now() - dt.timedelta(days=days_ago)
            tweet_date = '{0}-{1:0>2}-{2:0>2}'.format(
                td.year, td.month, td.day)
            # get list of up to 10 tweets
            tweet = api.search(q=query, count=10, until=tweet_date)
            print(f"search limit (start/stop): {tweet[0].created_at}")
            # return the id of the first tweet in the list
            return tweet[0].id

    def result(self):
        self.result = [json.loads(line) for line in open(
            r'{}/{}'.format(self.file_directory, self.json_file))]
        table_results = self.create_table(self.result)
        table_results['clean_tweet'] = table_results.tweet.apply(
            self.clean_tweet)
        table_results['clean_tweet'] = table_results.clean_tweet.str.replace(
            '_', '')
        table_results['clean_tweet'] = table_results.clean_tweet.str.replace(
            '#', '')
        table_results['clean_tweet'] = table_results.clean_tweet.str.replace(
            '/', '')
        table_results['clean_tweet'] = table_results.clean_tweet.str.replace(
            '\.', '')
        self.save_tweet('{}'.format('{}.txt'.format(self.file)))
        table_results['sentiment'] = table_results.clean_tweet.apply(
            self.get_tweet_sentiment)
        table_results.to_csv(r'csv/{}'.format('{}.csv'.format(self.file)))
        self.clean_app('{}/{}'.format(self.file_directory, self.name))

    def get_tweet_sentiment(self, tweet):
        """
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        """
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def save_tweet(self, tweet_file):
        with open(r'tweets/{}'.format(tweet_file), 'w',
                  encoding='utf-8') as dat:
            for tweet in range(len(self.result)):
                dat.write(self.clean_tweet(self.result[tweet]['text']))
                dat.write('\n')
        dat.close()

    def main(self):
        """ This is a script that continuously searches for tweets
        that were created over a given number of days. The search
        dates and search phrase can be changed below. """

        ''' search variables: '''

        search_keyword = '{}'.format(input('Input search keyword: '))

        search_phrases = search_keyword.split(',')

        time_limit = 1.5  # runtime limit in hours
        max_tweets = 100  # number of tweets per search (will be
        # iterated over) - maximum is 100
        min_days_old, max_days_old = 0, 7  # search limits e.g., from 7 to 8
        # gives current weekday from last week,
        # min_days_old=0 will search from right now
        location = '6.5244,3.3792,2500km'  # this geocode includes nearly all
        # American
        # states (and a large portion of Canada)
        # loop over search items,
        # creating a new file for each
        for search_phrase in search_phrases:
            self.file_directory = os.getcwd()
            print(f'Search phrase = {search_phrase}')
            ''' other variables '''
            self.name = search_phrase
            self.json_file_root = f'{self.name}/{self.name}'
            os.makedirs(os.path.dirname(self.json_file_root), exist_ok=True)
            read_IDs = False

            # open a file in which to store the tweets
            if max_days_old - min_days_old == 1:
                d = dt.datetime.now() - dt.timedelta(days=min_days_old)
                day = '{0}-{1:0>2}-{2:0>2}'.format(d.year, d.month, d.day)
            else:
                d1 = dt.datetime.now() - dt.timedelta(days=max_days_old - 1)
                d2 = dt.datetime.now() - dt.timedelta(days=min_days_old)
                day = '{0}-{1:0>2}-{2:0>2}_to_{3}-{4:0>2}-{5:0>2}'.format(
                    d1.year, d1.month, d1.day, d2.year, d2.month, d2.day)
            self.json_file = self.json_file_root + '_' + day + '.json'
            self.file = f'{self.name}_{day}'
            if os.path.isfile(self.json_file):
                print(f'Appending tweets to file named: {self.json_file}')
                read_IDs = True

            # authorize and load the twitter API
            api = self.connect_api()

            # set the 'starting point' ID for tweet collection
            if read_IDs:
                # open the json file and get the latest tweet ID
                with open(self.json_file, 'r') as f:
                    lines = f.readlines()
                    max_id = json.loads(lines[-1])['id']  # loads
                    print('Searching from the bottom ID in file')
            else:
                # get the ID of a tweet that is min_days_old
                if min_days_old == 0:
                    max_id = - 1
                else:
                    max_id = self.get_tweet_id(
                        api, days_ago=(min_days_old - 1))
            # set the smallest ID to search for
            since_id = self.get_tweet_id(api, days_ago=(max_days_old - 1))
            print(
                f"max id (starting point) = {max_id}\nsince id (ending point) = {since_id}")

            ''' tweet gathering loop  '''
            start = dt.datetime.now()
            end = start + dt.timedelta(hours=time_limit)
            count, exit_count = 0, 0
            while dt.datetime.now() < end:
                count += 1  # keep counting until you reach hours of time limit
                print(f'count = {count}')
                # collect tweets and update max_id
                tweets, max_id = self.tweet_search(api, search_phrase,
                                                   max_tweets,
                                                   max_id=max_id,
                                                   since_id=since_id,
                                                   geocode=location)
                # write tweets to file in JSON format
                if tweets:
                    self.write_tweets(tweets, self.json_file)
                    exit_count = 0
                else:
                    exit_count += 1
                    if exit_count == 3:
                        break
        self.result()
        newJob = TwitterCron()
        newJob.restart()


    def restart(self):
        time.sleep(1 * 5 * 60)
        self.starts()
            
    def starts(self):
        self.main()

if __name__ == '__main__':
    job = TwitterCron()
    job.starts()
