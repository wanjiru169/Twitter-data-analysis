import json
import pandas as pd
from  textblob import TextBlob
import  numpy as np

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:

        #get the list from the read_json function and access all the statuses_counts values

        
        
        statuses_count= [status['user']['statuses_count'] for status in self.tweets_list]

        return statuses_count
        
    def find_full_text(self)->list:
        
        
        text=[]

       

        for texts in self.tweets_list:
            try:

                text.append(texts.get('retweeted_status',None).get('extended_tweet',None).get('full_text', None))

            except AttributeError:
                text.append('')
        
        return text
       
       
    
    def find_sentiments(self, text)->list:
        
        polarity=[TextBlob(tweet).sentiment.polarity for  tweet in text] 
        subjectivity=[TextBlob(tweet).sentiment.subjectivity for tweet in text]


        return polarity, subjectivity
    def find_lang(self)->list:
        lang=[lans['lang']for lans in self .tweets_list]

        return lang 
    def find_created_time(self)->list:
        created_at=[created['created_at'] for created in self.tweets_list]
        return created_at

    def find_source(self)->list:
       
      
        source= [sources['source'] for sources in self.tweets_list]

        return source

    def find_screen_name(self)->list:
        
        screen_name = [screens['user']['screen_name'] for screens in self.tweets_list]

        return screen_name

    def find_followers_count(self)->list:
        
         
        
        
        followers_count =[followers['user']['followers_count'] for followers in self.tweets_list]
        return followers_count
    def find_friends_count(self)->list:
        
        
        
        
        friends_count =[friends['user']['friends_count'] for friends in self.tweets_list] 
        return friends_count
    def is_sensitive(self)->list:
        
        
        
        try:
            is_sensitive = [x.get('possibly_sensitive', None) for x in self.tweets_list]
        except KeyError:
            is_sensitive =  None

        return is_sensitive

    def find_favourite_count(self)->list:
      
        
        
        favourite_count=[]
        
        for favourites in self.tweets_list:
            try:     
      
                favourite_count.append(favourites.get('retweeted_status',None).get('favorite_count', None ))
        
            except AttributeError:
                favourite_count.append(np.nan)

      

       
        return favourite_count


    def find_retweet_count(self)->list:
       

        retweet_count=[]
        for retweets in self.tweets_list:
        
            try:
                retweet_count.append(retweets.get('retweeted_status', None).get('retweet_count',None))

            except AttributeError:
                retweet_count.append(np.NaN)

        return retweet_count



    def find_hashtags(self)->list:
        hashtags =[x['entities']['hashtags'] for x in self.tweets_list]
        return hashtags
    def find_mentions(self)->list:
        mentions = [x['entities']['user_mentions'] for x in self.tweets_list]

        return mentions
    def find_location(self)->list:
        try:
            location = [x['user']['location'] for x in self.tweets_list]
        except TypeError:
            location = ''
        
        return location

    def find_original_author(self)->list:
        origins=[org['user']['name']for org in self.tweets_list]

        return origins

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'screen_name','followers_count','friends_count', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        original_author=self.find_original_author()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count,retweet_count,original_author,  screen_name, follower_count, friends_count,  hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)
        df.to_csv('finals-processeds_tweet_data.csv', index=False)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("data/covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above

    