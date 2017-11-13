import twython
from twython import TwythonStreamer

#OAUTH_TOKEN = "YOUR_ACCESS_TOKEN"
#OAUTH_TOKEN_SECRET = "YOUR_TOKEN_SECRET"
#APP_KEY = "YOUR_CONSUMER_KEY"
#APP_SECRET = "YOUR_CONSUMER_SECRET"

OAUTH_TOKEN = '729900044-dbCwxal97lyVDj1mnF6Hcb3ZevoSyjeTUiS8huYh'
OAUTH_TOKEN_SECRET = 'qfu9O1LCRs5Isi8nFzG5fkcD6tNgFig44GZSfjjBCdFcB'
APP_KEY = 'Ud7bcH6AeWY3XvYm7QdLsh5PR'
APP_SECRET = '7elhBsoWTk1Kfi5lAs5SuozOvMMsKVnv7shpPSqB1tibovw0sN'
 



class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            print data['text'].encode('utf-8')

    def on_error(self, status_code, data):
        print status_code

        # Want to stop trying to get data because of the error?
        # Uncomment the next line!
        self.disconnect()

if __name__ == "__main__":
    stream = MyStreamer(APP_KEY, APP_SECRET,
                    OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    stream.statuses.filter(track=['python', 'javascript', 'ruby'])