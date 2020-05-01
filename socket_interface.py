#%%
import socket as Socket
import sys
import requests
import requests_oauthlib
import json
import time
import api_keys
from multiprocessing import Process, Queue
import random

#%%
class StreamError(Exception):

	def __init__(self, response_code, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.response_code = response_code

def stream(response):
	
	for response_line in response.iter_lines():
			if response_line:
				yield(response_line)

class StreamBackoff():

	def __init__(self, monitor_codes, maximum_wait, invert_codes):
		self.monitor_codes = monitor_codes
		self.increment = 0
		self.maximum_wait = maximum_wait
		self.reset()
		self.invert_codes = invert_codes

	def reset(self):
		self.time = time.time()
		self.increment = 0

	def allow_reconnection(self):
		return True

	def track_error(self, code):
		if code in self.monitor_codes or (self.invert_codes and not code in self.monitor_codes) or self.monitor_codes == 'all':
			self.increment += 1
			if time.time() > self.time + self.maximum_wait:
				raise(StreamError(code, 'Stream is not responding'))

class ExponentialBackoff(StreamBackoff):
	
	def __init__(self, monitor_codes, maximum_wait, invert_codes, intitial_time, multiplier):
		super().__init__(monitor_codes, maximum_wait, invert_codes)
		self.intitial_time = intitial_time
		self.multiplier = multiplier

	def allow_reconnection(self):
		return time.time() - self.time >= self.intitial_time * (self.multiplier**self.increment) - self.intitial_time


class LinearBackoff(StreamBackoff):
	
	def __init__(self, monitor_codes, maximum_wait, invert_codes, slope):
		super().__init__(monitor_codes, maximum_wait, invert_codes)
		self.slope = slope

	def allow_reconnection(self):
		return time.time() - self.time >= self.increment * self.slope

#%%
#implement backoff
def manage_connection(endpoint, output_queue, authorization, pause_time = 1.0):

	HTTP_ERROR_CODES = [401, 403, 404, 406, 413, 416, 503]
	backoffs = [
		LinearBackoff(HTTP_ERROR_CODES, 16, True, 0.25),
		ExponentialBackoff(HTTP_ERROR_CODES, 320, False, 5, 2),
		ExponentialBackoff([420],1000, False, 60, 2),
	]

	startime = time.time()
	stream_time = time.time()

	for backoff in backoffs:
		backoff.reset()

	while True:

		if all([backoff.allow_reconnection() for backoff in backoffs]):

			response = requests.get(endpoint, auth = authorization, stream = True)

			if response.status_code == 200:

				print('Connected to stream. Time: {}'.format(str(time.time() - startime)))
				stream_time = time.time()

				for tweet in stream(response):
					if not output_queue.full():
						output_queue.put(tweet)

				for backoff in backoffs:
					backoff.reset()

				print('Disconnected from stream. Elapsed time: {}'.format(str(time.time() - stream_time)))
			
			else:
				for backoff in backoffs:
					backoff.track_error(response.status_code)

				print('Connection failed. Code: {}'.format(str(response.status_code)))
		else:
			print('waiting')
			time.sleep(pause_time)

#%%
#manage_connection('https://stream.twitter.com/1.1/statuses/sample.json?language=en', BACKOFFS, output_fn=preprocess_tweet_json)

#%%
date_format = "Mon May 06 20:01:29 +0000 2019"
#%%
#link to spark through tcp port
def preprocess_tweet_json(tweet_str):
    try:
        tweet_json = json.loads(tweet_str)
        structured_json = dict(created_at = tweet_json["created_at"])
        if 'extended_tweet' in tweet_json:
            extended_json = tweet_json['extended_tweet']
            structured_json['text'] = extended_json['full_text']
            structured_json['hashtags'] = [hashtag['text'] for hashtag in extended_json['entities']['hashtags']]
        else:
            structured_json['text'] = tweet_json['text']
            structured_json['hashtags'] = [hashtag['text'] for hashtag in tweet_json['entities']['hashtags']]

        if tweet_json['place'] is None:
            structured_json['place_name'] = None
            structured_json['lon'] = None
            structured_json['lat'] = None
        else:
            structured_json['place_name'] = tweet_json['place']['full_name'] + ', ' + tweet_json['place']['country']
            bounding_box = tweet_json['place']['bounding_box']
            if bounding_box['type'] == 'Polygon':
                lon, lat = [sum(coors)/len(coors) for coors in list(zip(*bounding_box['coordinates'][0]))]
            else:
                lon, lat = bounding_box['coordinates'][0][0][:]
            structured_json['lon'] = lon
            structured_json['lat'] = lat

        return json.dumps(structured_json)

    except Exception as err:
        raise(err)

def send_to_spark(tcp_ip, tcp_port, attenuation, queue):
	
	socket = Socket.socket(Socket.AF_INET, Socket.SOCK_STREAM)
	socket.bind((tcp_ip, tcp_port))
	socket.listen(0)
	print('Created connection. Waiting for client')
	connection, addr = socket.accept()
	print("Created TCP connection...")
	
	tweets_collected = 0

	while True:
		if not queue.empty():
			send = preprocess_tweet_json(queue.get())
			connection.send((send+'\n').encode())
			#send = queue.get()
			#connection.send(send + '\n'.encode())
			tweets_collected += 1
			print('\rTweets streamed: {}'.format(str(tweets_collected)), end = '')
			

if __name__ == "__main__":

	#%%
	AUTHORIZATION = requests_oauthlib.OAuth1(api_keys.API_KEY, api_keys.API_SECRET_KEY, api_keys.ACCESS_TOKEN, api_keys.ACCESS_SECRET_TOKEN)

	API_ENDPOINT = 'https://stream.twitter.com/1.1/statuses/sample.json?language=en'

	datafeed_queue = Queue(100)
	#endpoint, output_queue, authorization
	stream_process = Process(target = manage_connection, args  = (API_ENDPOINT, datafeed_queue, AUTHORIZATION))
	stream_process.daemon = True

	tcp_process = Process(target = send_to_spark, args= ('localhost', 9009, 0, datafeed_queue))
	#tcp_process.daemon = True

	tcp_process.start()
	stream_process.start()

	tcp_process.join()
	stream_process.join()
