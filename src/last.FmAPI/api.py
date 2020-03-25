import requests
import json
import os

from flask import Flask
from datetime import datetime
from dotenv import load_dotenv

from config.index import environment_variable



app = Flask(__name__)

@app.route('/api', methods=['GET', 'POST'])
def home():
  parameters = {
    'lat': 40.71,
    'lon': -74
  }

  app.logger.debug('App is running')
  response = requests.get(environment_variable['open_notify'], params=parameters)

  res = response.json()['response']
  rise_time = []
  times = []
  duration = 0
  for i in res:
    rise_time.append(i['risetime'])
    duration += i['duration']
  
  for rt in rise_time:
    time = datetime.fromtimestamp(rt)
    times.append(time)

  return response.json(), 200


@app.route('/music', methods=['GET'])
def music():
  headers = {
    'user_agent':  environment_variable['user_agent']
  }
  payload = {
    'api_key': environment_variable['api_key'],
    'method': 'chart.gettopartists',
    'format': 'json',
  }
  res = requests.get(environment_variable['music_api'], headers=headers, params=payload)

  if res.status_code == 200:
    print(res.json()['artists']['artist'])
    return res.json()['artists'], 200

  return 'Error talking to the Music api', 400


@app.errorhandler(404)
def not_found(error):
  app.logger.warn('Page not found')
  return 'Not Found', 404



if __name__ == '__main__':
  app.run(port=2000, debug=True)
