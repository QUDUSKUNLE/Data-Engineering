import os
from dotenv import load_dotenv

load_dotenv(verbose=True)

environment_variable = {
  'open_notify': os.getenv('OPENNOTIFY'),
  'api_key': os.getenv('API_KEY'),
  'user_agent': os.getenv('USER_AGENT'),
  'music_api': os.getenv('MUSIC_API')
}
