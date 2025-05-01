import os
import time
import datetime
import requests
import pandas as pd
import pyarrow
from prefect import flow, task # Prefect flow and task decorators
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Let's set your map key that was emailed to you. It should look something like 'abcdef1234567890abcdef1234567890'
MAP_KEY = '5e8bad8d50fa1ca84ea72175e2bace34' #key from guy.80647@gmail.com
#MAP_KEY = 'abcdef1234567890abcdef1234567890'
thai_url = 'https://firms.modaps.eosdis.nasa.gov/api/country/csv/' + MAP_KEY + '/MODIS_NRT/THA/2'
