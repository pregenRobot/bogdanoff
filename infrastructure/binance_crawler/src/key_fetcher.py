import subprocess
from peewee import *
from bs4 import BeautifulSoup as Soup
from datetime import datetime
import time
import shutup
import uuid
from typing import *
import pymysql

db = PostgresqlDatabase(""
