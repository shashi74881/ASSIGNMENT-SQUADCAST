# -*- coding: utf-8 -*-
# ----------------------------------------------------
# Created By: Shashi Shekhar
# Created Date: Monday, November 27, 2023. 14:35:00 IST
# Version ='0.1'
# ----------------------------------------------------
"""
The following code reads CSV files and inserts it into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
from config import db_url


# Read movie and rating CSV files
movies_df = pd.read_csv('movies.csv')
ratings_df = pd.read_csv('ratings.csv')

# Connect to PostgreSQL database
engine = create_engine(db_url)

# Import DataFrames into PostgreSQL
movies_df.to_sql('movies', engine, if_exists='replace', index=False)
ratings_df.to_sql('ratings', engine, if_exists='replace', index=False)