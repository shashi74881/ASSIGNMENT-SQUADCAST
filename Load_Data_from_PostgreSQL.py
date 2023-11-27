# -*- coding: utf-8 -*-
# ----------------------------------------------------
# Created By: Shashi Shekhar
# Created Date: Monday, November 27, 2023. 16:25:00 IST
# Version ='0.1'
# ----------------------------------------------------
"""
The following code reads movies and rating tables from PostgreSQL, analyse and prints useful insights.
"""

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from config import db_url

try:
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # Create a Session object
    Session = sessionmaker(bind=engine)
    session = Session()

    # Retrieve movie data from the database
    query_movies = "SELECT * FROM movies"
    result_movies = session.execute(sa_text(query_movies)).fetchall()
    movies_df = pd.DataFrame(result_movies, columns=['movie_id', 'title', 'release_year', 'country', 'genre', 'director', 'duration', 'poster'])

    # Retrieve rating data from the database
    query_ratings = "SELECT * FROM ratings"
    result_ratings = session.execute(sa_text(query_ratings)).fetchall()
    rating_df = pd.DataFrame(result_ratings, columns=['rater_id', 'movie_id', 'rating', 'time'])

    # Merge movies_df and rating_df on 'movie_id'
    merged_df = pd.merge(movies_df, rating_df, on='movie_id')

    # Display top movies by duration and release year
    top_duration = movies_df.nlargest(5, 'duration')
    top_year = movies_df.nlargest(5, 'release_year')
    print("Top 5 Movie Titles by Duration:\n", top_duration[['title', 'duration']])
    print("\nTop 5 Movie Titles by Year of Release:\n", top_year[['title', 'release_year']])

    # Display top movies by average rating and number of ratings
    top_avg_rating = merged_df.groupby('title').filter(lambda x: len(x) >= 5).groupby('title')['rating'].mean().nlargest(5)
    top_num_ratings = merged_df.groupby('title').size().nlargest(5)
    print("\nTop 5 Movie Titles by Average Rating:\n", top_avg_rating)
    print("\nTop 5 Movie Titles by Number of Ratings:\n", top_num_ratings)

    # Determine and print the count of unique rater IDs
    num_unique_raters = merged_df['rater_id'].nunique()
    print("\nNumber of Unique Raters:", num_unique_raters)

    # Top 5 Rater IDs based on most movies rated and highest average rating
    top_raters_most_movies = merged_df['rater_id'].value_counts().nlargest(5)
    top_raters_highest_avg_rating = merged_df.groupby('rater_id').filter(lambda x: len(x) >= 5).groupby('rater_id')['rating'].mean().nlargest(5)
    print("\nTop 5 Rater IDs by Most Movies Rated:\n", top_raters_most_movies)
    print("\nTop 5 Rater IDs by Highest Average Rating:\n", top_raters_highest_avg_rating)

    # Top-rated movie for Michael Bay, Comedy, 2013, and India
    top_rated_movie = merged_df[
        (merged_df['director'] == 'Michael Bay') 
    ].nlargest(1, 'rating')
    print("\nTop Rated Movie of Michael Bay:\n", top_rated_movie[['title', 'director', 'genre', 'release_year', 'country', 'rating']])

    top_rated_movie = merged_df[
        (merged_df['genre'] == 'Comedy')
    ].nlargest(1, 'rating')
    print("\nTop Rated Comedy Movie:\n", top_rated_movie[['title', 'director', 'genre', 'release_year', 'country', 'rating']])

    top_rated_movie = merged_df[
        (merged_df['release_year'] == 2013)
    ].nlargest(1, 'rating')
    print("\nTop Rated Movie of 2013:\n", top_rated_movie[['title', 'director', 'genre', 'release_year', 'country', 'rating']])

    top_rated_movie = merged_df[
        (merged_df['country'] == 'India') 
    ].nlargest(1, 'rating')
    print("\nTop Rated Movie of India:\n", top_rated_movie[['title', 'director', 'genre', 'release_year', 'country', 'rating']])

    # Determine and print the favorite movie genre for Rater ID 1040
    rater_1040_favorite_genre = merged_df[merged_df['rater_id'] == 1040]['genre'].value_counts().idxmax()
    print("\nFavorite Movie Genre of Rater ID 1040:", rater_1040_favorite_genre)

    # Highest Average Rating for a Movie Genre by Rater ID 1040
    rater_1040_genre_ratings = merged_df[merged_df['rater_id'] == 1040]
    genre_avg_ratings_1040 = rater_1040_genre_ratings.groupby('genre')['rating'].mean()
    highest_avg_rating_genre_1040 = genre_avg_ratings_1040.max()
    print("\nHighest Average Rating for a Movie Genre by Rater ID 1040:", highest_avg_rating_genre_1040)

    # Year with Second-Highest Number of Action Movies
    second_highest_action_year = merged_df[
        (merged_df['genre'] == 'Action') &
        (merged_df['country'] == 'USA') &
        (merged_df['duration'] < 120)
    ].groupby('release_year').size().nlargest(2).index[1]
    print("\nYear with Second-Highest Number of Action Movies:", second_highest_action_year)

    merged_df['num_ratings'] = merged_df.groupby('movie_id')['rating'].transform('count')

    # Count of Movies with High Ratings
    high_rated_movies_count = merged_df[
        (merged_df['num_ratings'] >= 5) & 
        (merged_df['rating'] >= 7)
    ].shape[0]
    print("\nCount of Movies with High Ratings:", high_rated_movies_count)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the session
    session.close()
