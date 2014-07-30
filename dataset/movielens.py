"""

usage: python movielens-10m100k.py [movies.dat] [ratings.dat]

"""

from pymongo import MongoClient
from datetime import datetime
import sys
import re

def import_movies(db, fmovies):
    regex = re.compile("(?P<movieid>[0-9]+)::(?P<title>.+?) \((?P<year>\d{4})\)::(?P<genres>.+?)\n")

    count = 0
    movies = []
    for line in fmovies:
        match = regex.search(line)
        groups = match.groupdict()
        movie = {
            'movieid': int(groups['movieid']),
            'title': groups['title'],
            'year': int(groups['year']),
            'genres': groups['genres'].split('|')
        }
        movies.append(movie)
        count += 1

        if (count % 1000) == 0:
            # print count, "movies inserted"
            db.command('insert', 'movies', documents=movies, ordered=False)
            movies = []



def import_ratings(db, fratings):
    regex = re.compile("(?P<userid>[0-9]+)::(?P<movieid>[0-9]+)::(?P<rating>.*?)::(?P<ts>.*?)\n")

    count = 0
    ratings, movies, users = [], [], []
    for line in fratings:
        match = regex.search(line)
        groups = match.groupdict()
        rating = {
            'movieid': int(groups['movieid']),
            'userid': int(groups['userid']),
            'rating': float(groups['rating']),
            'ts': datetime.fromtimestamp(float(groups['ts']))
        }
        ratings.append(rating)

        movie_update = {
            'q': { 'movieid': int(groups['movieid']) },
            'u': { '$inc': {
                    'ratings' : 1,
                    'total_rating': float(groups['rating'])
                }
            }
        }
        movies.append(movie_update)

        user_update = {
            'q': { 'userid' : int(groups['userid']) },
            'u': { '$inc': { 'ratings': 1 } },
            'upsert': True
        }
        users.append(user_update)

        count += 1

        if (count % 1000) == 0:
            # print count, "ratings inserted, movies updated, users updated"
            db.command('insert', 'ratings', documents=ratings, ordered=False)
            db.command('update', 'movies', updates=movies, ordered=False)
            db.command('update', 'users', updates=users, ordered=False)
            ratings, movies, users = [], [], []


def main():

    db = MongoClient()['movielens']

    fmovies = open(sys.argv[1])
    fratings = open(sys.argv[2])

    import_movies(db, fmovies)
    import_ratings(db, fratings)

if __name__ == '__main__':
    main()
