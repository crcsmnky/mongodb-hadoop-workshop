# Loading Dataset

## Options

- Partial data set slimmed down for testing
- Full data set from the original source

## Partial Data Set

### Requirements

- Download [MongoDB](http://www.mongodb.org/downloads)
- [Install MongoDB](http://docs.mongodb.org/manual/installation/)

### Importing Data

    $ tar zxf mlsmall.tar.gz
    $ mongorestore -d movielens mlsmall/

## Full Data Set

### Requirements

- Download and install [Python 2.7](https://www.python.org/downloads/)
- Install [PyMongo](http://api.mongodb.org/python/current/installation.html) (via pip or easy_install)
- Download [MongoDB](http://www.mongodb.org/downloads)
- [Install MongoDB](http://docs.mongodb.org/manual/installation/)
- Download the [MovieLens 10M](http://grouplens.org/datasets/movielens/) archive and unzip

### Importing Data

    $ python dataset/movielens.py [/path/to/movies.dat] [/path/to/ratings.dat]
