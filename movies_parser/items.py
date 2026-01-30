# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviesParserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

import scrapy

class MovieItem(scrapy.Item):
    # Название фильма
    title = scrapy.Field()

    # Жанры
    genres = scrapy.Field()

    # Режиссёр
    director = scrapy.Field()

    # Страна
    country = scrapy.Field()

    # Год
    year = scrapy.Field()

    # Рейтинг IMDb (заполняется на втором шаге)
    imdb_rating = scrapy.Field()
