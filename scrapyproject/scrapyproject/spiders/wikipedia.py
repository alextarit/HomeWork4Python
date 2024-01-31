from typing import Iterable
import scrapy
from scrapy.http import Request
import regex as re


class WikipediaSpider(scrapy.Spider):
    name = "wikipedia"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B0%D0%BC_%D0%B8_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC"]

    def parse(self, response):
        category_countries = response.css("div.CategoryTreeItem > a::attr(href)").getall()[1:]
        for country in category_countries:
            yield scrapy.Request(url="https://ru.wikipedia.org" + country, callback=self.parse_level_1)

    def parse_level_1(self, response):
        category_year = response.css("div.CategoryTreeItem > a::attr(href)").getall()
        for year in category_year:
            yield scrapy.Request(url="https://ru.wikipedia.org" + year, callback=self.parse_level_2)

    def parse_level_2(self, response):
        category_item = response.css("#mw-pages ul > li > a::attr(href)").getall()
        for item in category_item:
            yield  scrapy.Request(url="https://ru.wikipedia.org" + item, callback=self.parse_level_3)
    
    def parse_level_3(self, response):
        if not response.css("#firstHeading > span::text"):
            self.logger.warning(f"No content found on page: {response.url}")
            return
    
        title = response.css("#firstHeading > span::text").getall()

        genres_with_links = response.css("th:contains('Жанр') ~ td span a::text").getall()
        genres_divided = ",".join(genres_with_links)
        genres_cleaned = re.sub(',+', ',', genres_divided)
        genres = re.sub(r'[^а-яА-Я, ]', '', genres_cleaned)
        genres = re.sub(',+', ',', genres)

        if not genres:
            genres = response.css("th:contains('Жанры') ~ td span.no-wikidata::text").get()
        if not genres:
            genres = response.css("th:contains('Жанр') ~ td span.no-wikidata::text").get()
        
        director = response.css("th:contains('Режиссёр') ~ td span a::text").get()
        if not director:
            director = response.css("th:contains('Режиссёры') ~ td span.no-wikidata::text").getall()


        year = response.css("th:contains('Год') ~ td a span.dtstart::text").re_first(r'\d{4}')
        if not year:
            year = response.css("th:contains('Год') + td::text").re_first(r'\d{4}')
        if not year:
            year = response.css("th:contains('Год') + td span.wikidata-snak span a::text").re_first(r'\d{4}')
        if not year:
            year = response.css("th:contains('Год') + td a::text").re_first(r'\d{4}')
        if not year:
            year = response.css("th:contains('Дата выхода') + td span a::text").re_first(r'\d{4}')
        if not year:
            year = response.css("th:contains('Дата выхода') ~ td span.no-wikidata::text").re_first(r'\d{4}')

        countries = response.css("th:contains('Страна') ~ td a span::text").getall()
        if not countries:
            countries = response.css("th:contains('Страна') ~ td span.country-name a::text").getall()
        if not countries:
            countries_elements = response.css("th:contains('Страны') ~ td span.no-wikidata a::text").getall()
            countries = [country.strip() for country in countries_elements]
        if not countries:
            countries_elements = response.css("th:contains('Страны') ~ td span.no-wikidata a span.wrap::text").getall()
            countries = [country.strip() for country in countries_elements]
        if not countries:
            countries_elements = response.css("th:contains('Страны') ~ td span.no-wikidata a span.wrap::text").getall()
            countries = [country.strip() for country in countries_elements]
        if not countries:
            countries_elements = response.css("th:contains('Страны') ~ td span.country-name a::text").getall()
            countries = [country.strip() for country in countries_elements]
        
        countries = ",".join(countries)
        countries = re.sub(r'[^а-яА-Я, ]', '', countries)
        countries = re.sub(',+', ',', countries)

       
        imdb_link = response.css("th:contains('IMDb') ~ td a.extiw[href*='imdb.com']::attr(href)").get()

        yield {"title": title,
               "genre": genres,
               "director": director,
               "year": year, 
               "country": countries,
               "imdb": imdb_link}
