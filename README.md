## Wiki Movies Scrapy Parser

Парсер на Scrapy, который собирает данные о фильмах из русской Википедии и сохраняет результат в CSV.

### Собираемые поля
- title
- genres
- director
- country
- year

### Установка
```bash
pip install -r requirements.txt
````
### Запуск
```
scrapy crawl wiki_movies
```
## `requirements.txt`

```txt
Scrapy==2.14.1
```
### Пример результата

Пример сформированного файла с результатами парсинга находится в:

`movies_parser/movies.csv`
