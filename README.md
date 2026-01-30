## Wiki Movies Scrapy Parser + IMDb

Парсер на **Scrapy**, который:

- собирает данные о фильмах из **Википедии**;
- при наличии ссылки на **IMDb** переходит на страницу фильма и извлекает **рейтинг IMDb**;
- сохраняет результат в формате **CSV**.

---

### Собираемые поля

- `title` — название фильма  
- `genres` — жанры  
- `director` — режиссёр  
- `country` — страна  
- `year` — год выпуска  
- `imdb_rating` — рейтинг IMDb  

---

### Установка

```bash
pip install -r requirements.txt
```
---
### Запуск
```
scrapy crawl wiki_movies
```
---
### requirements.txt
```
Scrapy==2.14.1
```
### Результат
После запуска будет создан файл:
```
movies.csv
```