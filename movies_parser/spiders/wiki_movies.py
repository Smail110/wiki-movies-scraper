import re
import scrapy


class WikiMoviesSpider(scrapy.Spider):
    """
    Парсер фильмов с ru.wikipedia.org:

    Собирает из инфобокса:
    - Название
    - Жанр
    - Режиссёр
    - Страна
    - Год

    Результат сохраняется в CSV через настройку FEEDS в settings.py.
    """
    name = "wiki_movies"

    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    # ---------------- helpers ----------------

    def clean_join(self, text_list):
        """Склеиваем список текстов, чистим мусор и ссылки [1], [2] и т.п."""
        text = "\n".join(t.strip() for t in text_list if t and t.strip())
        text = re.sub(r"\[[^\]]*\]", "", text)     # удалить [1], [2], ...
        text = re.sub(r"[ \t]+", " ", text)        # лишние пробелы
        text = re.sub(r"\n+", "\n", text).strip()  # лишние переносы
        return text

    def get_infobox_td_text(self, response, th_contains_list, td_must_have_plainlist=False):
        """
        Достаём текст из инфобокса по заголовку (th) и соседней ячейке (td).
        """
        contains_expr = " or ".join(
            [f'contains(normalize-space(.), "{x}")' for x in th_contains_list]
        )

        td_xpath = f'//th[{contains_expr}]/following-sibling::td[1]'
        if td_must_have_plainlist:
            td_xpath += '[contains(@class,"plainlist")]'

        texts = response.xpath(
            td_xpath + '//text()[not(ancestor::style) and not(ancestor::script)]'
        ).getall()

        return self.clean_join(texts)

    def normalize_list_field(self, raw_text):
        """Нормализуем список (жанры/страны) в строку 'A, B, C'."""
        if not raw_text:
            return ""
        parts = re.split(r"[\n,;]+", raw_text)
        parts = [p.strip() for p in parts if p and p.strip()]
        parts = list(dict.fromkeys(parts))  # убрать дубли сохранив порядок
        return ", ".join(parts)

    def extract_year(self, raw_text):
        """Берём первое число формата YYYY из произвольной строки."""
        if not raw_text:
            return ""
        raw_text = raw_text.replace("\n", " ")
        m = re.search(r"(?<!\d)(\d{4})(?!\d)", raw_text)
        return m.group(1) if m else ""

    def first_two_words(self, text):
        """Берём первые 2 слова (как в твоей логике)."""
        if not text:
            return ""
        words = text.split()
        return " ".join(words[:2]) if words else ""

    # ---------------- spider logic ----------------

    def parse(self, response):
        """Парсим страницы категории и идём по ссылкам на фильмы."""
        self.logger.info(f"CATEGORY PAGE: {response.url}")

        # На страницах категорий элементы обычно лежат в блоке #mw-pages
        movie_links = response.xpath(
            '//*[@id="mw-pages"]//div[contains(@class,"mw-category")]//a/@href'
            ' | //*[@id="mw-pages"]//div[contains(@class,"mw-category-group")]//a/@href'
        ).getall()

        movie_links = list(dict.fromkeys(movie_links))
        self.logger.info(f"FOUND MOVIE LINKS: {len(movie_links)}")

        for href in movie_links:
            # пропускаем служебные страницы (Категория:, Файл:, Служебная:)
            if ":" in href:
                continue
            yield response.follow(href, callback=self.parse_movie, priority=0)

        # следующая страница категории
        next_page = response.xpath('//*[@id="mw-pages"]//a[@rel="next"]/@href').get()
        if not next_page:
            next_page = response.xpath(
                '//*[@id="mw-pages"]//a[contains(@href,"pagefrom=")]/@href'
            ).get()

        if next_page:
            self.logger.info(f"NEXT PAGE FOUND: {next_page}")
            yield response.follow(next_page, callback=self.parse, dont_filter=True, priority=100)
        else:
            self.logger.warning("NEXT PAGE NOT FOUND")

    def parse_movie(self, response):
        """Парсим страницу фильма и отдаём item."""
        title = response.css("span.mw-page-title-main::text").get()
        if not title:
            return

        # убираем уточнения в скобках
        title = re.sub(r"\s*\([^)]*\)", "", title).strip()

        genres_raw = self.get_infobox_td_text(
            response, th_contains_list=["Жанр", "Жанры"], td_must_have_plainlist=True
        )
        genres = self.normalize_list_field(genres_raw)

        directors_raw = self.get_infobox_td_text(
            response, th_contains_list=["Режиссёр", "Режиссер"], td_must_have_plainlist=True
        )
        director = self.first_two_words(directors_raw)

        countries_raw = self.get_infobox_td_text(
            response, th_contains_list=["Страна", "Страны"], td_must_have_plainlist=False
        )
        country = self.normalize_list_field(countries_raw)

        year_raw = self.get_infobox_td_text(
            response, th_contains_list=["Год", "Дата выхода", "Первый показ", "Премьера"], td_must_have_plainlist=False
        )
        year = self.extract_year(year_raw)

        yield {
            "title": title,
            "genres": genres,
            "director": director,
            "country": country,
            "year": year,
        }
