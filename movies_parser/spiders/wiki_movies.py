import re
import json
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
    -imdb
    Результат сохраняется в CSV через настройку FEEDS в settings.py.
    """
    name = "wiki_movies"

    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    # --- Заголовки для IMDb ---
    # IMDb иногда режет Scrapy по User-Agent, поэтому маскируемся под браузер.
    IMDB_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    # ---------------- helpers ----------------

    def clean_join(self, text_list):
        """
        Склеиваем список текстов в один текст:
        - убираем пробельный мусор
        - убираем ссылки вида [1], [2]
        - нормализуем переносы строк
        """
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
        """
        Превращаем текстовый список в "A, B, C":
        - режем по переносам/запятым/точкам с запятой
        - убираем дубли
        """
        if not raw_text:
            return ""
        parts = re.split(r"[\n,;]+", raw_text)
        parts = [p.strip() for p in parts if p and p.strip()]
        parts = list(dict.fromkeys(parts))  # убрать дубли сохранив порядок
        return ", ".join(parts)

    def extract_year(self, raw_text):
        """
        Год может быть в разных форматах:
        - "1999"
        - "7 апреля 2003"
        - "Премьера: 12 октября 2012"
        Поэтому просто вытаскиваем первое число из 4 цифр.
        """
        if not raw_text:
            return ""
        raw_text = raw_text.replace("\n", " ")
        m = re.search(r"(?<!\d)(\d{4})(?!\d)", raw_text)
        return m.group(1) if m else ""

    def first_two_words(self, text):
        """
        Иногда в режиссёрах бывает длинная строка.
        """
        if not text:
            return ""
        words = text.split()
        return " ".join(words[:2]) if words else ""

    # -------- IMDb helpers --------

    def get_imdb_url(self, response):
        """
        Ищем строку IMDb в инфобоксе и берём внешнюю ссылку из следующего td.
        """
        imdb_url = response.xpath(
            '//th[contains(@class,"plainlist") and ('
            '   .//a[contains(normalize-space(.),"IMDb")] '
            '   or contains(normalize-space(.),"IMDb") '
            '   or .//a[contains(@href,"Internet_Movie_Database")]'
            ')]'
            '/following-sibling::td[1]'
            '//a[starts-with(@href,"http")]/@href'
        ).get()
        return imdb_url


    def _flatten_jsonld(self, obj):
        """
        IMDb JSON-LD иногда хранит данные внутри @graph.
        Эта функция превращает JSON-LD в плоский список объектов dict,
        чтобы легче было искать aggregateRating.
        """
        out = []
        if isinstance(obj, list):
            for x in obj:
                out.extend(self._flatten_jsonld(x))
            return out
        if isinstance(obj, dict):
            out.append(obj)
            g = obj.get("@graph")
            if isinstance(g, list):
                for x in g:
                    out.extend(self._flatten_jsonld(x))
        return out

    # ---------------- spider logic ----------------

    def parse(self, response):
        """
        Парсим категорию Википедии.
        Важно: на страницах категории элементы обычно лежат в блоке #mw-pages.
        Поэтому ссылки достаём оттуда (так надёжнее).
        """
        self.logger.info(f"CATEGORY PAGE: {response.url}")

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

        # переход на следующую страницу категории
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
        """
        Парсим страницу конкретного фильма в Википедии.
        """
        title = response.css("span.mw-page-title-main::text").get()
        if not title:
            return

        # убираем уточнения в скобках: "Титаник (фильм, 1997)" -> "Титаник"
        title = re.sub(r"\s*\([^)]*\)", "", title).strip()

        # --- ВАЖНО: оставляем твои слова как есть ---
        genres_raw = self.get_infobox_td_text(
            response, th_contains_list=["Жанр", "Жанры"], td_must_have_plainlist=True
        )
        genres = self.normalize_list_field(genres_raw)

        directors_raw = self.get_infobox_td_text(
            response, th_contains_list=["Режиссёр", "Режиссер"], td_must_have_plainlist=True
        )
        directors = self.first_two_words(directors_raw)

        countries_raw = self.get_infobox_td_text(
            response, th_contains_list=["Страна", "Страны"], td_must_have_plainlist=False
        )
        countries = self.normalize_list_field(countries_raw)

        year_raw = self.get_infobox_td_text(
            response, th_contains_list=["Год", "Дата выхода", "Первый показ", "Премьера"], td_must_have_plainlist=False
        )
        year = self.extract_year(year_raw)

        imdb_url = self.get_imdb_url(response)


        # Формируем запись (именно эти поля пойдут в CSV)
        item = {
            "title": title,
            "genres": genres,
            "director": directors,
            "country": countries,
            "year": year,
            "imdb_rating": "",  # заполним после IMDb (если найдём)
        }

        # Если есть IMDb — идём туда за рейтингом
        if imdb_url:
            yield scrapy.Request(
                url=imdb_url,
                callback=self.parse_imdb,
                headers=self.IMDB_HEADERS,
                meta={"item": item},
                dont_filter=True,
                priority=10,
            )
        else:
            yield item

    def parse_imdb(self, response):
        """
        Парсим страницу IMDb и достаём ratingValue из JSON-LD.
        """
        item = response.meta["item"]

        title = response.xpath("//title/text()").get() or ""
        self.logger.info(
            f"[IMDb] status={response.status} len={len(response.text)} url={response.url} title={title[:90]!r}"
        )

        # Иногда IMDb редиректит на consent/gdpr
        if "consent" in response.url.lower() or "gdpr" in response.url.lower():
            self.logger.warning(f"[IMDb] CONSENT/GDPR page: {response.url}")

        rating = ""

        # 1) JSON-LD (+ @graph)
        json_lds = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for raw in json_lds:
            raw = (raw or "").strip()
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                # если JSON кривой — ищем ratingValue regex'ом прямо в тексте блока
                m = re.search(r'"ratingValue"\s*:\s*([0-9]+(?:\.[0-9]+)?)', raw)
                if m:
                    rating = m.group(1)
                    break
                continue

            # раскладываем @graph / list / dict в плоский список
            for obj in self._flatten_jsonld(data):
                if not isinstance(obj, dict):
                    continue
                ag = obj.get("aggregateRating")
                if isinstance(ag, dict) and ag.get("ratingValue") is not None:
                    rating = str(ag["ratingValue"]).strip()
                    break
            if rating:
                break

        # 2) fallback по всей странице (если вдруг JSON-LD не нашли)
        if not rating:
            m = re.search(r'"ratingValue"\s*:\s*([0-9]+(?:\.[0-9]+)?)', response.text)
            if m:
                rating = m.group(1)

        item["imdb_rating"] = rating

        yield item
