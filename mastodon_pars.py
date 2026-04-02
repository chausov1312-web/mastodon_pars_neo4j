"""
Mastodon Parser - загрузка постов из Mastodon в графовую БД Neo4j

Структура графа:
    (Mastodon_Account)-[:POSTED]->(Mastodon_Post)
    (Mastodon_Account)-[:FOLLOWS]->(Mastodon_Account)
    (Mastodon_Post)-[:MENTIONS]->(Mastodon_Account)
    (Mastodon_Post)-[:TAGS]->(Hashtag)
    (Mastodon_Post)-[:REBLOG_OF]->(Mastodon_Post)
    (Mastodon_Post)-[:REPLY_TO]->(Mastodon_Post)
    (Text)-[:part]->(Mastodon_Post)
    (URI)-[:part]->(Mastodon_Post)
    (Domain)-[:rel]->(URI)
    (Photo|Video|Audio|File)-[:part]->(Mastodon_Post)
"""

import asyncio
import datetime
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

import aiohttp
from mastodon import Mastodon
from neo4j import GraphDatabase


# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

@dataclass
class Config:
    """Конфигурация приложения"""
    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = ""

    # Mastodon
    MASTODON_INSTANCE: str = "https://mastodon.social"
    ACCESS_TOKEN: str = ""

    # Параметры загрузки
    MAX_PAGES: int = 0  # 0 = без ограничений
    MAX_POSTS: int = 0  # 0 = без ограничений
    MIN_DATE: Optional[datetime.datetime] = None

    # Аккаунты для парсинга
    ACCOUNTS: List[str] = None
    PRIOR: int = 1

    # Лимиты для подписок/подписчиков (ЖЁСТКИЕ)
    MAX_FOLLOWING: int = 50   # Макс. количество подписок (0 = без ограничений)
    MAX_FOLLOWERS: int = 50   # Макс. количество подписчиков (0 = без ограничений)

    # Лимиты API для запросов
    API_LIMIT_FOLLOWING: int = 80  # Лимит за раз для following (макс 80)
    API_LIMIT_FOLLOWERS: int = 80  # Лимит за раз для followers (макс 80)
    API_LIMIT_POSTS: int = 40      # Лимит за раз для постов (макс 40)

    # Режим загрузки подписок/подписчиков
    FOLLOW_MODE: str = "sequential"  # "sequential" - последовательный, "async" - асинхронный

    # Логи
    LOG_LEVEL: int = logging.INFO
    LOG_FILE: str = "mastodon_pars.log"

    def __post_init__(self):
        if self.ACCOUNTS is None:
            self.ACCOUNTS = []


# =============================================================================
# ЛОГГЕР
# =============================================================================

class Logger:
    """Класс для логгирования событий"""
    
    # Символы для разных типов сообщений
    ICONS = {
        'info': 'ℹ',
        'success': '✓',
        'warning': '⚠',
        'error': '✗',
        'start': '▶',
        'end': '◀',
        'process': '⚙',
        'data': '📊',
    }
    
    def __init__(self, name: str, log_file: Optional[str] = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()
        
        # Консольный обработчик
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_format = logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # Файловый обработчик
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)
    
    def _log(self, icon_key: str, message: str, level: int = logging.INFO):
        icon = self.ICONS.get(icon_key, '•')
        self.logger.log(level, f"{icon} {message}")
    
    def info(self, message: str):
        self._log('info', message)
    
    def success(self, message: str):
        self._log('success', message)
    
    def warning(self, message: str):
        self._log('warning', message, logging.WARNING)
    
    def error(self, message: str):
        self._log('error', message, logging.ERROR)
    
    def start(self, message: str):
        self._log('start', f"НАЧАЛО: {message}")
    
    def end(self, message: str):
        self._log('end', f"ЗАВЕРШЕНИЕ: {message}")
    
    def process(self, message: str):
        self._log('process', message)
    
    def data(self, message: str):
        self._log('data', message)
    
    def separator(self, char: str = "=", length: int = 60):
        self.logger.info(char * length)


# =============================================================================
# МЕНЕДЖЕР NEO4J
# =============================================================================

class Neo4jManager:
    """Менеджер подключений и запросов к Neo4j"""
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._verify_connection()
    
    def _verify_connection(self):
        """Проверка подключения к БД"""
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к Neo4j: {e}")
    
    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        """Выполнение Cypher-запроса"""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return result
    
    def execute_write(self, query: str, params: Optional[Dict] = None) -> Any:
        """Выполнение Cypher-запроса на запись"""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return result.consume()
    
    def clear_database(self):
        """Очистка базы данных от всех узлов и связей"""
        query = "MATCH (n) DETACH DELETE n"
        with self.driver.session() as session:
            result = session.run(query)
            summary = result.consume()
            deleted_nodes = summary.counters.nodes_deleted
            deleted_relationships = summary.counters.relationships_deleted
        return deleted_nodes, deleted_relationships

    def get_stats(self) -> Dict[str, int]:
        """Получение статистики по узлам графа"""
        labels = ['Mastodon_Account', 'Mastodon_Post', 'Text', 'URI', 'Domain',
                  'Hashtag', 'Photo', 'Video', 'Audio', 'File']
        stats = {}
        with self.driver.session() as session:
            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                records = list(result)
                stats[label] = records[0]['count'] if records else 0
        return stats

    def get_accounts_with_prior(self, prior: int = 1) -> List[Dict]:
        """Получение списка аккаунтов с указанным prior"""
        query = "MATCH (a:Mastodon_Account) WHERE a.prior = $prior RETURN a.id, a.last_post_id"
        with self.driver.session() as session:
            result = session.run(query, {'prior': prior})
            records = list(result)  # Преобразуем в список перед итерацией
            return [{'id': r['a.id'], 'last_post_id': r['a.last_post_id'] or 0} for r in records]

    def close(self):
        """Закрытие подключения"""
        self.driver.close()


# =============================================================================
# MASTODON API КЛИЕНТ
# =============================================================================

class MastodonClient:
    """Клиент для работы с Mastodon API"""

    def __init__(self, instance: str, access_token: str, 
                 api_limit_following: int = 80, 
                 api_limit_followers: int = 80,
                 api_limit_posts: int = 40):
        self.api = Mastodon(access_token=access_token, api_base_url=instance)
        self.api_limit_following = api_limit_following
        self.api_limit_followers = api_limit_followers
        self.api_limit_posts = api_limit_posts
        self._verify_connection()

    def _verify_connection(self):
        """Проверка подключения к API"""
        try:
            self.api.instance()
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к Mastodon API: {e}")

    def get_account(self, username: str) -> Optional[Dict]:
        """Получение информации об аккаунте"""
        try:
            clean_username = username.strip('@')
            if clean_username.startswith('http'):
                clean_username = clean_username.rstrip('/').split('/')[-1]
            return self.api.account_lookup(clean_username)
        except Exception as e:
            return None

    def get_account_statuses(self, account_id: int, max_id: Optional[int] = None,
                             limit: int = None) -> List[Dict]:
        """Получение постов аккаунта"""
        if limit is None:
            limit = self.api_limit_posts
        try:
            return self.api.account_statuses(
                account_id,
                only_media=False,
                exclude_replies=False,
                exclude_reblogs=False,
                limit=limit,
                max_id=max_id
            )
        except Exception as e:
            raise Exception(f"Ошибка при получении постов: {e}")

    def get_account_following(self, account_id: int, limit: int = None) -> List[Dict]:
        """Получение списка подписок аккаунта"""
        if limit is None:
            limit = self.api_limit_following
        try:
            return self.api.account_following(account_id, limit=limit)
        except Exception as e:
            raise Exception(f"Ошибка при получении подписок: {e}")

    def get_account_followers(self, account_id: int, limit: int = None) -> List[Dict]:
        """Получение списка подписчиков аккаунта"""
        if limit is None:
            limit = self.api_limit_followers
        try:
            return self.api.account_followers(account_id, limit=limit)
        except Exception as e:
            raise Exception(f"Ошибка при получении подписчиков: {e}")

    def get_status_context(self, status_id: int) -> Dict:
        """Получение контекста поста (родители и дети в треде)"""
        try:
            return self.api.status_context(status_id)
        except Exception as e:
            raise Exception(f"Ошибка при получении контекста поста: {e}")

    def get_status(self, status_id: int) -> Dict:
        """Получение информации о посте по ID"""
        try:
            return self.api.status(status_id)
        except Exception as e:
            raise Exception(f"Ошибка при получении поста: {e}")


# =============================================================================
# АСИНХРОННЫЙ MASTODON API КЛИЕНТ
# =============================================================================

class AsyncMastodonClient:
    """Асинхронный клиент для работы с Mastodon API"""

    def __init__(self, instance: str, access_token: str):
        self.instance = instance.rstrip('/')
        self.access_token = access_token
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {'Authorization': f'Bearer {access_token}'}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Асинхронный HTTP запрос к API"""
        url = f"{self.instance}/api/v1/{endpoint}"
        async with self.session.request(method, url, params=params) as response:
            response.raise_for_status()
            return await response.json()

    async def get_account(self, account_id: int) -> Dict:
        """Получение информации об аккаунте по ID"""
        return await self._request('GET', f'accounts/{account_id}')

    async def get_account_following(self, account_id: int, max_id: Optional[int] = None, limit: int = 80) -> List[Dict]:
        """Получение списка подписок аккаунта"""
        params = {'limit': limit}
        if max_id:
            params['max_id'] = max_id
        return await self._request('GET', f'accounts/{account_id}/following', params)

    async def get_account_followers(self, account_id: int, max_id: Optional[int] = None, limit: int = 80) -> List[Dict]:
        """Получение списка подписчиков аккаунта"""
        params = {'limit': limit}
        if max_id:
            params['max_id'] = max_id
        return await self._request('GET', f'accounts/{account_id}/followers', params)

    async def get_account_statuses(self, account_id: int, max_id: Optional[int] = None, limit: int = 40) -> List[Dict]:
        """Получение постов аккаунта"""
        params = {'limit': limit, 'exclude_replies': False, 'only_media': False}
        if max_id:
            params['max_id'] = max_id
        return await self._request('GET', f'accounts/{account_id}/statuses', params)

    async def get_status_context(self, status_id: int) -> Dict:
        """Получение контекста поста"""
        return await self._request('GET', f'statuses/{status_id}/context')

    async def get_status(self, status_id: int) -> Dict:
        """Получение информации о посте"""
        return await self._request('GET', f'statuses/{status_id}')

    async def paginate_list(self, account_id: int, list_type: str, max_concurrent: int = 5) -> List[Dict]:
        """
        Асинхронная пагинация списка подписок или подписчиков.
        Загружает все страницы параллельно пачками.
        """
        all_items = []
        max_id = None
        page_count = 0

        while True:
            # Загружаем пачку страниц параллельно
            tasks = []
            for _ in range(max_concurrent):
                if list_type == 'following':
                    tasks.append(self.get_account_following(account_id, max_id=max_id, limit=80))
                else:
                    tasks.append(self.get_account_followers(account_id, max_id=max_id, limit=80))

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                break

            # Обрабатываем результаты
            has_more = False
            for result in results:
                if isinstance(result, list) and result:
                    all_items.extend(result)
                    max_id = result[-1]['id']
                    has_more = True
                    page_count += 1
                else:
                    has_more = False
                    break

            if not has_more:
                break

        return all_items


# =============================================================================
# ПОСЛЕДОВАТЕЛЬНЫЙ КЛИЕНТ ДЛЯ ЗАГРУЗКИ ПОДПИСОК/ПОДПИСЧИКОВ
# =============================================================================

class SequentialMastodonClient:
    """
    Последовательный клиент для загрузки подписок и подписчиков.
    Использует надёжный режим с пагинацией (~1 запрос/сек).
    """

    def __init__(self, instance: str, access_token: str, page_limit: int = 80):
        self.instance = instance.rstrip('/')
        self.access_token = access_token
        self.page_limit = page_limit
        self.headers = {'Authorization': f'Bearer {access_token}'}
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _fetch_page(self, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> tuple:
        """
        Получает одну страницу подписчиков/подписок с повторными попытками.
        Возвращает (data, next_url).
        """
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    # Обработка rate limiting
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After", "5")
                        wait_time = int(retry_after) if retry_after.isdigit() else 5
                        wait_time = max(1, min(wait_time, 300))
                        await asyncio.sleep(wait_time)
                        continue

                    response.raise_for_status()
                    data = await response.json()

                    # Парсим заголовок Link для следующей страницы
                    next_url = None
                    link_header = response.headers.get("Link", "")
                    if link_header:
                        for part in link_header.split(","):
                            if 'rel="next"' in part or "rel='next'" in part:
                                next_url = part.split(";")[0].strip().strip("<>")
                                break

                    return data, next_url

            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    raise Exception(f"Ошибка после {max_retries} попыток: {e}")
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    raise Exception("Таймаут запроса")

        return None, None

    async def get_all_following(self, account_id: int, max_limit: int = 0,
                                 log_func: Optional[callable] = None) -> List[Dict]:
        """
        Последовательная загрузка всех подписок аккаунта.
        max_limit - максимальное количество (0 = без ограничений).
        """
        all_following = []
        seen_ids = set()

        url = f"{self.instance}/api/v1/accounts/{account_id}/following"
        params = {"limit": self.page_limit}
        page = 0
        start_time = time.time()

        while url and (max_limit == 0 or len(all_following) < max_limit):
            page += 1
            try:
                following, next_url = await self._fetch_page(url, params)

                if not following:
                    break

                # Фильтруем дубликаты
                new_following = []
                for f in following:
                    fid = f["id"]
                    if fid not in seen_ids:
                        seen_ids.add(fid)
                        if max_limit == 0 or len(all_following) < max_limit:
                            new_following.append(f)
                            all_following.append(f)

                elapsed = time.time() - start_time
                if log_func:
                    log_func(f"Страница {page}: +{len(new_following)} (всего: {len(all_following)}) [{elapsed:.1f}s]")

                # Переходим к следующей странице
                url = next_url
                params = None

                # Задержка для rate limiting (API: 300 запросов/15мин)
                await asyncio.sleep(1.0)

            except Exception as e:
                if log_func:
                    log_func(f"Ошибка: {e}")
                break

        return all_following

    async def get_all_followers(self, account_id: int, max_limit: int = 0,
                                 log_func: Optional[callable] = None) -> List[Dict]:
        """
        Последовательная загрузка всех подписчиков аккаунта.
        max_limit - максимальное количество (0 = без ограничений).
        """
        all_followers = []
        seen_ids = set()

        url = f"{self.instance}/api/v1/accounts/{account_id}/followers"
        params = {"limit": self.page_limit}
        page = 0
        start_time = time.time()

        while url and (max_limit == 0 or len(all_followers) < max_limit):
            page += 1
            try:
                followers, next_url = await self._fetch_page(url, params)

                if not followers:
                    break

                # Фильтруем дубликаты
                new_followers = []
                for f in followers:
                    fid = f["id"]
                    if fid not in seen_ids:
                        seen_ids.add(fid)
                        if max_limit == 0 or len(all_followers) < max_limit:
                            new_followers.append(f)
                            all_followers.append(f)

                elapsed = time.time() - start_time
                if log_func:
                    log_func(f"Страница {page}: +{len(new_followers)} (всего: {len(all_followers)}) [{elapsed:.1f}s]")

                # Переходим к следующей странице
                url = next_url
                params = None

                # Задержка для rate limiting (API: 300 запросов/15мин)
                await asyncio.sleep(1.0)

            except Exception as e:
                if log_func:
                    log_func(f"Ошибка: {e}")
                break

        return all_followers


# =============================================================================
# ПАРСЕР ДАННЫХ
# =============================================================================

class DataParser:
    """Парсер для извлечения данных из контента"""
    
    @staticmethod
    def extract_uris_from_html(html_content: str) -> List[str]:
        """Извлечение URI из HTML-контента"""
        uris = []
        href_pattern = r'href=["\']([^"\']+)["\']'
        for match in re.finditer(href_pattern, html_content):
            url = match.group(1)
            if url.startswith('http'):
                uris.append(url)
        return uris
    
    @staticmethod
    def strip_html(html_content: str) -> str:
        """Очистка HTML-тегов из контента"""
        return re.sub(r'<[^>]+>', '', html_content)
    
    @staticmethod
    def get_domain(uri: str) -> Optional[str]:
        """Извлечение домена из URI"""
        return urlparse(uri).netloc


# =============================================================================
# ЗАГРУЗЧИК ДАННЫХ В NEO4J
# =============================================================================

class GraphLoader:
    """Загрузчик данных в графовую БД"""

    def __init__(self, db: Neo4jManager, logger: Logger, mastodon_client: Optional[MastodonClient] = None):
        self.db = db
        self.log = logger
        self.mastodon = mastodon_client
    
    def add_account(self, account: Dict, prior: int = 1) -> bool:
        """
        Добавление аккаунта в БД.
        Возвращает True, если аккаунт создан, False - если обновлён.
        """
        query = """
        MERGE (a:Mastodon_Account {id: $id})
        ON CREATE SET 
            a.dateC = datetime(),
            a.username = $username,
            a.display_name = $display_name,
            a.url = $url,
            a.created_at = datetime($created_at),
            a.followers_count = $followers_count,
            a.following_count = $following_count,
            a.statuses_count = $statuses_count,
            a.prior = $prior,
            a.last_post_id = 0
        ON MATCH SET 
            a.prior = $prior,
            a.dateM = datetime()
        """
        params = {
            'id': account['id'],
            'username': account['username'],
            'display_name': account['display_name'],
            'url': account['url'],
            'created_at': account['created_at'].isoformat(),
            'followers_count': account['followers_count'],
            'following_count': account['following_count'],
            'statuses_count': account['statuses_count'],
            'prior': prior
        }
        result = self.db.execute_write(query, params)
        return result.counters.nodes_created > 0
    
    def save_post(self, toot: Dict, check_prior: bool = True, target_prior: int = 1, 
                  create_account: bool = True) -> bool:
        """
        Сохранение поста в БД.
        check_prior - проверять prior автора перед созданием связи POSTED
        target_prior - целевой prior для проверки
        create_account - создавать узел аккаунта, если не существует
        Возвращает True, если пост создан, False - если обновлён.
        """
        with self.db.driver.session() as session:
            # Проверяем существование поста
            check_query = "MATCH (p:Mastodon_Post {id: $id}) RETURN p.dateC as dateC"
            check_result = session.run(check_query, {'id': toot['id']})
            check_records = list(check_result)
            check_record = check_records[0] if check_records else None
            is_new = check_record is None

            # Создаём/обновляем узел поста
            post_query = """
            MERGE (p:Mastodon_Post {id: $id})
            ON CREATE SET p.dateC = datetime()
            SET
                p.account_id = $account_id,
                p.created_at = datetime($created_at),
                p.url = $url,
                p.reblogs_count = $reblogs_count,
                p.favourites_count = $favourites_count,
                p.replies_count = $replies_count,
                p.sensitive = $sensitive,
                p.spoiler_text = $spoiler_text,
                p.in_reply_to_id = $in_reply_to_id,
                p.reblog_id = $reblog_id,
                p.dateM = datetime()
            """
            post_params = {
                'id': toot['id'],
                'account_id': toot['account']['id'],
                'created_at': toot['created_at'].isoformat(),
                'url': toot['url'],
                'reblogs_count': toot['reblogs_count'],
                'favourites_count': toot['favourites_count'],
                'replies_count': toot['replies_count'],
                'sensitive': toot['sensitive'],
                'spoiler_text': toot['spoiler_text'],
                'in_reply_to_id': toot['in_reply_to_id'],
                'reblog_id': toot['reblog']['id'] if toot.get('reblog') else None
            }
            session.run(post_query, post_params)

            # Сохраняем аккаунт автора (если не существует и create_account=True)
            if create_account:
                author = toot['account']
                author_query = """
                MERGE (a:Mastodon_Account {id: $account_id})
                ON CREATE SET
                    a.dateC = datetime(),
                    a.username = $username,
                    a.display_name = $display_name,
                    a.url = $url,
                    a.prior = 0
                """
                session.run(author_query, {
                    'account_id': author['id'],
                    'username': author['username'],
                    'display_name': author['display_name'],
                    'url': author['url']
                })

            # Связь с автором: (Account)-[:POSTED]->(Post)
            # Создаём связь только если автор имеет целевой prior
            if check_prior:
                author_prior_query = """
                MATCH (a:Mastodon_Account {id: $account_id})
                MATCH (p:Mastodon_Post {id: $post_id})
                WITH a, p
                WHERE a.prior = $target_prior
                MERGE (a)-[:POSTED]->(p)
                """
                session.run(author_prior_query, {
                    'account_id': toot['account']['id'],
                    'post_id': toot['id'],
                    'target_prior': target_prior
                })
            else:
                author_query = """
                MATCH (a:Mastodon_Account {id: $account_id})
                MATCH (p:Mastodon_Post {id: $post_id})
                MERGE (a)-[:POSTED]->(p)
                """
                session.run(author_query, {
                    'account_id': toot['account']['id'],
                    'post_id': toot['id']
                })

            # Текст поста: (Text)-[:part]->(Post)
            text_content = DataParser.strip_html(toot['content'])
            if text_content.strip():
                text_query = """
                MERGE (t:Text {descr: $text})
                ON CREATE SET t.dateC = datetime()
                WITH t
                MATCH (p:Mastodon_Post {id: $post_id})
                MERGE (t)-[:part]->(p)
                """
                session.run(text_query, {
                    'text': text_content[:8219],
                    'post_id': toot['id']
                })

            # URI и домены: (URI)-[:part]->(Post), (Domain)-[:rel]->(URI)
            uris = DataParser.extract_uris_from_html(toot['content'])
            for uri in uris:
                domain = DataParser.get_domain(uri)
                uri_query = """
                MERGE (u:URI {value: $uri})
                ON CREATE SET u.dateC = datetime()
                WITH u
                MATCH (p:Mastodon_Post {id: $post_id})
                MERGE (u)-[:part]->(p)
                """
                session.run(uri_query, {'uri': uri, 'post_id': toot['id']})

                if domain:
                    domain_query = """
                    MERGE (d:Domain {name: $domain})
                    ON CREATE SET d.dateC = datetime()
                    WITH d
                    MATCH (u:URI {value: $uri})
                    MERGE (d)-[:rel]->(u)
                    """
                    session.run(domain_query, {'domain': domain, 'uri': uri})

            # Упоминания: (Post)-[:MENTIONS]->(Account)
            for mention in toot['mentions']:
                mention_query = """
                MATCH (p:Mastodon_Post {id: $post_id})
                MATCH (a:Mastodon_Account {id: $account_id})
                MERGE (p)-[:MENTIONS]->(a)
                """
                session.run(mention_query, {
                    'post_id': toot['id'],
                    'account_id': mention['id']
                })

            # Хештеги: (Post)-[:TAGS]->(Hashtag)
            for tag in toot['tags']:
                tag_query = """
                MERGE (h:Hashtag {name: $name})
                ON CREATE SET h.dateC = datetime()
                WITH h
                MATCH (p:Mastodon_Post {id: $post_id})
                MERGE (p)-[:TAGS]->(h)
                """
                session.run(tag_query, {'name': tag['name'], 'post_id': toot['id']})

            # Медиафайлы по типам
            for media in toot['media_attachments']:
                self._save_media(session, media, toot['id'])

            # Репосты: (Post)-[:REBLOG_OF]->(OriginalPost)
            if toot.get('reblog'):
                reblog_query = """
                MATCH (p:Mastodon_Post {id: $post_id})
                MATCH (orig:Mastodon_Post {id: $reblog_id})
                MERGE (p)-[:REBLOG_OF]->(orig)
                """
                session.run(reblog_query, {
                    'post_id': toot['id'],
                    'reblog_id': toot['reblog']['id']
                })

            # Ответы: (Post)-[:REPLY_TO]->(ParentPost)
            if toot.get('in_reply_to_id'):
                reply_query = """
                MATCH (p:Mastodon_Post {id: $post_id})
                MATCH (parent:Mastodon_Post {id: $reply_to_id})
                MERGE (p)-[:REPLY_TO]->(parent)
                """
                session.run(reply_query, {
                    'post_id': toot['id'],
                    'reply_to_id': toot['in_reply_to_id']
                })

    def _save_media(self, session, media: Dict, post_id: int):
        """Сохранение медиафайла"""
        media_type = media['type']
        media_id = media['id']

        if media_type == 'image':
            label = 'Photo'
        elif media_type == 'video':
            label = 'Video'
        elif media_type == 'audio':
            label = 'Audio'
        else:
            label = 'File'

        media_query = f"""
        MERGE (m:{label} {{id: $id}})
        ON CREATE SET m.dateC = datetime()
        SET
            m.type = $media_type,
            m.url = $url,
            m.preview_url = $preview_url,
            m.description = $description,
            m.dateM = datetime()
        WITH m
        MATCH (p:Mastodon_Post {{id: $post_id}})
        MERGE (m)-[:part]->(p)
        """
        session.run(media_query, {
            'id': media_id,
            'media_type': media_type,
            'url': media['url'],
            'preview_url': media['preview_url'],
            'description': media.get('description', ''),
            'post_id': post_id
        })
    
    def save_posts_batch(self, toots: List[Dict], target_prior: int = 1) -> int:
        """
        Батчевое сохранение постов.
        Возвращает количество новых постов.
        """
        if not toots:
            return 0
            
        new_count = 0
        with self.db.driver.session() as session:
            for toot in toots:
                # Проверяем существование поста
                check_query = "MATCH (p:Mastodon_Post {id: $id}) RETURN p.id"
                check_result = session.run(check_query, {'id': toot['id']})
                is_new = not check_result.single()
                if is_new:
                    new_count += 1

                # Создаём/обновляем узел поста
                post_query = """
                MERGE (p:Mastodon_Post {id: $id})
                ON CREATE SET p.dateC = datetime()
                SET
                    p.account_id = $account_id,
                    p.created_at = datetime($created_at),
                    p.url = $url,
                    p.reblogs_count = $reblogs_count,
                    p.favourites_count = $favourites_count,
                    p.replies_count = $replies_count,
                    p.sensitive = $sensitive,
                    p.spoiler_text = $spoiler_text,
                    p.in_reply_to_id = $in_reply_to_id,
                    p.reblog_id = $reblog_id,
                    p.dateM = datetime()
                """
                session.run(post_query, {
                    'id': toot['id'],
                    'account_id': toot['account']['id'],
                    'created_at': toot['created_at'].isoformat(),
                    'url': toot['url'],
                    'reblogs_count': toot['reblogs_count'],
                    'favourites_count': toot['favourites_count'],
                    'replies_count': toot['replies_count'],
                    'sensitive': toot['sensitive'],
                    'spoiler_text': toot['spoiler_text'],
                    'in_reply_to_id': toot['in_reply_to_id'],
                    'reblog_id': toot['reblog']['id'] if toot.get('reblog') else None
                })

                # Сохраняем аккаунт автора
                author = toot['account']
                author_query = """
                MERGE (a:Mastodon_Account {id: $account_id})
                ON CREATE SET
                    a.dateC = datetime(),
                    a.username = $username,
                    a.display_name = $display_name,
                    a.url = $url,
                    a.prior = 0
                """
                session.run(author_query, {
                    'account_id': author['id'],
                    'username': author['username'],
                    'display_name': author['display_name'],
                    'url': author['url']
                })

                # Связь с автором (только для целевых аккаунтов)
                author_prior_query = """
                MATCH (a:Mastodon_Account {id: $account_id})
                MATCH (p:Mastodon_Post {id: $post_id})
                WITH a, p
                WHERE a.prior = $target_prior
                MERGE (a)-[:POSTED]->(p)
                """
                session.run(author_prior_query, {
                    'account_id': toot['account']['id'],
                    'post_id': toot['id'],
                    'target_prior': target_prior
                })

                # Текст поста
                text_content = DataParser.strip_html(toot['content'])
                if text_content.strip():
                    text_query = """
                    MERGE (t:Text {descr: $text})
                    ON CREATE SET t.dateC = datetime()
                    WITH t
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (t)-[:part]->(p)
                    """
                    session.run(text_query, {
                        'text': text_content[:8219],
                        'post_id': toot['id']
                    })

                # URI и домены
                uris = DataParser.extract_uris_from_html(toot['content'])
                for uri in uris:
                    domain = DataParser.get_domain(uri)
                    uri_query = """
                    MERGE (u:URI {value: $uri})
                    ON CREATE SET u.dateC = datetime()
                    WITH u
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (u)-[:part]->(p)
                    """
                    session.run(uri_query, {'uri': uri, 'post_id': toot['id']})

                    if domain:
                        domain_query = """
                        MERGE (d:Domain {name: $domain})
                        ON CREATE SET d.dateC = datetime()
                        WITH d
                        MATCH (u:URI {value: $uri})
                        MERGE (d)-[:rel]->(u)
                        """
                        session.run(domain_query, {'domain': domain, 'uri': uri})

                # Упоминания
                for mention in toot['mentions']:
                    mention_query = """
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (a:Mastodon_Account {id: $account_id})
                    ON CREATE SET a.username = $username, a.display_name = $display_name
                    MERGE (p)-[:MENTIONS]->(a)
                    """
                    session.run(mention_query, {
                        'post_id': toot['id'],
                        'account_id': mention['id'],
                        'username': mention.get('username', ''),
                        'display_name': mention.get('display_name', '')
                    })

                # Хештеги
                for tag in toot['tags']:
                    tag_query = """
                    MERGE (h:Hashtag {name: $name})
                    ON CREATE SET h.dateC = datetime()
                    WITH h
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (p)-[:TAGS]->(h)
                    """
                    session.run(tag_query, {'name': tag['name'], 'post_id': toot['id']})

                # Медиафайлы
                for media in toot['media_attachments']:
                    self._save_media_batch(session, media, toot['id'])

                # Репосты
                if toot.get('reblog'):
                    reblog_query = """
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (orig:Mastodon_Post {id: $reblog_id})
                    ON CREATE SET orig.dateC = datetime()
                    MERGE (p)-[:REBLOG_OF]->(orig)
                    """
                    session.run(reblog_query, {
                        'post_id': toot['id'],
                        'reblog_id': toot['reblog']['id']
                    })

                # Ответы
                if toot.get('in_reply_to_id'):
                    reply_query = """
                    MATCH (p:Mastodon_Post {id: $post_id})
                    MERGE (parent:Mastodon_Post {id: $reply_to_id})
                    ON CREATE SET parent.dateC = datetime()
                    MERGE (p)-[:REPLY_TO]->(parent)
                    """
                    session.run(reply_query, {
                        'post_id': toot['id'],
                        'reply_to_id': toot['in_reply_to_id']
                    })

        return new_count

    def _save_media_batch(self, session, media: Dict, post_id: int):
        """Сохранение медиафайла в рамках батча"""
        media_type = media['type']
        media_id = media['id']

        if media_type == 'image':
            label = 'Photo'
        elif media_type == 'video':
            label = 'Video'
        elif media_type == 'audio':
            label = 'Audio'
        else:
            label = 'File'

        media_query = f"""
        MERGE (m:{label} {{id: $id}})
        ON CREATE SET m.dateC = datetime()
        SET
            m.type = $media_type,
            m.url = $url,
            m.preview_url = $preview_url,
            m.description = $description,
            m.dateM = datetime()
        WITH m
        MATCH (p:Mastodon_Post {{id: $post_id}})
        MERGE (m)-[:part]->(p)
        """
        session.run(media_query, {
            'id': media_id,
            'media_type': media_type,
            'url': media['url'],
            'preview_url': media['preview_url'],
            'description': media.get('description', ''),
            'post_id': post_id
        })
    
    def update_account_last_post_id(self, account_id: int, last_post_id: int):
        """Обновление ID последнего загруженного поста"""
        query = """
        MATCH (a:Mastodon_Account {id: $id})
        SET a.last_post_id = $last_post_id, a.dateM = datetime()
        """
        with self.db.driver.session() as session:
            session.run(query, {'id': account_id, 'last_post_id': last_post_id})
    
    def get_account_info(self, account_id: int) -> Optional[Dict]:
        """Получение информации об аккаунте из БД"""
        query = "MATCH (a:Mastodon_Account {id: $id}) RETURN a.prior, a.username, a.last_post_id"
        with self.db.driver.session() as session:
            result = session.run(query, {'id': account_id})
            records = list(result)
            record = records[0] if records else None
            if record:
                return {
                    'prior': record['a.prior'],
                    'username': record['a.username'],
                    'last_post_id': record['a.last_post_id'] or 0
                }
        return None

    def is_target_account(self, account_id: int, prior: int = 1) -> bool:
        """Проверка, является ли аккаунт целевым (с указанным prior)"""
        info = self.get_account_info(account_id)
        return info is not None and info.get('prior') == prior
    
    def get_accounts_with_prior(self, prior: int = 1) -> List[Dict]:
        """Получение списка аккаунтов с указанным prior"""
        query = "MATCH (a:Mastodon_Account) WHERE a.prior = $prior RETURN a.id, a.last_post_id"
        with self.db.driver.session() as session:
            result = session.run(query, {'prior': prior})
            records = list(result)
            return [{'id': r['a.id'], 'last_post_id': r['a.last_post_id'] or 0} for r in records]

    def add_account_relationship(self, from_account_id: int, to_account_id: int, rel_type: str = 'FOLLOWS'):
        """
        Добавление связи между аккаунтами.
        rel_type: 'FOLLOWS' (from следует за to) или 'FOLLOWED_BY' (обратная связь)
        """
        query = """
        MATCH (a:Mastodon_Account {id: $from_id})
        MATCH (b:Mastodon_Account {id: $to_id})
        MERGE (a)-[r:FOLLOWS]->(b)
        ON CREATE SET r.dateC = datetime()
        """
        with self.db.driver.session() as session:
            session.run(query, {'from_id': from_account_id, 'to_id': to_account_id})

    def save_account_batch(self, accounts: List[Dict], prior: int = 1) -> Dict[str, int]:
        """
        Массовое сохранение аккаунтов в БД.
        Возвращает статистику: {'created': N, 'updated': N}
        """
        stats = {'created': 0, 'updated': 0}
        for account in accounts:
            is_created = self.add_account(account, prior)
            if is_created:
                stats['created'] += 1
            else:
                stats['updated'] += 1
        return stats

    def save_post_with_context(self, toot: Dict, context_ancestors: Optional[List[Dict]] = None, 
                                check_prior: bool = True, target_prior: int = 1,
                                create_account: bool = False) -> bool:
        """
        Сохранение поста с обработкой контекста (комментарии).
        context_ancestors - список родительских постов в треде (от старшего к ближайшему)
        create_account - создавать узел аккаунта для автора (по умолчанию False для комментариев)
        """
        is_new = self.save_post(toot, check_prior=check_prior, target_prior=target_prior, 
                                create_account=create_account)

        # Обрабатываем связь REPLY_TO, если есть контекст
        if context_ancestors and len(context_ancestors) > 0:
            # Последний элемент в ancestors - это непосредственный родитель
            parent_post = context_ancestors[-1]
            # Сохраняем родительский пост рекурсивно
            self.save_post_with_context(parent_post, context_ancestors[:-1], 
                                        check_prior=check_prior, target_prior=target_prior,
                                        create_account=create_account)
            # Создаём связь REPLY_TO
            with self.db.driver.session() as session:
                reply_query = """
                MATCH (p:Mastodon_Post {id: $post_id})
                MATCH (parent:Mastodon_Post {id: $parent_id})
                MERGE (p)-[:REPLY_TO]->(parent)
                """
                session.run(reply_query, {
                    'post_id': toot['id'],
                    'parent_id': parent_post['id']
                })

        return is_new

    def fetch_and_save_context(self, post_id: int, processed_posts: set, 
                                check_prior: bool = True, target_prior: int = 1) -> int:
        """
        Рекурсивная загрузка контекста (комментариев) для поста.
        processed_posts - множество уже обработанных ID постов (для избежания дублирования)
        check_prior - проверять prior автора перед созданием связи POSTED
        target_prior - целевой prior для проверки
        Возвращает количество загруженных постов.
        
        ВАЖНО: Аккаунты авторов комментариев НЕ создаются (create_account=False).
        Создаются только посты и связи REPLY_TO.
        """
        loaded_count = 0

        try:
            context = self.mastodon.get_status_context(post_id)
        except Exception as e:
            self.log.warning(f"Не удалось получить контекст для поста {post_id}: {e}")
            return 0

        ancestors = context.get('ancestors', [])
        descendants = context.get('descendants', [])

        # Сначала обрабатываем ancestors (родительские комментарии) рекурсивно
        for ancestor in ancestors:
            if ancestor['id'] not in processed_posts:
                processed_posts.add(ancestor['id'])
                # Сохраняем пост БЕЗ создания аккаунта (create_account=False)
                self.save_post_with_context(ancestor, [], check_prior=check_prior, 
                                           target_prior=target_prior, create_account=False)
                loaded_count += 1
                # Рекурсивно загружаем контекст для этого ancestor
                loaded_count += self.fetch_and_save_context(ancestor['id'], processed_posts, 
                                                            check_prior=check_prior, target_prior=target_prior)

        # Затем обрабатываем descendants (дочерние комментарии)
        for descendant in descendants:
            if descendant['id'] not in processed_posts:
                processed_posts.add(descendant['id'])
                # Сохраняем пост БЕЗ создания аккаунта (create_account=False)
                self.save_post(descendant, check_prior=check_prior, target_prior=target_prior, 
                              create_account=False)
                loaded_count += 1
                # Рекурсивно загружаем контекст для этого descendant
                loaded_count += self.fetch_and_save_context(descendant['id'], processed_posts, 
                                                            check_prior=check_prior, target_prior=target_prior)

        return loaded_count


# =============================================================================
# ОСНОВНОЙ КЛАСС ПРИЛОЖЕНИЯ
# =============================================================================

class MastodonParser:
    """Основной класс приложения"""

    def __init__(self, config: Config):
        self.config = config
        self.log = Logger("MastodonParser", log_file=config.LOG_FILE, level=config.LOG_LEVEL)
        self.db = Neo4jManager(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD)
        self.mastodon = MastodonClient(
            config.MASTODON_INSTANCE,
            config.ACCESS_TOKEN,
            api_limit_following=config.API_LIMIT_FOLLOWING,
            api_limit_followers=config.API_LIMIT_FOLLOWERS,
            api_limit_posts=config.API_LIMIT_POSTS
        )
        self.async_mastodon = None  # Инициализируется в async_run()
        self.sequential_client = None  # Инициализируется в async_run()
        self.loader = GraphLoader(self.db, self.log, self.mastodon)
        self.processed_accounts = set()
        self.processed_posts = set()

    async def _init_async_client(self):
        """Инициализация асинхронного клиента"""
        if self.async_mastodon is None:
            self.async_mastodon = AsyncMastodonClient(
                self.config.MASTODON_INSTANCE,
                self.config.ACCESS_TOKEN
            )
            await self.async_mastodon.__aenter__()
        
        if self.sequential_client is None:
            self.sequential_client = SequentialMastodonClient(
                self.config.MASTODON_INSTANCE,
                self.config.ACCESS_TOKEN,
                page_limit=self.config.API_LIMIT_FOLLOWING
            )
            await self.sequential_client.__aenter__()
    
    def add_accounts(self, usernames: List[str], prior: int = 1) -> tuple:
        """Добавление аккаунтов в БД"""
        self.log.start(f"Добавление аккаунтов: {usernames}")
        added_count = 0
        updated_count = 0

        for username in usernames:
            self.log.process(f"Получение информации об аккаунте @{username}...")
            account = self.mastodon.get_account(username)

            if account is None:
                self.log.error(f"Аккаунт @{username} не найден")
                continue

            self.log.success(f"Аккаунт найден: {account['display_name']} (@{account['username']})")

            is_created = self.loader.add_account(account, prior)
            if is_created:
                added_count += 1
                self.log.success(f"Аккаунт @{username} добавлен в БД")
            else:
                updated_count += 1
                self.log.info(f"Аккаунт @{username} обновлён в БД")

        self.log.end(f"Добавление аккаунтов: добавлено={added_count}, обновлено={updated_count}")
        return added_count, updated_count

    async def _paginate_account_list_async(self, account_id: int, list_type: str, max_limit: int = 0) -> List[Dict]:
        """
        Получение списка подписок или подписчиков с ЖЁСТКИМ лимитом.
        max_limit - максимальное количество аккаунтов (0 = без ограничений)
        """
        accounts = []
        max_id = None
        page_count = 0
        start_time = time.time()
        rate_limit_delay = 0.2  # Задержка между запросами
        
        while max_limit == 0 or len(accounts) < max_limit:
            try:
                await asyncio.sleep(rate_limit_delay)
                
                if list_type == 'following':
                    batch = await self.async_mastodon.get_account_following(account_id, max_id=max_id)
                else:
                    batch = await self.async_mastodon.get_account_followers(account_id, max_id=max_id)
            except Exception as e:
                self.log.error(f"Ошибка при загрузке {list_type}: {e}")
                break
            
            if not batch:
                break
            
            # Добавляем аккаунты по одному с проверкой лимита
            for item in batch:
                if max_limit > 0 and len(accounts) >= max_limit:
                    break
                accounts.append(item)
            
            page_count += 1
            max_id = batch[-1]['id'] if batch else None
            
            # Логирование прогресса
            elapsed = time.time() - start_time
            items_per_sec = len(accounts) / elapsed if elapsed > 0 else 0
            
            if max_limit > 0:
                progress = len(accounts) / max_limit * 100
                self.log.process(
                    f"{list_type}: {len(accounts)}/{max_limit} ({progress:.1f}%) | "
                    f"страниц: {page_count} | {items_per_sec:.1f} акк/сек"
                )
        
        elapsed_total = time.time() - start_time
        self.log.success(
            f"{list_type} завершён: {len(accounts)} аккаунтов за {page_count} страниц "
            f"({elapsed_total:.0f} сек)"
        )
        
        return accounts

    def clear_database(self):
        """Очистка базы данных от всех узлов и связей"""
        self.log.warning("Очистка базы данных...")
        deleted_nodes, deleted_relationships = self.db.clear_database()
        self.log.success(f"Удалено узлов: {deleted_nodes}, связей: {deleted_relationships}")
    
    async def parse_account_relationships_async(self, account_id: int, prior: int = 1) -> Dict[str, int]:
        """
        Парсинг подписок и подписчиков аккаунта.
        Использует режим, указанный в config.FOLLOW_MODE.
        """
        stats = {'following_count': 0, 'followers_count': 0, 'new_accounts': 0}

        try:
            account_info = await self.async_mastodon.get_account(account_id)
        except Exception as e:
            self.log.warning(f"Не удалось получить информацию об аккаунте {account_id}: {e}")
            return stats

        self.log.process(f"Парсинг подписок и подписчиков для аккаунта @{account_info['username']}...")

        # Определяем лимиты
        following_limit = self.config.MAX_FOLLOWING if self.config.MAX_FOLLOWING > 0 else 0
        followers_limit = self.config.MAX_FOLLOWERS if self.config.MAX_FOLLOWERS > 0 else 0

        self.log.data(f"Ожидаемое количество: подписок={account_info.get('following_count', 0)}, "
                     f"подписчиков={account_info.get('followers_count', 0)}")
        if following_limit > 0:
            self.log.data(f"Лимит подписок: {following_limit}")
        if followers_limit > 0:
            self.log.data(f"Лимит подписчиков: {followers_limit}")

        # Выбираем режим загрузки
        if self.config.FOLLOW_MODE == "sequential":
            # Последовательный режим
            self.log.info(">>> Режим: последовательная загрузка (~1 запрос/сек)")
            
            # Создаём логгер-функцию для последовательного клиента
            def log_func(msg):
                self.log.process(msg)
            
            # Загружаем подписки и подписчиков последовательно
            following = await self.sequential_client.get_all_following(
                account_id, max_limit=following_limit, log_func=log_func
            )
            followers = await self.sequential_client.get_all_followers(
                account_id, max_limit=followers_limit, log_func=log_func
            )
        else:
            # Асинхронный режим (параллельная загрузка)
            self.log.info(">>> Режим: асинхронная загрузка (параллельно)")
            
            # Парсим подписки и подписчиков параллельно
            following_task = asyncio.create_task(
                self._paginate_account_list_async(
                    account_id, 'following',
                    max_limit=following_limit
                )
            )
            followers_task = asyncio.create_task(
                self._paginate_account_list_async(
                    account_id, 'followers',
                    max_limit=followers_limit
                )
            )

            # Ждём завершения обоих задач
            following, followers = await asyncio.gather(following_task, followers_task)

        stats['following_count'] = len(following)
        stats['followers_count'] = len(followers)

        # Батчевое сохранение аккаунтов с прогрессом
        # ВАЖНО: ставим prior=0 для аккаунтов из подписок/подписчиков
        if following:
            self.log.process(f"Сохранение {len(following)} аккаунтов из подписок в БД (prior=0)...")
            saved_count = self._save_accounts_batch_with_progress(following, prior=0, label="подписки")
            self.log.process(f"Создание связей FOLLOWS для {len(following)} аккаунтов...")
            self._create_follows_relationships_batch(account_id, [a['id'] for a in following], direction='outgoing')
            self.log.success(f"Связи подписок созданы: {len(following)} аккаунтов")

        if followers:
            self.log.process(f"Сохранение {len(followers)} аккаунтов из подписчиков в БД (prior=0)...")
            saved_count = self._save_accounts_batch_with_progress(followers, prior=0, label="подписчики")
            self.log.process(f"Создание связей FOLLOWS для {len(followers)} аккаунтов...")
            self._create_follows_relationships_batch(account_id, [f['id'] for f in followers], direction='incoming')
            self.log.success(f"Связи подписчиков созданы: {len(followers)} аккаунтов")

        self.log.success(
            f"Связи аккаунта сохранены: подписок={stats['following_count']}, "
            f"подписчиков={stats['followers_count']}"
        )

        return stats

    def _save_accounts_batch(self, accounts: List[Dict], prior: int = 1):
        """Батчевое сохранение аккаунтов в БД"""
        if not accounts:
            return

        with self.db.driver.session() as session:
            for account in accounts:
                # Обработка created_at (может быть строкой или datetime)
                created_at = account.get('created_at')
                if created_at:
                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        except:
                            created_at = None
                    if created_at:
                        created_at = created_at.isoformat()
                
                query = """
                MERGE (a:Mastodon_Account {id: $id})
                ON CREATE SET
                    a.dateC = datetime(),
                    a.username = $username,
                    a.display_name = $display_name,
                    a.url = $url,
                    a.created_at = $created_at,
                    a.followers_count = $followers_count,
                    a.following_count = $following_count,
                    a.statuses_count = $statuses_count,
                    a.prior = $prior,
                    a.last_post_id = 0
                ON MATCH SET
                    a.username = $username,
                    a.display_name = $display_name,
                    a.url = $url,
                    a.followers_count = $followers_count,
                    a.following_count = $following_count,
                    a.statuses_count = $statuses_count,
                    a.dateM = datetime()
                """
                session.run(query, {
                    'id': account['id'],
                    'username': account['username'],
                    'display_name': account['display_name'],
                    'url': account['url'],
                    'created_at': created_at,
                    'followers_count': account.get('followers_count', 0),
                    'following_count': account.get('following_count', 0),
                    'statuses_count': account.get('statuses_count', 0),
                    'prior': prior
                })

    def _save_accounts_batch_with_progress(self, accounts: List[Dict], prior: int = 1, label: str = "аккаунты") -> int:
        """Батчевое сохранение аккаунтов с отображением прогресса"""
        if not accounts:
            return 0
            
        start_time = time.time()
        batch_size = 500  # Обновляем лог каждые 500 аккаунтов
        
        with self.db.driver.session() as session:
            # Используем транзакцию для гарантии коммита
            with session.begin_transaction() as tx:
                for i, account in enumerate(accounts, 1):
                    # Обработка created_at
                    created_at = account.get('created_at')
                    if created_at:
                        if isinstance(created_at, str):
                            try:
                                created_at = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            except:
                                created_at = None
                        if created_at:
                            created_at = created_at.isoformat()
                    
                    query = """
                    MERGE (a:Mastodon_Account {id: $id})
                    ON CREATE SET
                        a.dateC = datetime(),
                        a.username = $username,
                        a.display_name = $display_name,
                        a.url = $url,
                        a.created_at = $created_at,
                        a.followers_count = $followers_count,
                        a.following_count = $following_count,
                        a.statuses_count = $statuses_count,
                        a.prior = $prior,
                        a.last_post_id = 0
                    ON MATCH SET
                        a.username = $username,
                        a.display_name = $display_name,
                        a.url = $url,
                        a.followers_count = $followers_count,
                        a.following_count = $following_count,
                        a.statuses_count = $statuses_count,
                        a.dateM = datetime()
                    """
                    tx.run(query, {
                        'id': account['id'],
                        'username': account['username'],
                        'display_name': account['display_name'],
                        'url': account['url'],
                        'created_at': created_at,
                        'followers_count': account.get('followers_count', 0),
                        'following_count': account.get('following_count', 0),
                        'statuses_count': account.get('statuses_count', 0),
                        'prior': prior
                    })
                    
                    # Логирование прогресса
                    if i % batch_size == 0 or i == len(accounts):
                        elapsed = time.time() - start_time
                        items_per_sec = i / elapsed if elapsed > 0 else 0
                        remaining = (len(accounts) - i) / items_per_sec if items_per_sec > 0 else 0
                        self.log.process(
                            f"  {label}: {i}/{len(accounts)} ({i/len(accounts)*100:.1f}%) | "
                            f"{items_per_sec:.1f} акк/сек | ост. {remaining:.0f} сек"
                        )
                
                # Явный коммит транзакции
                tx.commit()
        
        elapsed_total = time.time() - start_time
        self.log.success(f"  {label}: сохранено {len(accounts)} аккаунтов за {elapsed_total:.1f} сек")
        return len(accounts)

    def _create_follows_relationships_batch(self, account_id: int, related_ids: List[int], direction: str = 'outgoing'):
        """
        Батчевое создание связей FOLLOWS.
        direction: 'outgoing' - account_id следует за related_ids
                   'incoming' - related_ids следуют за account_id
        """
        if not related_ids:
            return 0

        start_time = time.time()
        
        with self.db.driver.session() as session:
            if direction == 'outgoing':
                # account_id FOLLOWS related_ids
                query = """
                MATCH (a:Mastodon_Account {id: $account_id})
                UNWIND $related_ids AS related_id
                MATCH (b:Mastodon_Account {id: related_id})
                MERGE (a)-[r:FOLLOWS]->(b)
                ON CREATE SET r.dateC = datetime()
                """
            else:
                # related_ids FOLLOWS account_id
                query = """
                MATCH (b:Mastodon_Account {id: $account_id})
                UNWIND $related_ids AS related_id
                MATCH (a:Mastodon_Account {id: related_id})
                MERGE (a)-[r:FOLLOWS]->(b)
                ON CREATE SET r.dateC = datetime()
                """
            
            result = session.run(query, {'account_id': account_id, 'related_ids': related_ids})
            result.consume()  # Гарантируем завершение запроса
        
        elapsed = time.time() - start_time
        self.log.info(f"  Связей FOLLOWS обработано: {len(related_ids)} ({elapsed:.1f} сек)")
        return len(related_ids)
    
    def fetch_posts_for_account(self, account_id: int, parse_comments: bool = True) -> int:
        """Загрузка постов для одного аккаунта с батчевой обработкой"""
        account_info = self.loader.get_account_info(account_id)
        if account_info is None:
            self.log.error(f"Аккаунт {account_id} не найден в БД")
            return 0

        username = account_info['username']
        last_post_id = account_info['last_post_id']

        limits = []
        if last_post_id != 0:
            limits.append(f"last_post_id={last_post_id}")
        if self.config.MAX_POSTS > 0:
            limits.append(f"max_posts={self.config.MAX_POSTS}")
        if self.config.MAX_PAGES > 0:
            limits.append(f"max_pages={self.config.MAX_PAGES}")
        if self.config.MIN_DATE:
            limits.append(f"min_date={self.config.MIN_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

        self.log.start(f"Загрузка постов для @{username} ({', '.join(limits)})")

        loaded_count = 0
        page_count = 0
        max_id = None
        first_post_id = None
        stop_reason = None
        batch_toots = []  # Батч для накопления постов
        batch_size = 20  # Размер батча для записи

        while True:
            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"лимит страниц ({self.config.MAX_PAGES})"
                break

            try:
                self.log.process(f"Запрос постов (страница {page_count + 1})...")
                toots = self.mastodon.get_account_statuses(account_id, max_id=max_id, limit=40)

                if not toots:
                    stop_reason = "посты закончились"
                    self.log.success("Посты закончились")
                    break

                self.log.success(f"Получено {len(toots)} постов")

            except Exception as e:
                self.log.error(f"Ошибка при запросе постов: {e}")
                time.sleep(10)
                continue

            # Накопление постов в батч
            for toot in toots:
                if last_post_id != 0 and toot['id'] == last_post_id:
                    stop_reason = f"достигнут last_post_id={last_post_id}"
                    self.log.success(f"Достигнут last_post_id={last_post_id}")
                    break

                if self.config.MIN_DATE and toot['created_at'] < self.config.MIN_DATE:
                    stop_reason = "достигнута минимальная дата"
                    break

                if first_post_id is None:
                    first_post_id = toot['id']
                    self.log.info(f"Первый пост: {first_post_id}")

                batch_toots.append(toot)
                loaded_count += 1

                # Парсим комментарии для каждого поста
                if parse_comments:
                    self.log.process(f"Парсинг комментариев для поста {toot['id']}...")
                    comments_loaded = self.loader.fetch_and_save_context(
                        toot['id'],
                        self.processed_posts,
                        check_prior=True,
                        target_prior=self.config.PRIOR
                    )
                    if comments_loaded > 0:
                        self.log.data(f"Загружено комментариев: {comments_loaded}")

                # Если батч заполнен - сохраняем
                if len(batch_toots) >= batch_size:
                    self.loader.save_posts_batch(batch_toots, target_prior=self.config.PRIOR)
                    self.log.data(f"Сохранён батч из {len(batch_toots)} постов")
                    batch_toots = []

                if self.config.MAX_POSTS > 0 and loaded_count >= self.config.MAX_POSTS:
                    stop_reason = f"лимит постов ({self.config.MAX_POSTS})"
                    break

            if stop_reason:
                break
            if self.config.MIN_DATE and toots and toots[-1]['created_at'] < self.config.MIN_DATE:
                break
            if last_post_id != 0 and any(t['id'] == last_post_id for t in toots):
                break

            # Сохраняем оставшиеся посты
            if batch_toots:
                self.loader.save_posts_batch(batch_toots, target_prior=self.config.PRIOR)
                self.log.data(f"Сохранён финальный батч из {len(batch_toots)} постов")
                batch_toots = []

            if toots:
                max_id = toots[-1]['id']
                page_count += 1
            else:
                break

            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"лимит страниц ({self.config.MAX_PAGES})"
                break

        # Обновляем last_post_id
        if first_post_id:
            self.loader.update_account_last_post_id(account_id, first_post_id)
            self.log.info(f"Обновлён last_post_id={first_post_id}")

        result_msg = f"Загружено {loaded_count} постов за {page_count} страниц"
        if stop_reason:
            result_msg += f" ({stop_reason})"
        self.log.end(result_msg)

        return loaded_count
    
    async def async_run(self):
        """Асинхронный основной метод запуска"""
        self.log.separator("#")
        self.log.start("ЗАПУСК СКРИПТА mastodon_pars.py (асинхронный режим)")
        self.log.separator("#")

        start_time = datetime.datetime.now()
        
        # Инициализация асинхронного клиента
        await self._init_async_client()

        # Шаг 1: Добавление аккаунтов
        self.log.info(">>> Шаг 1: Добавление аккаунтов")
        self.log.info(f"Список аккаунтов: {self.config.ACCOUNTS}")
        self.add_accounts(self.config.ACCOUNTS, prior=self.config.PRIOR)

        # Шаг 2: Парсинг связей аккаунтов (асинхронно)
        self.log.info(">>> Шаг 2: Парсинг связей аккаунтов (асинхронно)")
        accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
        total_accounts = len(accounts)
        self.log.data(f"Найдено аккаунтов: {total_accounts}")

        total_relationships = {'following': 0, 'followers': 0, 'new_accounts': 0}
        for idx, account in enumerate(accounts, 1):
            self.log.separator("=", 50)
            self.log.start(f"Обработка связей аккаунта {idx}/{total_accounts}")
            rel_stats = await self.parse_account_relationships_async(account['id'], prior=self.config.PRIOR)
            total_relationships['following'] += rel_stats['following_count']
            total_relationships['followers'] += rel_stats['followers_count']
            total_relationships['new_accounts'] += rel_stats['new_accounts']
            self.log.end(f"Аккаунт {idx}/{total_accounts} завершён")

        self.log.data(
            f"Всего связей: подписок={total_relationships['following']}, "
            f"подписчиков={total_relationships['followers']}, "
            f"новых аккаунтов={total_relationships['new_accounts']}"
        )

        # Шаг 3: Загрузка постов ТОЛЬКО для целевых аккаунтов (с prior=1)
        self.log.info(">>> Шаг 3: Загрузка постов (ТОЛЬКО целевые аккаунты)")
        self.log.info(f"Настройки: max_pages={self.config.MAX_PAGES}, "
                     f"max_posts={self.config.MAX_POSTS}, "
                     f"min_date={self.config.MIN_DATE}, "
                     f"prior={self.config.PRIOR}")

        accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
        total_accounts = len(accounts)
        self.log.data(f"Найдено целевых аккаунтов: {total_accounts}")

        total_loaded = 0
        for idx, account in enumerate(accounts, 1):
            self.log.separator("=", 50)
            self.log.start(f"Обработка аккаунта {idx}/{total_accounts}")
            loaded = self.fetch_posts_for_account(account['id'], parse_comments=True)
            total_loaded += loaded
            self.log.end(f"Аккаунт {idx}/{total_accounts} завершён")

        # Статистика
        end_time = datetime.datetime.now()
        stats = self.db.get_stats()

        self.log.separator("#")
        self.log.end("РАБОТА ЗАВЕРШЕНА")
        self.log.separator("#")

        self.log.info(f"Время выполнения: {end_time - start_time}")
        self.log.data(f"Загружено постов: {total_loaded}")
        self.log.separator("=")
        self.log.info("Статистика графа:")
        for label, count in stats.items():
            self.log.data(f"  - {label}: {count}")
        self.log.separator("=")

        self.log.info("Проверка на дубликаты:")
        self.log.success("  - Дубликаты НЕ создаются (используется MERGE по id)")
        self.log.success("  - При повторном запуске посты обновляются")

    def run(self):
        """Обёртка для запуска асинхронного метода"""
        return asyncio.run(self.async_run())

    def close(self):
        """Закрытие подключений"""
        if self.async_mastodon:
            asyncio.run(self.async_mastodon.__aexit__(None, None, None))
        if self.sequential_client:
            asyncio.run(self.sequential_client.__aexit__(None, None, None))
        self.db.close()


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

if __name__ == "__main__":
    # =============================================================================
    # НАСТРОЙКИ ЗАГРУЗКИ
    # =============================================================================

    # Список аккаунтов для парсинга (username или полный URL)
    ACCOUNTS_TO_PARSE = [
        "https://mastodon.social/@Edent",
        # "https://mastodon.social/@JuliusGoat",
        # "https://mastodon.social/@nixCraft",
        # Добавьте другие аккаунты:
        # "https://mastodon.social/@username",
        # "@username@instance.com",
    ]

    # Параметры загрузки
    MAX_PAGES = 1       # Макс. количество страниц (0 = без ограничений)
    MAX_POSTS = 10       # Макс. количество постов (0 = без ограничений)
    MIN_DATE = None     # Минимальная дата (None = без ограничений)
    PRIOR = 1           # Приоритет аккаунтов (для фильтрации в БД)
    
    # Лимиты для подписок/подписчиков (ЖЁСТКИЕ - сколько всего загружать)
    MAX_FOLLOWING = 500   # Макс. количество подписок (0 = без ограничений)
    MAX_FOLLOWERS = 500   # Макс. количество подписчиков (0 = без ограничений)
    

    # Режим загрузки подписок/подписчиков
    FOLLOW_MODE = "sequential"  # "sequential" - последовательный (~1 запрос (80 аккаунтов)/сек), "async" - асинхронный

    CLEAR_DB = False    # Очистить базу перед запуском

    # Пример установки минимальной даты:
    # MIN_DATE = datetime.datetime(2024, 1, 1)

    # =============================================================================

    # Создаём конфигурацию
    config = Config(
        ACCOUNTS=ACCOUNTS_TO_PARSE,
        MAX_PAGES=MAX_PAGES,
        MAX_POSTS=MAX_POSTS,
        MIN_DATE=MIN_DATE,
        PRIOR=PRIOR,
        MAX_FOLLOWING=MAX_FOLLOWING,
        MAX_FOLLOWERS=MAX_FOLLOWERS,
        FOLLOW_MODE=FOLLOW_MODE
    )

    # Создаём и запускаем парсер
    parser = MastodonParser(config)
    try:
        if CLEAR_DB:
            parser.clear_database()
        parser.run()
    finally:
        parser.close()
