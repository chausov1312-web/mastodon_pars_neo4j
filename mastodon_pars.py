"""
Mastodon Parser - загрузка постов из Mastodon в графовую БД Neo4j

Структура графа:
    (Mastodon_Account)-[:POSTED]->(Mastodon_Post)
    (Mastodon_Post)-[:MENTIONS]->(Mastodon_Account)
    (Mastodon_Post)-[:TAGS]->(Hashtag)
    (Mastodon_Post)-[:REBLOG_OF]->(Mastodon_Post)
    (Mastodon_Post)-[:REPLY_TO]->(Mastodon_Post)
    (Text)-[:part]->(Mastodon_Post)
    (URI)-[:part]->(Mastodon_Post)
    (Domain)-[:rel]->(URI)
    (Photo|Video|Audio|File)-[:part]->(Mastodon_Post)
"""

import datetime
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

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
    NEO4J_PASSWORD: str = "password"

    # Mastodon
    MASTODON_INSTANCE: str = "https://mastodon.social"
    ACCESS_TOKEN: str = "***********"

    # Параметры загрузки
    MAX_PAGES: int = 0  # 0 = без ограничений
    MAX_POSTS: int = 0  # 0 = без ограничений
    MIN_DATE: Optional[datetime.datetime] = None

    # Аккаунты для парсинга
    ACCOUNTS: List[str] = None
    PRIOR: int = 1

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
    
    def __init__(self, instance: str, access_token: str):
        self.api = Mastodon(access_token=access_token, api_base_url=instance)
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
            # Убираем @ и URL
            clean_username = username.strip('@')
            if clean_username.startswith('http'):
                clean_username = clean_username.rstrip('/').split('/')[-1]
            return self.api.account_lookup(clean_username)
        except Exception as e:
            return None
    
    def get_account_statuses(self, account_id: int, max_id: Optional[int] = None, 
                             limit: int = 40) -> List[Dict]:
        """Получение постов аккаунта"""
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
    
    def __init__(self, db: Neo4jManager, logger: Logger):
        self.db = db
        self.log = logger
    
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
    
    def save_post(self, toot: Dict) -> bool:
        """
        Сохранение поста в БД.
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

            # Связь с автором: (Account)-[:POSTED]->(Post)
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

        return is_new
    
    def _save_media(self, session, media: Dict, post_id: int):
        """Сохранение медиафайла с классификацией по типу"""
        media_type = media['type']
        media_id = media['id']

        # Определяем тип узла по типу медиа
        if media_type == 'image':
            label = 'Photo'
        elif media_type == 'video':
            label = 'Video'
        elif media_type == 'audio':
            label = 'Audio'
        else:
            label = 'File'

        # Создаём узел медиа
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
    
    def get_accounts_with_prior(self, prior: int = 1) -> List[Dict]:
        """Получение списка аккаунтов с указанным prior"""
        query = "MATCH (a:Mastodon_Account) WHERE a.prior = $prior RETURN a.id, a.last_post_id"
        with self.db.driver.session() as session:
            result = session.run(query, {'prior': prior})
            records = list(result)
            return [{'id': r['a.id'], 'last_post_id': r['a.last_post_id'] or 0} for r in records]


# =============================================================================
# ОСНОВНОЙ КЛАСС ПРИЛОЖЕНИЯ
# =============================================================================

class MastodonParser:
    """Основной класс приложения"""
    
    def __init__(self, config: Config):
        self.config = config
        self.log = Logger("MastodonParser", log_file=config.LOG_FILE, level=config.LOG_LEVEL)
        self.db = Neo4jManager(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD)
        self.mastodon = MastodonClient(config.MASTODON_INSTANCE, config.ACCESS_TOKEN)
        self.loader = GraphLoader(self.db, self.log)
    
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
    
    def fetch_posts_for_account(self, account_id: int) -> int:
        """Загрузка постов для одного аккаунта"""
        # Получаем информацию об аккаунте из БД
        account_info = self.loader.get_account_info(account_id)
        if account_info is None:
            self.log.error(f"Аккаунт {account_id} не найден в БД")
            return 0
        
        username = account_info['username']
        last_post_id = account_info['last_post_id']
        
        # Формируем информацию о лимитах
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
        
        while True:
            # Проверка лимита страниц
            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"лимит страниц ({self.config.MAX_PAGES})"
                break
            
            # Получаем посты
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
                self.log.info("Ожидание 10 секунд перед повторной попыткой...")
                time.sleep(10)
                continue
            
            # Обрабатываем каждый пост
            for toot in toots:
                # Проверка last_post_id
                if last_post_id != 0 and toot['id'] == last_post_id:
                    stop_reason = f"достигнут last_post_id={last_post_id}"
                    self.log.success(f"Достигнут last_post_id={last_post_id}")
                    break
                
                # Проверка по дате
                if self.config.MIN_DATE:
                    toot_date = toot['created_at']
                    if toot_date < self.config.MIN_DATE:
                        stop_reason = f"достигнута минимальная дата"
                        self.log.success(f"Достигнута минимальная дата: {toot_date.strftime('%Y-%m-%d %H:%M:%S')}")
                        break
                
                # Сохраняем ID первого поста
                if first_post_id is None:
                    first_post_id = toot['id']
                    self.log.info(f"Первый пост: {first_post_id}")
                
                # Сохраняем пост
                is_new = self.loader.save_post(toot)
                loaded_count += 1
                
                if is_new:
                    self.log.data(f"Загружено {loaded_count} постов (новых)")
                else:
                    self.log.info(f"Загружено {loaded_count} постов (обновлено существующие)")
                
                # Проверка лимита постов
                if self.config.MAX_POSTS > 0 and loaded_count >= self.config.MAX_POSTS:
                    stop_reason = f"лимит постов ({self.config.MAX_POSTS})"
                    self.log.success(f"Достигнут лимит max_posts={self.config.MAX_POSTS}")
                    break
            
            # Проверки после цикла обработки
            if self.config.MIN_DATE and toots and toots[-1]['created_at'] < self.config.MIN_DATE:
                break
            if last_post_id != 0 and any(t['id'] == last_post_id for t in toots):
                break
            
            # Обновляем max_id для следующей страницы
            if toots:
                max_id = toots[-1]['id']
                page_count += 1
            else:
                break
            
            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"лимит страниц ({self.config.MAX_PAGES})"
                break
        
        # Обновляем last_post_id в БД
        if first_post_id:
            self.loader.update_account_last_post_id(account_id, first_post_id)
            self.log.info(f"Обновлён last_post_id={first_post_id} для аккаунта {account_id}")
        
        result_msg = f"Загружено {loaded_count} постов за {page_count} страниц"
        if stop_reason:
            result_msg += f" ({stop_reason})"
        self.log.end(result_msg)
        
        return loaded_count
    
    def run(self):
        """Основной метод запуска"""
        self.log.separator("#")
        self.log.start("ЗАПУСК СКРИПТА mastodon_pars.py")
        self.log.separator("#")

        start_time = datetime.datetime.now()

        # Шаг 1: Добавление аккаунтов
        self.log.info(">>> Шаг 1: Добавление аккаунтов")
        self.log.info(f"Список аккаунтов: {self.config.ACCOUNTS}")
        self.add_accounts(self.config.ACCOUNTS, prior=self.config.PRIOR)

        # Шаг 2: Загрузка постов
        self.log.info(">>> Шаг 2: Загрузка постов")
        self.log.info(f"Настройки: max_pages={self.config.MAX_PAGES}, "
                     f"max_posts={self.config.MAX_POSTS}, "
                     f"min_date={self.config.MIN_DATE}, "
                     f"prior={self.config.PRIOR}")

        # Получаем список аккаунтов
        accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
        total_accounts = len(accounts)
        self.log.data(f"Найдено аккаунтов: {total_accounts}")

        # Загружаем посты для каждого аккаунта
        total_loaded = 0
        for idx, account in enumerate(accounts, 1):
            self.log.separator("=", 50)
            self.log.start(f"Обработка аккаунта {idx}/{total_accounts}")
            loaded = self.fetch_posts_for_account(account['id'])
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
    
    def close(self):
        """Закрытие подключений"""
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
        "https://mastodon.social/@JuliusGoat",
        # Добавьте другие аккаунты:
        # "https://mastodon.social/@username",
        # "@username@instance.com",
    ]
    
    # Параметры загрузки
    MAX_PAGES = 3       # Макс. количество страниц (0 = без ограничений)
    MAX_POSTS = 0       # Макс. количество постов (0 = без ограничений)
    MIN_DATE = None     # Минимальная дата (None = без ограничений)
    PRIOR = 1           # Приоритет аккаунтов (для фильтрации в БД)
    
    # Пример установки минимальной даты:
    # MIN_DATE = datetime.datetime(2024, 1, 1)
    
    # =============================================================================
    
    # Создаём конфигурацию
    config = Config(
        ACCOUNTS=ACCOUNTS_TO_PARSE,
        MAX_PAGES=MAX_PAGES,
        MAX_POSTS=MAX_POSTS,
        MIN_DATE=MIN_DATE,
        PRIOR=PRIOR
    )

    # Создаём и запускаем парсер
    parser = MastodonParser(config)
    try:
        parser.run()
    finally:
        parser.close()
