# Mastodon Parser

Парсер постов из социальной сети **Mastodon** для загрузки данных в графовую базу данных **Neo4j**.

## Описание

Скрипт загружает посты (тооты) из указанных аккаунтов Mastodon и сохраняет их в Neo4j с полной структурой связей:
- Аккаунты пользователей
- Посты с метаданными
- Текст постов (отдельным узлом)
- Ссылки и домены
- Хештеги
- Медиафайлы (фото, видео, аудио, файлы)
- Связи между объектами (репосты, ответы, упоминания)
- **Подписки и подписчики аккаунтов** (связи FOLLOWS)

## Структура графа

```
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
```

## Требования

- Python 3.8+
- Neo4j 4.x или 5.x
- Доступ к API Mastodon (токен доступа)

## Установка

1. **Установите зависимости:**

```bash
pip install -r requirements.txt
```

2. **Настройте подключение к Neo4j:**

Убедитесь, что Neo4j запущен и доступен по адресу `bolt://localhost:7687`.

3. **Получите токен доступа Mastodon:**

   - Зарегистрируйте приложение в настройках вашего инстанса Mastodon
   - Создайте токен доступа с правами `read:statuses`, `read:accounts`

## Настройка

Откройте файл `mastodon_pars.py` и отредактируйте секцию **НАСТРОЙКИ ЗАГРУЗКИ**:

```python
if __name__ == "__main__":
    # Список аккаунтов для парсинга (username или полный URL)
    ACCOUNTS_TO_PARSE = [
        "https://mastodon.social/@nixCraft",
        "https://mastodon.social/@Edent",
        # Добавьте другие аккаунты:
        # "@username@instance.com",
    ]

    # Параметры загрузки
    MAX_PAGES = 3       # Макс. количество страниц (0 = без ограничений)
    MAX_POSTS = 0       # Макс. количество постов (0 = без ограничений)
    MIN_DATE = None     # Минимальная дата (None = без ограничений)
    PRIOR = 1           # Приоритет аккаунтов (для фильтрации в БД)
    
    # Лимиты для подписок/подписчиков
    MAX_FOLLOWING = 100   # Макс. количество подписок (0 = без ограничений)
    MAX_FOLLOWERS = 100   # Макс. количество подписчиков (0 = без ограничений)
    
    # Режим загрузки подписок/подписчиков
    FOLLOW_MODE = "sequential"  # "sequential" - последовательный, "async" - асинхронный
```

### Параметры конфигурации

| Параметр | Описание | Пример |
|----------|----------|--------|
| `ACCOUNTS_TO_PARSE` | Список аккаунтов для парсинга | `["@user@instance.com"]` |
| `MAX_PAGES` | Макс. страниц на аккаунт (0 = все) | `3` |
| `MAX_POSTS` | Макс. постов на аккаунт (0 = все) | `100` |
| `MIN_DATE` | Минимальная дата загрузки | `datetime(2024, 1, 1)` |
| `PRIOR` | Приоритет аккаунтов в БД | `1` |
| `MAX_FOLLOWING` | Макс. подписок для загрузки (0 = все) | `100` |
| `MAX_FOLLOWERS` | Макс. подписчиков для загрузки (0 = все) | `100` |
| `FOLLOW_MODE` | Режим загрузки связей | `"sequential"` или `"async"` |
| `API_LIMIT_FOLLOWING` | Размер страницы для подписок (макс 80) | `80` |
| `API_LIMIT_FOLLOWERS` | Размер страницы для подписчиков (макс 80) | `80` |
| `API_LIMIT_POSTS` | Размер страницы для постов (макс 40) | `40` |

### Режимы загрузки подписок/подписчиков

| Режим | Описание | Скорость | Надёжность |
|-------|----------|----------|------------|
| `"sequential"` | Последовательная загрузка (~1 запрос/сек) | Медленнее | Высокая |
| `"async"` | Параллельная асинхронная загрузка | Быстрее | Средняя |

**Рекомендация:** Используйте `sequential` для больших объёмов данных, чтобы избежать rate limiting.

## Запуск

```bash
python mastodon_pars.py
```

## Пример вывода

```
17:23:06 | ############################################################
17:23:06 | ▶ НАЧАЛО: ЗАПУСК СКРИПТА mastodon_pars.py
17:23:06 | ############################################################
17:23:06 | ℹ >>> Шаг 1: Добавление аккаунтов
17:23:06 | ℹ Список аккаунтов: ['https://mastodon.social/@Edent']
17:23:07 | ✓ Аккаунт найден: Terence Eden (@Edent)
17:23:07 | ✓ Аккаунт @https://mastodon.social/@Edent добавлен в БД
17:23:07 | ℹ >>> Шаг 2: Загрузка постов
17:23:16 | ✓ Получено 40 постов
17:23:25 | 📊 Найдено аккаунтов: 2
...
```

## Логирование

Все события записываются:
- **В консоль** — с цветными иконками для разных типов событий
- **В файл** — `mastodon_pars.log` (полная информация с timestamp)

### Типы событий

| Иконка | Тип | Описание |
|--------|-----|----------|
| ▶ | Start | Начало операции |
| ◀ | End | Завершение операции |
| ✓ | Success | Успешное выполнение |
| ℹ | Info | Информационное сообщение |
| ⚠ | Warning | Предупреждение |
| ✗ | Error | Ошибка |
| ⚙ | Process | Процесс выполнения |
| 📊 | Data | Статистика/данные |

## Проверка на дубликаты

- **Дубликаты НЕ создаются** — используется `MERGE` по уникальному `id`
- **При повторном запуске** — существующие посты обновляются
- **Прогресс загрузки** — сохраняется в поле `last_post_id` аккаунта

## Примеры Cypher-запросов

### Получить все посты аккаунта:
```cypher
MATCH (a:Mastodon_Account {username: "nixCraft"})-[:POSTED]->(p:Mastodon_Post)
RETURN p.created_at, p.url, p.reblogs_count, p.favourites_count
ORDER BY p.created_at DESC
```

### Найти подписки аккаунта:
```cypher
MATCH (a:Mastodon_Account {username: "nixCraft"})-[:FOLLOWS]->(b:Mastodon_Account)
RETURN b.username, b.display_name, b.followers_count
ORDER BY b.followers_count DESC
```

### Найти подписчиков аккаунта:
```cypher
MATCH (a:Mastodon_Account {username: "nixCraft"})<-[:FOLLOWS]-(b:Mastodon_Account)
RETURN b.username, b.display_name, b.followers_count
ORDER BY b.followers_count DESC
```

### Найти посты с хештегом:
```cypher
MATCH (p:Mastodon_Post)-[:TAGS]->(h:Hashtag {name: "python"})
RETURN p.created_at, p.url
ORDER BY p.created_at DESC
```

### Найти взаимные подписки (друзья):
```cypher
MATCH (a:Mastodon_Account {username: "nixCraft"})-[r1:FOLLOWS]->(b:Mastodon_Account)
WHERE EXISTS {
    MATCH (b)-[:FOLLOWS]->(a)
}
RETURN b.username, b.display_name
ORDER BY b.username
```

### Получить статистику графа:
```cypher
MATCH (n)
RETURN labels(n)[0] as label, count(n) as count
ORDER BY count DESC
```

### Найти связанные домены:
```cypher
MATCH (d:Domain)-[:rel]->(u:URI)<-[:part]-(p:Mastodon_Post)
RETURN d.name, count(u) as uri_count, count(p) as post_count
ORDER BY uri_count DESC
```

## Структура проекта

```
mastodon_pars/
├── mastodon_pars.py          # Основной скрипт парсера
├── mastodon_followings.py    # Парсер подписок (отдельный)
├── mastodon_followers.py     # Парсер подписчиков (отдельный)
├── clear_db.py               # Скрипт очистки БД
├── requirements.txt          # Зависимости Python
├── README.md                # Эта документация
├── mastodon_following.json  # Данные подписок (экспорт)
├── mastodon_followers.json  # Данные подписчиков (экспорт)
└── mastodon_pars.log        # Файл логов (создаётся при запуске)
```

## Возможные ошибки

### `ConnectionError: Не удалось подключиться к Neo4j`
- Проверьте, запущен ли Neo4j
- Убедитесь, что учётные данные верны

### `MastodonNetworkError`
- Проверьте токен доступа
- Убедитесь, что инстанс Mastodon доступен

### `FloodWaitError` (ограничение API)
- Скрипт автоматически ожидает сброса лимита
- Используйте `FOLLOW_MODE = "sequential"` для снижения нагрузки
- Уменьшите `MAX_PAGES` или `MAX_FOLLOWING/MAX_FOLLOWERS`

### Rate limiting при загрузке подписок/подписчиков
- API Mastodon ограничивает ~300 запросов за 15 минут
- Последовательный режим автоматически делает задержку 1 сек между запросами
- При получении 429 ошибки скрипт ждёт указанное в заголовке время

## Лицензия

Скрипт предоставлен "как есть" для образовательных целей.
