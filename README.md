# News Platform

Azerbaijani xəbər toplama platforması — Telegram kanalları və xəbər saytları üçün vahid sistem.

## Arxitektura

Bu layihə iki tamamilə **izolə** scraper-i vahid bir infrastrukturda birləşdirir:

| Modul | Məqsəd | Cədvəl | Texnologiya |
|-------|--------|--------|-------------|
| **Telegram Scraper** (`news_ingestor`) | Telegram kanallarından xəbər toplama | `news` | Async (asyncpg, SQLAlchemy, Telethon) |
| **Web Scraper** (`sonxeber_scraper`) | 20 Azerbaycan xəbər portalından xəbər toplama | `articles` | Sync (psycopg, requests, BeautifulSoup) |

### Cədvəl İzolasiyası

```
PostgreSQL (news_ingestor DB)
├── news          ← Telegram scraper (mütləq izolə)
└── articles      ← Web scraper (mütləq izolə)
```

İki modul eyni verilənlər bazasını paylaşır, lakin **heç bir ortaq cədvəl yoxdur**. Kodları, import-ları, ORM-ləri və runtime-ları tamamilə ayrıdır.

## Docker ilə İşə Salma

### 1. PostgreSQL-i başladın

```bash
docker compose up -d postgres
```

PostgreSQL `localhost:5433` portunda əlçatandır (yerli 5432 ilə konflikt olmasın deyə).

### 2. Mevcut verileri köçürün (isteğe bağlı)

Əgər köhnə sistemlərdən data varsa:

```bash
./scripts/migrate_data.sh
```

### 3. Telegram sessiyasını yaradın (bir dəfəlik)

```bash
docker compose run --rm telegram-worker python -m news_ingestor.cli login-telegram
```

### 4. Bütün servisləri başladın

```bash
docker compose up -d
```

Bu 4 servis başlayacaq:
- `postgres` — Paylaşılan verilənlər bazası
- `control-api` — Telegram mənbə idarəetmə API (port 8081)
- `telegram-worker` — Canlı Telegram ingestion
- `web-scraper` — 20 xəbər portalının scraping-i

### Servis idarəetməsi

```bash
# Bütün servislərin statusu
docker compose ps

# Loglar
docker compose logs -f telegram-worker
docker compose logs -f web-scraper

# Tək servisi yenidən başlatmaq
docker compose restart web-scraper
docker compose restart telegram-worker

# Hamısını dayandırmaq
docker compose down
```

## Docker olmadan İşə Salma

### Ön şərtlər

- Python 3.12+
- PostgreSQL (yerli, işlək)

### Quraşdırma

```bash
cd news-platform
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

`.env` faylını düzəldin (yerli PostgreSQL-ə uyğun):

```env
DATABASE_URL=postgresql+asyncpg://your_user@localhost:5432/news_ingestor
SONXEBER_PGHOST=127.0.0.1
SONXEBER_PGPORT=5432
SONXEBER_PGUSER=your_user
SONXEBER_PGPASSWORD=
SONXEBER_PGDATABASE=news_ingestor
SONXEBER_PGADMIN_DATABASE=postgres
```

### Telegram Scraper əmrləri

```bash
# DB schema-nı hazırla
python -m news_ingestor.cli init-db

# Telegram sessiyası yarat
python -m news_ingestor.cli login-telegram

# Telegram mənbə əlavə et
python -m news_ingestor.cli add-telegram-source https://t.me/bakutvxeber

# Worker-i başlat
python -m news_ingestor.cli run-telegram-worker

# Audit
python -m news_ingestor.cli audit-telegram --all-sources

# Status
python -m news_ingestor.cli source-status
```

### Web Scraper əmrləri

```bash
# Tək run, bütün saytlar
python main.py sync-once

# Tək sayt
python main.py sync-once --source oxu.az
python main.py sync-once --source report.az

# Foreground polling
python main.py poll

# Tək sayt polling
python main.py poll --source apa.az

# Background daemon
python main.py start
python main.py status
python main.py stop

# Statistika
python main.py stats
```

## Dəstəklənən Mənbələr

### Telegram Kanalları

`add-telegram-source` əmri ilə istənilən Telegram kanalı əlavə edilə bilər.

### Xəbər Saytları (20 portal)

| # | Portal | Əsas Discovery |
|---|--------|----------------|
| 1 | 1news.az | sitemap + lenta pagination |
| 2 | apa.az | RSS + all-news pagination |
| 3 | axar.az | sitemap_latest + feed + homepage |
| 4 | azxeber.com | sitemap + xeberler pagination |
| 5 | azerbaijan.az | /news pagination + forward probe |
| 6 | azertag.az | archive pagination + forward probe |
| 7 | ikisahil.az | RSS + /lent pagination |
| 8 | islam.az | feed + AJAX pagination |
| 9 | islamazeri.com | /xeberler pagination + homepage |
| 10 | metbuat.az | RSS + olke-metbuati pagination |
| 11 | milli.az | sitemap_latest + xeber lenti |
| 12 | oxu.az | news-sitemap + infinite scroll |
| 13 | report.az | news-sitemap + infinity batches |
| 14 | sia.az | sitemap_latest + feed + /latest |
| 15 | siyasetinfo.az | feed + homepage pagination |
| 16 | sonxeber.az | homepage + son-xeberler + probe |
| 17 | teleqraf.az | sitemap_latest + /latest pagination |
| 18 | xeberler.az | RSS + /new/content pagination |
| 19 | yeniazerbaycan.com | sitemap + SonXeber pagination |
| 20 | yenixeber.az | homepage + xeberler + son-xeberler |

## API Endpoints

Control API `localhost:8081` portunda:

- `GET /health`
- `GET /status`
- `GET /sources`
- `POST /sources/telegram`
- `POST /sources/{source_key}/pause`
- `POST /sources/{source_key}/resume`
- `GET /telegram/dialogs/search?q=...`

## Layihə Strukturu

```
news-platform/
├── docker-compose.yml          # 4 servis: postgres, control-api, telegram-worker, web-scraper
├── Dockerfile                  # Vahid image, hər servisdə fərqli CMD
├── pyproject.toml              # Birləşmiş dependencies
├── .env                        # Konfiqurasiya (git-ə düşmür)
├── .env.example                # Şablon
├── main.py                     # Web scraper entry point
├── src/
│   └── news_ingestor/          # Telegram scraper modulu (izolə)
│       ├── cli.py
│       ├── settings.py
│       ├── db/                 # SQLAlchemy ORM → `news` cədvəli
│       ├── telegram/           # Telethon client, ingestor, serializer
│       ├── control/            # FastAPI control API
│       ├── services/           # Normalizer, dedup, audit, checkpoints
│       ├── utils/              # JSON, text, time helpers
│       └── websites/           # Generic website fetcher/parser
├── sonxeber_scraper/           # Web scraper modulu (izolə)
│   ├── cli.py
│   ├── config.py
│   ├── db.py                   # psycopg raw SQL → `articles` cədvəli
│   ├── service.py
│   ├── sources.py              # 20 client factory
│   ├── *_client.py             # Hər portal üçün ayrı client
│   └── utils.py                # URL/date parsing
├── tests/
├── scripts/
│   └── migrate_data.sh         # Mevcut verilerin göçü
├── state/telethon/             # Telegram session faylları
└── data/                       # Web scraper PID/log
```
