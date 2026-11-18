# Vest monitor

Демо системи моніторингу тварин через розумний жилет, у тих технологіях, що визначені в лабах:
MQTT → ingest → Kafka (3 брокери) → writer / detection → PostgreSQL + TimescaleDB (primary + синхронна репліка), Redis для кешу профілів і пауз між сповіщеннями.
GDPR-видалення — сага, яку оркеструє Temporal, кроки виконує saga-worker.

```
жилети ──MQTT QoS1──► mosquitto ──► ingest ──acks=all──► Kafka: telemetry (12 партицій × 3 репліки)
 (симулятор,          PUBACK жилету;  32 сесії в             ├─► group writer    ──► TimescaleDB primary ══sync══► replica
  ноутбук або Pi)     тримає, поки   спільній підписці,     │    (synchronous_commit = local, PA/EL)         │
                      ingest не      ACK брокеру лише       └─► group detection ──► alerts, сповіщення       │
                      підтвердить    після запису в Kafka         │  профіль: cache-aside у Redis              │
                                                                   └──────────────────────────────────────── api ◄┘
                                                         профіль пишеться з remote_apply (PC/EC), графіки читаються з репліки
```

## Запуск

```bash
docker compose -p vest up -d --build
```

Дашборд: http://localhost:18300 (лише з цього комп'ютера: з нього можна вимикати контейнери).

| Що | Адреса |
|---|---|
| Дашборд і API | http://localhost:18300 |
| MQTT (для симулятора на Raspberry Pi) | `<ip ноутбука>:11883` |
| Redis (для симулятора на Raspberry Pi) | `<ip ноутбука>:16379` |
| Postgres primary / replica | `postgres://vest:vest@localhost:15432/vest`, `:15433` |
| Temporal UI (історія саг GDPR) | http://localhost:18233 |
| Temporal gRPC (для `temporal` CLI) | `localhost:17233` |

Зупинити: `docker compose -p vest down`. З даними: `docker compose -p vest down -v`.

Сценарій показу: [DEMO.md](DEMO.md).

## Симулятор на Raspberry Pi

Другий парк жилетів з окремої машини. На Pi має бути Node 24 (64-бітна ОС) або Docker.

```bash
# на ноутбуці: IP у локальній мережі
ipconfig getifaddr en0
```

```bash
# скопіювати проєкт на Pi (без node_modules)
rsync -a --exclude node_modules ./ pi@raspberrypi.local:~/vest-monitor/
```

```bash
# на Pi
cd ~/vest-monitor && npm ci --omit=dev
SIM_NAME=raspberry ID_OFFSET=5000 VESTS=500 MQTT_URL=mqtt://192.168.1.10:11883 REDIS_URL=redis://192.168.1.10:16379 node src/simulator.ts
```

Кількість жилетів, швидкість часу і «вимкнути мережу» регулюються на самому парку: пульт відкривається на Pi або з телефона за адресою `http://raspberrypi.local:8080` (для симулятора на ноутбуці — http://localhost:18080). Дашборд ноутбука лише показує всі парки.

`ID_OFFSET=5000` дає жилети VST-5001…, щоб не перетинатися з ноутбуком. На дашборді з'явиться окрема картка «raspberry» з власним повзунком. Висмикнути Wi-Fi на Pi — справжній partition: жилети пишуть у буфер (до 1000 пакетів кожен), після повернення мережі вивантажують усе разом.

## Код

TypeScript, Node 24 запускає `.ts` напряму. Перевірка типів: `npm run typecheck`.

| Файл | Що робить |
|---|---|
| `proto/vest.proto` | формат пакета жилета (Protobuf, масиви дельт) |
| `src/simulator.ts` | віртуальні жилети, офлайн-буфер, керування з дашборда |
| `src/ingest.ts` | MQTT → валідація → Kafka `acks=all` → лише тоді PUBACK брокеру |
| `src/kafka.ts` | топік, consumer group з комітом офсету після обробки |
| `src/writer.ts` | Kafka → `metrics` (hypertable), ідемпотентний INSERT |
| `src/detection.ts` | Kafka → правила за профілем тварини → тривоги; профіль у Redis (cache-aside, single-flight, TTL з jitter) |
| `src/api.ts` | REST + SSE + дашборд; читання з репліки, запис профілю з `remote_apply` і optimistic lock (409); старт і стан саги GDPR |
| `src/saga/workflows.ts` | сага GDPR-видалення (Temporal workflow): 5 кроків, лише вперед |
| `src/saga/activities.ts` | кроки саги, кожен ідемпотентний: запит, Postgres, телеметрія й агрегати, Parquet (заглушка), Redis |
| `src/saga-worker.ts` | воркер Temporal, черга `gdpr`; workflow збирається при збірці образу (`src/saga/bundle.ts`) |
| `src/migrate.ts`, `db/saga.sql` | `vest_bindings`, `deletion_requests`, `pets.version`; застосовується і на наявній базі |
| `src/chaos.ts` | вимикає/вмикає контейнери для демо (через Docker socket, лише з білого списку) |
| `db/init.sql` | схема, політики стиснення й зберігання, continuous aggregates, ролі з різним `synchronous_commit` |
| `db/replica.sh`, `db/01-replication.sh` | потокова репліка |
| `public/index.html` | дашборд |
