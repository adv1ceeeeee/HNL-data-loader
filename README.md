### NHL Data Loader

Описание проекта

Этот проект представляет собой инструмент для загрузки и обработки данных о матчах NHL (Национальной хоккейной лиги) из публичного API NHL в базу данных PostgreSQL. Скрипт собирает различные типы данных о матчах, включая статистику игроков, события в матчах (play-by-play), информацию о судьях и тренерах, а также данные о звездах матча.

### Основные возможности

1. Параллельная загрузка данных о матчах за указанный период
2. Сбор данных различных типов данных NHL:
    - Boxscore (основная статистика матча)
    - Play-by-play (события в матче)
    - Landing page (общая информация о матче)
    - Right rail (дополнительная статистика)
    - Schedule_by_date  (расписание матчей)
4. Преобразование сложных JSON-структур в плоские таблицы
5. Расчет дополнительных метрик (например, длительность матча)
6. Загрузка данных в PostgreSQL с обработкой всех конфликтов

### Требования
- Python 3.8+
- Установленные зависимости из requirements.txt
- Доступ к PostgreSQL серверу (заранее подготовленному)

### Установка
1. Клонируйте репозиторий:
```bash
git clone https://github.com/yourusername/nhl-data-loader.git
cd nhl-data-loader
```

2. Установите зависимости:

```bash
pip install -r requirements.txt

nhlpy>=1.0.0
pandas>=1.3.0
psycopg2-binary>=2.9.0
colorama>=0.4.0
tqdm>=4.0.0
python-dotenv>=0.19.0
httpx>=0.23.0
httpcore>=0.15.0
urllib3>=1.26.0
requests>=2.28.0

```

3. Настройте подключение к PostgreSQL в файле Data_loader_final.py:

```bash
DB_CONFIG = {
    "host": "your_host",
    "database": "your_hDB",
    "user": "your_user",
    "password": "your_password",
    "port": "your_port"
}
```

### Использование
Запустите скрипт - по умолчанию скрипт загружает данные за один день (21 сентября 2025 года). Чтобы изменить даты, отредактируйте строки в конце файла:
```bash
start_date = "2025-09-21"
end_date = "2025-09-21"
```
Если (`start_date=end_date`), то скрипт загрузит информацию о матчах за 1 день, в противном случае за период.

### Структура базы данных

Создайте БД в PostgreSQL СУБД следующего формата

```bash
CREATE TABLE public.game_period_stats (
  id integer NOT NULL DEFAULT nextval('game_period_stats_id_seq'::regclass),
  game_id text,
  period_number integer,
  period_type text CHECK (period_type = ANY (ARRAY['REG'::text, 'OT'::text, 'SO'::text])),
  goals_home integer,
  goals_away integer,
  shots_home integer,
  shots_away integer,
  home_puck_control double precision,
  away_puck_control double precision,
  home_pim integer,
  away_pim integer,
  home_hits integer,
  away_hits integer,
  CONSTRAINT game_period_stats_pkey PRIMARY KEY (id),
  CONSTRAINT game_period_stats_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.nhl_games_extended(game_id)
);
CREATE TABLE public.nhl_games_extended (
  game_id text NOT NULL,
  game_date date,
  game_type integer,
  venue text,
  period_type text,
  period_number integer,
  home_id integer,
  home_abbrev text,
  home_score integer,
  home_sog integer,
  home_name text,
  home_city text,
  away_id integer,
  away_abbrev text,
  away_score integer,
  away_sog integer,
  away_name text,
  away_city text,
  home_forwards_count integer,
  home_forwards_goals integer,
  home_forwards_assists integer,
  home_forwards_hits integer,
  home_forwards_pim integer,
  home_forwards_blockedshots integer,
  home_forwards_shifts integer,
  home_forwards_plusminus integer,
  home_forwards_giveaways integer,
  home_forwards_takeaways integer,
  home_defense_count integer,
  home_defense_goals integer,
  home_defense_assists integer,
  home_defense_hits integer,
  home_defense_pim integer,
  home_defense_blockedshots integer,
  home_defense_shifts integer,
  home_defense_plusminus integer,
  home_defense_giveaways integer,
  home_defense_takeaways integer,
  home_forwards_avg_toi double precision,
  home_defense_avg_toi double precision,
  home_forwards_total_toi double precision,
  home_defense_total_toi double precision,
  home_skaters_total_toi double precision,
  home_total_plusminus integer,
  home_total_giveaways integer,
  home_total_takeaways integer,
  home_goalies_count integer,
  home_goalies_saves integer,
  home_goalies_savepctg double precision,
  home_goalies_evenstrength_shots_against integer,
  home_goalies_powerplay_shots_against integer,
  home_goalies_shorthanded_shots_against integer,
  home_goalies_evenstrength_goals_against integer,
  home_goalies_powerplay_goals_against integer,
  home_goalies_shorthanded_goals_against integer,
  home_goalies_total_shots_against integer,
  away_forwards_count integer,
  away_forwards_goals integer,
  away_forwards_assists integer,
  away_forwards_hits integer,
  away_forwards_pim integer,
  away_forwards_blockedshots integer,
  away_forwards_shifts integer,
  away_forwards_plusminus integer,
  away_forwards_giveaways integer,
  away_forwards_takeaways integer,
  away_defense_count integer,
  away_defense_goals integer,
  away_defense_assists integer,
  away_defense_hits integer,
  away_defense_pim integer,
  away_defense_blockedshots integer,
  away_defense_shifts integer,
  away_defense_plusminus integer,
  away_defense_giveaways integer,
  away_defense_takeaways integer,
  away_forwards_avg_toi double precision,
  away_defense_avg_toi double precision,
  away_forwards_total_toi double precision,
  away_defense_total_toi double precision,
  away_skaters_total_toi double precision,
  away_total_plusminus integer,
  away_total_giveaways integer,
  away_total_takeaways integer,
  away_goalies_count integer,
  away_goalies_saves integer,
  away_goalies_savepctg double precision,
  away_goalies_evenstrength_shots_against integer,
  away_goalies_powerplay_shots_against integer,
  away_goalies_shorthanded_shots_against integer,
  away_goalies_evenstrength_goals_against integer,
  away_goalies_powerplay_goals_against integer,
  away_goalies_shorthanded_goals_against integer,
  away_goalies_total_shots_against integer,
  home_puck_control_total double precision,
  away_puck_control_total double precision,
  referee_1 text,
  referee_2 text,
  linesman_1 text,
  linesman_2 text,
  home_coach text,
  away_coach text,
  home_faceoffwinningpctg double precision,
  away_faceoffwinningpctg double precision,
  home_powerplaypctg double precision,
  away_powerplaypctg double precision,
  home_pim integer,
  away_pim integer,
  home_hits integer,
  away_hits integer,
  home_blockedshots integer,
  away_blockedshots integer,
  home_giveaways integer,
  away_giveaways integer,
  home_takeaways integer,
  away_takeaways integer,
  game_time text,
  home_team text,
  away_team text,
  neutral_site boolean,
  home_powerplay_chances integer,
  away_powerplay_chances integer,
  total_shots_home integer,
  total_shots_away integer,
  game_duration double precision,
  CONSTRAINT nhl_games_extended_pkey PRIMARY KEY (game_id)
);
CREATE TABLE public.scratches (
  id integer NOT NULL DEFAULT nextval('scratches_id_seq'::regclass),
  game_id text,
  team_type text CHECK (team_type = ANY (ARRAY['home'::text, 'away'::text])),
  player_name text,
  scratch_order integer,
  CONSTRAINT scratches_pkey PRIMARY KEY (id),
  CONSTRAINT scratches_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.nhl_games_extended(game_id)
);
CREATE TABLE public.stars (
  id integer NOT NULL DEFAULT nextval('stars_id_seq'::regclass),
  game_id text,
  star_number integer CHECK (star_number >= 1 AND star_number <= 3),
  player_id text,
  player_name text,
  team text,
  position text,
  goals integer,
  assists integer,
  points integer,
  CONSTRAINT stars_pkey PRIMARY KEY (id),
  CONSTRAINT stars_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.nhl_games_extended(game_id)
);
```

Скрипт заполняет следующие таблицы:
- nhl_games_extended - основная таблица с расширенной статистикой матчей
- game_period_stats - статистика по периодам матча
- scratches - информация о запасных игроках
- stars - информация о звездах матча

### Логирование
Скрипт использует цветное логирование в консоль с указанием времени выполнения операций.

Производительность:
- Для ускорения загрузки данных реализовано:
- Многопоточная обработка (до 16 потоков)
- Пакетная вставка данных в PostgreSQL
- Прогресс-бары для отслеживания выполнения
