from nhlpy import NHLClient
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
from colorama import init, Fore, Style
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_batch
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.set_option('display.max_rows', None)  # Показывать все строки
pd.set_option('display.max_columns', None)  # Показывать все столбцы
pd.set_option('display.width', None)  # Автоматически подбирать ширину
pd.set_option('display.max_colwidth', None)  # Показывать полное содержимое ячеек

# Инициализация colorama
init()

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('nhlpy').setLevel(logging.WARNING)

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        return f"{Fore.LIGHTWHITE_EX}{message}{Style.RESET_ALL}"

handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
    force=True
)

# Инициализация PSQL клиента
DB_CONFIG = {
    "host": "localhost",
    "database": "NHL_project",
    "user": "postgres",
    "password": "89137961052",
    "port": "5432"
}

# Инициализация клиента NHL
client = NHLClient(verbose=False)

def get_db_connection():
    """Создаёт подключение к локальной PostgreSQL БД"""
    return psycopg2.connect(**DB_CONFIG)

# Функции get_game_data, flatten_data и другие остаются без изменений
def get_game_data(game_id, data_type):
    """Получение данных определенного типа для конкретной игры"""
    try:
        if data_type == 'boxscore':
            return client.game_center.boxscore(game_id)
        elif data_type == 'play_by_play':
            return client.game_center.play_by_play(game_id)
        elif data_type == 'landing':
            return client.game_center.landing(game_id)
        elif data_type == 'right_rail':
            return client.game_center.right_rail(game_id)
        elif data_type == 'schedule_by_date':
            date = datetime.strptime(game_id[:8], "%Y%m%d").strftime("%Y-%m-%d")
            return client.schedule.get_schedule(date=date)
    except Exception as e:
        logging.info(f"Ошибка при получении {data_type} для игры {game_id}: {e}")
        return None

def flatten_data(raw_data, data_type):
    """Преобразование сырых данных в плоскую структуру"""
    if not raw_data:
        return None

    if data_type == 'boxscore':
        return flatten_boxscore(raw_data)
    elif data_type == 'play_by_play':
        return flatten_play_by_play(raw_data)
    elif data_type == 'landing':
        return flatten_landing(raw_data)
    elif data_type == 'right_rail':
        return flatten_right_rail(raw_data)
    elif data_type == 'schedule_by_date':
        return flatten_schedule_by_date(raw_data)
    return None

def flatten_boxscore(boxscore):
    """Преобразование boxscore в плоскую таблицу с расширенной статистикой"""
    if not boxscore:
        return None

    def parse_goalie_stat(stat_value, default=0):
        """Парсит статистику вратаря, которая может быть в формате '21/23' или числом"""
        if isinstance(stat_value, str) and '/' in stat_value:
            return int(stat_value.split('/')[0])
        try:
            return int(stat_value) if stat_value is not None else default
        except (ValueError, TypeError):
            return default

    def parse_toi(toi_str):
        """Конвертирует время на льду (MM:SS) в минуты с десятичной дробью"""
        if not toi_str or toi_str == '0:00':
            return 0.0
        try:
            minutes, seconds = map(int, toi_str.split(':'))
            return round(minutes + seconds / 60, 2)
        except:
            return 0.0

    flat_data = {
        'game_id': str(boxscore.get('id')),
        'game_date': boxscore.get('gameDate'),
        'game_type': int(boxscore.get('gameType', 0)),
        'game_state': boxscore.get('gameState'),
        'venue': boxscore.get('venue', {}).get('default'),
        'period_type': boxscore.get('periodDescriptor', {}).get('periodType'),
        'period_number': int(boxscore.get('periodDescriptor', {}).get('number', 0)),
    }

    for team in ['homeTeam', 'awayTeam']:
        prefix = team.replace('Team', '_').lower()
        team_data = boxscore[team]
        flat_data.update({
            f'{prefix}id': int(team_data.get('id', 0)),
            f'{prefix}abbrev': team_data.get('abbrev'),
            f'{prefix}score': int(team_data.get('score', 0)),
            f'{prefix}sog': int(team_data.get('sog', 0)),
            f'{prefix}name': team_data.get('commonName', {}).get('default'),
            f'{prefix}city': team_data.get('placeName', {}).get('default'),
        })

    if 'playerByGameStats' in boxscore:
        for team in ['homeTeam', 'awayTeam']:
            if team in boxscore['playerByGameStats']:
                prefix = team.replace('Team', '_').lower()
                team_stats = boxscore['playerByGameStats'][team]
                total_plus_minus = 0
                total_giveaways = 0
                total_takeaways = 0

                forwards_toi = []
                defense_toi = []
                total_forwards_toi = 0
                total_defense_toi = 0

                for position in ['forwards', 'defense']:
                    if position in team_stats:
                        players = team_stats[position]
                        position_plus_minus = sum(int(p.get('plusMinus', 0)) for p in players)
                        position_giveaways = sum(int(p.get('giveaways', 0)) for p in players)
                        position_takeaways = sum(int(p.get('takeaways', 0)) for p in players)
                        position_total_toi = sum(parse_toi(p.get('toi', '0:00')) for p in players)

                        total_plus_minus += position_plus_minus
                        total_giveaways += position_giveaways
                        total_takeaways += position_takeaways

                        if position == 'forwards':
                            total_forwards_toi = position_total_toi
                        else:
                            total_defense_toi = position_total_toi

                        for p in players:
                            toi = parse_toi(p.get('toi', '0:00'))
                            if position == 'forwards':
                                forwards_toi.append(toi)
                            else:
                                defense_toi.append(toi)

                        flat_data.update({
                            f'{prefix}{position}_count': len(players),
                            f'{prefix}{position}_goals': sum(int(p.get('goals', 0)) for p in players),
                            f'{prefix}{position}_assists': sum(int(p.get('assists', 0)) for p in players),
                            f'{prefix}{position}_hits': sum(int(p.get('hits', 0)) for p in players),
                            f'{prefix}{position}_pim': sum(int(p.get('pim', 0)) for p in players),
                            f'{prefix}{position}_blockedshots': sum(int(p.get('blockedShots', 0)) for p in players),
                            f'{prefix}{position}_shifts': sum(int(p.get('shifts', 0)) for p in players),
                            f'{prefix}{position}_plusminus': position_plus_minus,
                            f'{prefix}{position}_giveaways': position_giveaways,
                            f'{prefix}{position}_takeaways': position_takeaways,
                        })

                flat_data.update({
                    f'{prefix}forwards_avg_toi': round(sum(forwards_toi) / len(forwards_toi), 2) if forwards_toi else 0,
                    f'{prefix}defense_avg_toi': round(sum(defense_toi) / len(defense_toi), 2) if defense_toi else 0,
                    f'{prefix}forwards_total_toi': total_forwards_toi,
                    f'{prefix}defense_total_toi': total_defense_toi,
                    f'{prefix}skaters_total_toi': total_forwards_toi + total_defense_toi,
                })

                flat_data[f'{prefix}total_plusminus'] = total_plus_minus
                flat_data[f'{prefix}total_giveaways'] = total_giveaways
                flat_data[f'{prefix}total_takeaways'] = total_takeaways

                if 'goalies' in team_stats:
                    goalies = team_stats['goalies']
                    if goalies:
                        flat_data.update({
                            f'{prefix}goalies_count': len(goalies),
                            f'{prefix}goalies_saves': sum(parse_goalie_stat(g.get('saves', 0)) for g in goalies),
                            f'{prefix}goalies_savepctg': sum(float(g.get('savePctg', 0)) for g in goalies) / len(goalies),
                            f'{prefix}goalies_evenstrength_shots_against': sum(parse_goalie_stat(g.get('evenStrengthShotsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_powerplay_shots_against': sum(parse_goalie_stat(g.get('powerPlayShotsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_shorthanded_shots_against': sum(parse_goalie_stat(g.get('shorthandedShotsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_evenstrength_goals_against': sum(parse_goalie_stat(g.get('evenStrengthGoalsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_powerplay_goals_against': sum(parse_goalie_stat(g.get('powerPlayGoalsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_shorthanded_goals_against': sum(parse_goalie_stat(g.get('shorthandedGoalsAgainst', 0)) for g in goalies),
                            f'{prefix}goalies_total_shots_against': sum(parse_goalie_stat(g.get('shotsAgainst', 0)) for g in goalies),
                        })
                    else:
                        flat_data.update({
                            f'{prefix}goalies_count': 0,
                            f'{prefix}goalies_saves': 0,
                            f'{prefix}goalies_savepctg': 0.0,
                            f'{prefix}goalies_evenstrength_shots_against': 0,
                            f'{prefix}goalies_powerplay_shots_against': 0,
                            f'{prefix}goalies_shorthanded_shots_against': 0,
                            f'{prefix}goalies_evenstrength_goals_against': 0,
                            f'{prefix}goalies_powerplay_goals_against': 0,
                            f'{prefix}goalies_shorthanded_goals_against': 0,
                            f'{prefix}goalies_total_shots_against': 0,
                        })

    return flat_data

def flatten_play_by_play(play_by_play):
    """Преобразование play-by-play в плоскую таблицу"""
    if not play_by_play or 'plays' not in play_by_play:
        return None

    pbp_data = []
    game_id = play_by_play.get('id')

    # Получаем названия команд
    home_team = play_by_play.get('homeTeam', {}).get('abbrev', '')
    away_team = play_by_play.get('awayTeam', {}).get('abbrev', '')

    for play in play_by_play['plays']:
        play_data = {
            'game_id': game_id,
            'home_team': home_team,
            'away_team': away_team,
            'event_id': play.get('eventId'),
            'period': play.get('periodDescriptor', {}).get('number'),
            'period_type': play.get('periodDescriptor', {}).get('periodType'),
            'time_in_period': play.get('timeInPeriod'),
            'time_remaining': play.get('timeRemaining'),
            'event_type': play.get('typeCode'),
            'event_description': play.get('typeDescKey'),
            'team_id': play.get('details', {}).get('eventOwnerTeamId'),
            'x_coord': play.get('details', {}).get('xCoord'),
            'y_coord': play.get('details', {}).get('yCoord'),
            'player1_id': play.get('details', {}).get('scoringPlayerId'),
            'player2_id': play.get('details', {}).get('assist1PlayerId'),
            'player3_id': play.get('details', {}).get('assist2PlayerId'),
            'shot_type': play.get('details', {}).get('shotType'),
            'penalty_minutes': play.get('details', {}).get('penaltyMinutes'),
        }
        pbp_data.append(play_data)

    return pbp_data

def flatten_landing(landing):
    """Преобразование landing page данных в плоскую таблицу"""
    if not landing:
        return None

    flat_data = {
        'game_id': landing.get('id'),
        'game_date': landing.get('gameDate'),
        'game_type': landing.get('gameType'),
        'game_state': landing.get('gameState'),
        'venue': landing.get('venue', {}).get('default'),
        'period_type': landing.get('periodDescriptor', {}).get('periodType'),
        'period_number': landing.get('periodDescriptor', {}).get('number'),
    }

    for team in ['homeTeam', 'awayTeam']:
        if team in landing:
            prefix = team.replace('Team', '_').lower()
            team_data = landing[team]
            flat_data.update({
                f'{prefix}id': team_data.get('id'),
                f'{prefix}abbrev': team_data.get('abbrev'),
                f'{prefix}score': team_data.get('score'),
                f'{prefix}sog': team_data.get('sog'),
                f'{prefix}name': team_data.get('commonName', {}).get('default'),
                f'{prefix}city': team_data.get('placeName', {}).get('default'),
            })

    if 'summary' in landing:
        summary = landing['summary']
        if 'threeStars' in summary and summary['threeStars']:
            for i, star in enumerate(summary['threeStars'][:3], 1):
                # Очищаем имя звезды, извлекая только default
                star_name = star.get('name', {})
                if isinstance(star_name, dict) and 'default' in star_name:
                    star_name = star_name['default']
                flat_data.update({
                    f'star_{i}_id': star.get('playerId'),
                    f'star_{i}_name': star_name,
                    f'star_{i}_team': star.get('teamAbbrev'),
                    f'star_{i}_position': star.get('position'),
                    f'star_{i}_goals': star.get('goals'),
                    f'star_{i}_assists': star.get('assists'),
                    f'star_{i}_points': star.get('points'),
                })

    return flat_data

def flatten_right_rail(right_rail, game_id):
    flat_data = {'game_id': game_id}
    if 'gameInfo' in right_rail:
        game_info = right_rail['gameInfo']
        # Рефери
        referees = game_info.get('referees', [])
        for i in range(1, 3):  # Всегда обрабатываем двух рефери
            if i <= len(referees):
                ref_name = referees[i-1].get('default', '')
                if isinstance(ref_name, dict):
                    ref_name = ref_name.get('default', '')
                flat_data[f'referee_{i}'] = ref_name
            else:
                flat_data[f'referee_{i}'] = 'N/A'
        # Линейные судьи
        linesmen = game_info.get('linesmen', [])
        for i in range(1, 3):
            if i <= len(linesmen):
                linesman_name = linesmen[i-1].get('default', '')
                if isinstance(linesman_name, dict):
                    linesman_name = linesman_name.get('default', '')
                flat_data[f'linesman_{i}'] = linesman_name
            else:
                flat_data[f'linesman_{i}'] = 'N/A'
        # Обработка тренеров и запасных
        for team in ['homeTeam', 'awayTeam']:
            if team in game_info:
                prefix = team.replace('Team', '_').lower()
                team_data = game_info[team]
                if 'headCoach' in team_data:
                    coach = team_data['headCoach'].get('default')
                    if isinstance(coach, dict):
                        coach = coach.get('default', '')
                    flat_data[f'{prefix}coach'] = coach
                if 'scratches' in team_data:
                    scratches = []
                    for p in team_data['scratches']:
                        first_name = p.get('firstName', {}).get('default', '') if isinstance(p.get('firstName'), dict) else p.get('firstName', '')
                        last_name = p.get('lastName', {}).get('default', '') if isinstance(p.get('lastName'), dict) else p.get('lastName', '')
                        full_name = f"{first_name} {last_name}".strip()
                        if full_name:
                            scratches.append(full_name)
                    for i, scratch in enumerate(scratches, 1):
                        flat_data[f'{prefix}scratches_{i}'] = scratch
    if 'teamGameStats' in right_rail:
        for stat in right_rail['teamGameStats']:
            category = stat.get('category', '').lower().replace(' ', '_')
            flat_data[f'home_{category}'] = stat.get('homeValue')
            flat_data[f'away_{category}'] = stat.get('awayValue')
        # Обработка голов по периодам
        if 'linescore' in right_rail and 'byPeriod' in right_rail['linescore']:
            for period in right_rail['linescore']['byPeriod']:
                period_num = period['periodDescriptor']['number']
                flat_data[f'goals_home_p{period_num}'] = period['home']
                flat_data[f'goals_away_p{period_num}'] = period['away']
        # Обработка бросков по периодам
        if 'shotsByPeriod' in right_rail:
            for period in right_rail['shotsByPeriod']:
                period_num = period['periodDescriptor']['number']
                flat_data[f'shots_home_p{period_num}'] = period['home']
                flat_data[f'shots_away_p{period_num}'] = period['away']
    return flat_data

def flatten_schedule_by_date(schedule):
    """Преобразование schedule_by_date данных в плоскую таблицу"""
    if not schedule or 'games' not in schedule:
        return None

    games_data = []
    for game in schedule['games']:
        # Обработка времени начала матча
        start_time = game.get('startTimeUTC')
        try:
            # Преобразуем строку времени в datetime объект
            dt = pd.to_datetime(start_time)
            # Извлекаем только время (часы:минуты)
            game_time = dt.strftime('%H:%M')
        except:
            game_time = None

        game_data = {
            'game_id': game.get('id'),
            'game_date': game.get('startTimeUTC'),  # Полная дата и время
            'game_time': game_time,  # Только время начала (HH:MM)
            'game_type': game.get('gameType'),
            'game_state': game.get('gameState'),
            'venue': game.get('venue', {}).get('default'),
            'home_team': game.get('homeTeam', {}).get('abbrev'),
            'away_team': game.get('awayTeam', {}).get('abbrev'),
            'neutral_site': game.get('neutralSite'),
        }
        games_data.append(game_data)

    return games_data


def get_games_for_date_range(start_date, end_date):
    """Получение списка игр за период параллельно"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
    game_ids = []

    def fetch_schedule(date):
        date_str = date.strftime("%Y-%m-%d")
        try:
            schedule = client.schedule.get_schedule(date=date_str)
            return [str(game["id"]) for game in schedule["games"]] if schedule and "games" in schedule else []
        except Exception as e:
            logging.info(f"Ошибка при получении расписания на {date_str}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(tqdm(executor.map(fetch_schedule, date_list), total=len(date_list),
                            bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:3.0f}%]"))

    for result in results:
        game_ids.extend(result)

    return game_ids

def calculate_game_duration(pbp_df, game_type):
    """
    Рассчитывает длительность игры в минутах на основе DataFrame play-by-play.
    Возвращает 60.0 (минут) по умолчанию, если не удается определить длительность.
    """
    if pbp_df.empty:
        return 60.0  # Значение по умолчанию для регулярного сезона

    try:
        # Ищем событие окончания игры (game-end)
        game_end = pbp_df[pbp_df['event_description'] == 'game-end']

        # Если не нашли game-end, берем последнее событие period-end
        if game_end.empty:
            game_end = pbp_df[pbp_df['event_description'] == 'period-end'].tail(1)

        # Если все равно не нашли, берем последнее событие в матче
        if game_end.empty:
            game_end = pbp_df.tail(1)

        if not game_end.empty:
            last_event = game_end.iloc[0]
            period = last_event['period']
            time_in_period = last_event['time_in_period']

            # Преобразуем время в минуты с десятичной частью
            minutes, seconds = map(int, time_in_period.split(':'))
            period_minutes = minutes + seconds / 60

            # Рассчитываем общее время
            if period <= 3:
                total_minutes = (period - 1) * 20 + period_minutes
            else:
                # Определяем длину одного овертайма
                ot_length = 5 if game_type in [1, 2] else 20  # 5 минут для регулярки и плей-офф
                total_minutes = 60 + (period - 3 - 1) * ot_length + period_minutes

            return round(total_minutes, 2)

        return 60.0  # Значение по умолчанию

    except Exception as e:
        logging.info(f"Ошибка при расчете длительности игры: {e}")
        return 60.0  # Значение по умолчанию


def insert_data_to_postgres(combined_df):
    """Загружает данные из combined_df в таблицы локальной PostgreSQL."""

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Подготовка данных для nhl_games_extended
        nhl_games_extended_columns = [
            'game_id', 'game_date', 'game_type', 'venue', 'period_type', 'period_number',
            'home_id', 'home_abbrev', 'home_score', 'home_sog', 'home_name', 'home_city',
            'away_id', 'away_abbrev', 'away_score', 'away_sog', 'away_name', 'away_city',
            'home_forwards_count', 'home_forwards_goals', 'home_forwards_assists', 'home_forwards_hits',
            'home_forwards_pim', 'home_forwards_blockedshots', 'home_forwards_shifts', 'home_forwards_plusminus',
            'home_forwards_giveaways', 'home_forwards_takeaways', 'home_defense_count', 'home_defense_goals',
            'home_defense_assists', 'home_defense_hits', 'home_defense_pim', 'home_defense_blockedshots',
            'home_defense_shifts', 'home_defense_plusminus', 'home_defense_giveaways', 'home_defense_takeaways',
            'home_forwards_avg_toi', 'home_defense_avg_toi', 'home_forwards_total_toi', 'home_defense_total_toi',
            'home_skaters_total_toi', 'home_total_plusminus', 'home_total_giveaways', 'home_total_takeaways',
            'home_goalies_count', 'home_goalies_saves', 'home_goalies_savepctg',
            'home_goalies_evenstrength_shots_against',
            'home_goalies_powerplay_shots_against', 'home_goalies_shorthanded_shots_against',
            'home_goalies_evenstrength_goals_against', 'home_goalies_powerplay_goals_against',
            'home_goalies_shorthanded_goals_against', 'home_goalies_total_shots_against',
            'away_forwards_count', 'away_forwards_goals', 'away_forwards_assists', 'away_forwards_hits',
            'away_forwards_pim', 'away_forwards_blockedshots', 'away_forwards_shifts', 'away_forwards_plusminus',
            'away_forwards_giveaways', 'away_forwards_takeaways', 'away_defense_count', 'away_defense_goals',
            'away_defense_assists', 'away_defense_hits', 'away_defense_pim', 'away_defense_blockedshots',
            'away_defense_shifts', 'away_defense_plusminus', 'away_defense_giveaways', 'away_defense_takeaways',
            'away_forwards_avg_toi', 'away_defense_avg_toi', 'away_forwards_total_toi', 'away_defense_total_toi',
            'away_skaters_total_toi', 'away_total_plusminus', 'away_total_giveaways', 'away_total_takeaways',
            'away_goalies_count', 'away_goalies_saves', 'away_goalies_savepctg',
            'away_goalies_evenstrength_shots_against',
            'away_goalies_powerplay_shots_against', 'away_goalies_shorthanded_shots_against',
            'away_goalies_evenstrength_goals_against', 'away_goalies_powerplay_goals_against',
            'away_goalies_shorthanded_goals_against', 'away_goalies_total_shots_against',
            'referee_1', 'referee_2', 'linesman_1', 'linesman_2',
            'home_coach', 'away_coach', 'home_faceoffwinningpctg', 'away_faceoffwinningpctg', 'home_powerplaypctg',
            'away_powerplaypctg', 'home_pim', 'away_pim', 'home_hits', 'away_hits', 'home_blockedshots',
            'away_blockedshots',
            'home_giveaways', 'away_giveaways', 'home_takeaways', 'away_takeaways', 'game_time', 'home_team',
            'away_team',
            'neutral_site', 'home_powerplay_chances', 'away_powerplay_chances', 'total_shots_home', 'total_shots_away',
            'game_duration'
        ]

        # Создаем копию DataFrame для обработки
        nhl_games_extended_df = combined_df[nhl_games_extended_columns].copy()

        # Определяем колонки, которые должны быть integer
        integer_columns = [
            'game_type', 'period_number', 'home_id', 'home_score', 'home_sog', 'away_id', 'away_score', 'away_sog',
            'home_forwards_count', 'home_forwards_goals', 'home_forwards_assists', 'home_forwards_hits',
            'home_forwards_pim', 'home_forwards_blockedshots', 'home_forwards_shifts', 'home_forwards_plusminus',
            'home_forwards_giveaways', 'home_forwards_takeaways', 'home_defense_count', 'home_defense_goals',
            'home_defense_assists', 'home_defense_hits', 'home_defense_pim', 'home_defense_blockedshots',
            'home_defense_shifts', 'home_defense_plusminus', 'home_defense_giveaways', 'home_defense_takeaways',
            'home_goalies_count', 'home_goalies_saves', 'home_goalies_evenstrength_shots_against',
            'home_goalies_powerplay_shots_against', 'home_goalies_shorthanded_shots_against',
            'home_goalies_evenstrength_goals_against', 'home_goalies_powerplay_goals_against',
            'home_goalies_shorthanded_goals_against', 'home_goalies_total_shots_against',
            'away_forwards_count', 'away_forwards_goals', 'away_forwards_assists', 'away_forwards_hits',
            'away_forwards_pim', 'away_forwards_blockedshots', 'away_forwards_shifts', 'away_forwards_plusminus',
            'away_forwards_giveaways', 'away_forwards_takeaways', 'away_defense_count', 'away_defense_goals',
            'away_defense_assists', 'away_defense_hits', 'away_defense_pim', 'away_defense_blockedshots',
            'away_defense_shifts', 'away_defense_plusminus', 'away_defense_giveaways', 'away_defense_takeaways',
            'away_goalies_count', 'away_goalies_saves', 'away_goalies_evenstrength_shots_against',
            'away_goalies_powerplay_shots_against', 'away_goalies_shorthanded_shots_against',
            'away_goalies_evenstrength_goals_against', 'away_goalies_powerplay_goals_against',
            'away_goalies_shorthanded_goals_against', 'away_goalies_total_shots_against',
            'home_pim', 'away_pim', 'home_hits', 'away_hits', 'home_blockedshots', 'away_blockedshots',
            'home_giveaways', 'away_giveaways', 'home_takeaways', 'away_takeaways',
            'home_powerplay_chances', 'away_powerplay_chances', 'total_shots_home', 'total_shots_away',
            'home_total_giveaways', 'away_total_giveaways', 'home_total_takeaways', 'away_total_takeaways',
            'home_total_plusminus', 'away_total_plusminus'
        ]

        # Приводим колонки к integer
        for col in integer_columns:
            if col in nhl_games_extended_df.columns:
                nhl_games_extended_df[col] = pd.to_numeric(nhl_games_extended_df[col], errors='coerce').fillna(0).round(
                    0).astype(int)

        # Приводим колонки к float для дробных чисел
        float_columns = [
            'home_forwards_avg_toi', 'home_defense_avg_toi', 'home_forwards_total_toi', 'home_defense_total_toi',
            'home_skaters_total_toi', 'away_forwards_avg_toi', 'away_defense_avg_toi', 'away_forwards_total_toi',
            'away_defense_total_toi', 'away_skaters_total_toi', 'home_goalies_savepctg', 'away_goalies_savepctg',
            'home_faceoffwinningpctg', 'away_faceoffwinningpctg',
            'home_powerplaypctg', 'away_powerplaypctg', 'game_duration'
        ]
        for col in float_columns:
            if col in nhl_games_extended_df.columns:
                nhl_games_extended_df[col] = nhl_games_extended_df[col].fillna(0.0).astype(float)

        # Приводим game_id к строке
        nhl_games_extended_df['game_id'] = nhl_games_extended_df['game_id'].astype(str)

        # Преобразуем game_date в формат даты
        nhl_games_extended_df['game_date'] = pd.to_datetime(nhl_games_extended_df['game_date']).dt.strftime('%Y-%m-%d')

        # Преобразуем neutral_site к boolean
        nhl_games_extended_df['neutral_site'] = nhl_games_extended_df['neutral_site'].astype(bool)

        # Подготовка данных для вставки
        nhl_games_extended_data = nhl_games_extended_df.to_dict(orient='records')

        # Вставка в nhl_games_extended
        insert_query = """
            INSERT INTO nhl_games_extended ({})
            VALUES ({})
            ON CONFLICT (game_id) DO NOTHING
        """.format(
            ', '.join(nhl_games_extended_columns),
            ', '.join(['%s'] * len(nhl_games_extended_columns)))

        chunk_size = 50
        with tqdm(total=len(nhl_games_extended_data)) as pbar:
            for i in range(0, len(nhl_games_extended_data), chunk_size):
                chunk = nhl_games_extended_data[i:i + chunk_size]
                values = [tuple(record[col] for col in nhl_games_extended_columns) for record in chunk]

                try:
                    execute_batch(cur, insert_query, values)
                    conn.commit()
                    pbar.update(len(chunk))
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Ошибка при вставке чанка: {e}")
                    pbar.update(len(chunk))
                    continue

        # 2. Вставка в game_period_stats
        game_period_stats_data = []
        for _, row in combined_df.iterrows():
            for period in range(1, 6):  # До 5 периодов (3 основных + 2 овертайма)
                if f'goals_home_p{period}' in row and pd.notna(row[f'goals_home_p{period}']):
                    period_type = 'REG' if period <= 3 else 'OT'
                    period_data = {
                        'game_id': str(row['game_id']),
                        'period_number': int(period),
                        'period_type': period_type,
                        'goals_home': int(row.get(f'goals_home_p{period}', 0)),
                        'goals_away': int(row.get(f'goals_away_p{period}', 0)),
                        'shots_home': int(row.get(f'shots_home_p{period}', 0)),
                        'shots_away': int(row.get(f'shots_away_p{period}', 0)),
                        'home_pim': int(row.get('home_pim', 0)),
                        'away_pim': int(row.get('away_pim', 0)),
                        'home_hits': int(row.get('home_hits', 0)),
                        'away_hits': int(row.get('away_hits', 0))
                    }
                    game_period_stats_data.append(period_data)

        if game_period_stats_data:
            insert_period_query = """
                                  INSERT INTO game_period_stats (game_id, period_number, period_type,
                                                                 goals_home, goals_away, shots_home, shots_away,
                                                                 home_pim, away_pim, home_hits, away_hits)
                                  VALUES (%(game_id)s, %(period_number)s, %(period_type)s, %(goals_home)s,
                                          %(goals_away)s, %(shots_home)s, %(shots_away)s,
                                          %(home_pim)s, %(away_pim)s, %(home_hits)s, \
                                          %(away_hits)s)
                                  ON CONFLICT DO NOTHING \
                                  """
            execute_batch(cur, insert_period_query, game_period_stats_data)
            conn.commit()
            logging.info(f"Данные успешно загружены в game_period_stats: {len(game_period_stats_data)} записей")

        # 3. Вставка в scratches
        scratches_data = []
        for _, row in combined_df.iterrows():
            for team_type in ['home', 'away']:
                for i in range(1, 15):  # До 14 запасных на команду
                    scratch_col = f'{team_type}_scratches_{i}'
                    if scratch_col in row and row[scratch_col] != 'No More Scratches':
                        scratches_data.append({
                            'game_id': str(row['game_id']),
                            'team_type': team_type,
                            'player_name': str(row[scratch_col]),
                            'scratch_order': int(i)
                        })

        if scratches_data:
            insert_scratches_query = """
                                     INSERT INTO scratches (game_id, team_type, player_name, scratch_order)
                                     VALUES (%(game_id)s, %(team_type)s, %(player_name)s, %(scratch_order)s)
                                     ON CONFLICT DO NOTHING \
                                     """
            execute_batch(cur, insert_scratches_query, scratches_data)
            conn.commit()
            logging.info(f"Данные успешно загружены в scratches: {len(scratches_data)} записей")

        # 4. Вставка в stars
        stars_data = []
        for _, row in combined_df.iterrows():
            for star_num in [1, 2, 3]:
                star_id_col = f'star_{star_num}_id'
                star_name_col = f'star_{star_num}_name'

                # Проверяем, что star_id существует, не NaN, не 'N/A' и является числом
                if (star_id_col in row and pd.notna(row[star_id_col]) and
                        star_name_col in row and pd.notna(row[star_name_col]) and
                        str(row[star_id_col]) != 'N/A' and str(row[star_id_col]).replace('.', '').isdigit()):
                    try:
                        player_id = int(row[star_id_col])  # Преобразуем в int
                        # Обрабатываем 'N/A' для goals, assists, points
                        goals = int(row.get(f'star_{star_num}_goals', 0)) if str(
                            row.get(f'star_{star_num}_goals', 0)) != 'N/A' else 0
                        assists = int(row.get(f'star_{star_num}_assists', 0)) if str(
                            row.get(f'star_{star_num}_assists', 0)) != 'N/A' else 0
                        points = int(row.get(f'star_{star_num}_points', 0)) if str(
                            row.get(f'star_{star_num}_points', 0)) != 'N/A' else 0
                        star_data = {
                            'game_id': str(row['game_id']),
                            'star_number': int(star_num),
                            'player_id': player_id,
                            'player_name': str(row[star_name_col]),
                            'team': str(row.get(f'star_{star_num}_team', 'N/A')),
                            'position': str(row.get(f'star_{star_num}_position', 'N/A')),
                            'goals': goals,
                            'assists': assists,
                            'points': points
                        }
                        stars_data.append(star_data)
                    except ValueError as e:
                        logging.warning(f"Пропущен player_id '{row[star_id_col]}' для game_id {row['game_id']}: {e}")
                        continue

        if stars_data:
            insert_stars_query = """
                                 INSERT INTO stars (game_id, star_number, player_id, player_name,
                                                    team, position, goals, assists, points)
                                 VALUES (%(game_id)s, %(star_number)s, %(player_id)s, %(player_name)s,
                                         %(team)s, %(position)s, %(goals)s, %(assists)s, %(points)s)
                                 ON CONFLICT DO NOTHING \
                                 """
            execute_batch(cur, insert_stars_query, stars_data)
            conn.commit()
            logging.info(f"Данные успешно загружены в stars: {len(stars_data)} записей")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при загрузке данных: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    start_date = "2025-09-21"
    end_date = "2025-09-21"

    logging.info(f"Получаем данные об играх с {start_date} по {end_date}...")
    game_ids = get_games_for_date_range(start_date, end_date)

    if not game_ids:
        logging.info("Не найдено игр за указанный период")
        exit()

    logging.info(f"Найдено {len(game_ids)} игр. Собираем данные...")

    data_types = ['boxscore', 'play_by_play', 'landing', 'right_rail']
    all_data = {data_type: [] for data_type in data_types}
    all_data['schedule_by_date'] = []

    # Функция для обработки одной игры
    def process_game(game_id):
        game_data = {}
        for data_type in data_types:
            raw_data = get_game_data(game_id, data_type)
            if raw_data:
                flat_data = flatten_data(raw_data, data_type) if data_type != 'right_rail' else flatten_right_rail(
                    raw_data, game_id)
                if flat_data:
                    game_data[data_type] = flat_data
        return game_data


    # Параллельная обработка игр с прогресс-баром
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(tqdm(executor.map(process_game, game_ids), total=len(game_ids),
                              bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:3.0f}%]"))

    # Собираем данные в all_data с прогресс-баром
    logging.info("Сборка данных...")
    with tqdm(total=len(results), bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:3.0f}%]") as pbar:
        for result in results:
            for data_type, data in result.items():
                if isinstance(data, list):
                    all_data[data_type].extend(data)
                else:
                    all_data[data_type].append(data)
            pbar.update(1)

    logging.info(f"Обработка {len(game_ids)} игр завершена! Переходим к обработке полученных данных...")

    # Обработка schedule_by_date с прогресс-баром
    logging.info("Получаем schedule_by_date...")
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - current).days + 1
    date_list = [current + timedelta(days=x) for x in range((end - current).days + 1)]


    def fetch_schedule_for_day(current_date):
        date_str = current_date.strftime("%Y-%m-%d")
        game_id_for_date = current_date.strftime("%Y%m%d") + "0001"
        schedule_data = get_game_data(game_id_for_date, 'schedule_by_date')
        if schedule_data:
            flat_schedule = flatten_data(schedule_data, 'schedule_by_date')
            if flat_schedule:
                return flat_schedule
        return []

    with ThreadPoolExecutor(max_workers=16) as executor:
        schedule_results = list(tqdm(
            executor.map(fetch_schedule_for_day, date_list),
            total=len(date_list),
            bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:3.0f}%]"
        ))

    # Собираем результаты
    for result in schedule_results:
        if result:
            all_data['schedule_by_date'].extend(result)

    # Создание DataFrame с прогресс-баром
    logging.info("Создание DataFrame...")
    dfs = {}
    with tqdm(total=len(all_data), bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:3.0f}%]") as pbar:
        for data_type in all_data:
            if all_data[data_type]:
                df = pd.DataFrame(all_data[data_type]) if isinstance(all_data[data_type][0], dict) else pd.DataFrame.from_records(all_data[data_type])
                dfs[data_type] = df
            pbar.update(1)

    if 'play_by_play' in dfs:
        for game_id in dfs['play_by_play']['game_id'].unique():
            game_pbp = dfs['play_by_play'][dfs['play_by_play']['game_id'] == game_id]
            game_boxscore = dfs['boxscore'][dfs['boxscore']['game_id'] == game_id]
            if not game_boxscore.empty:
                home_id = game_boxscore['home_id'].iloc[0]
                away_id = game_boxscore['away_id'].iloc[0]
                game_pbp = game_pbp.copy()
                game_pbp['home_id'] = home_id
                game_pbp['away_id'] = away_id

    if 'boxscore' in dfs and 'landing' in dfs and 'right_rail' in dfs and 'schedule_by_date' in dfs:
        try:
            for df in dfs.values():
                if 'game_id' in df.columns:
                    df['game_id'] = df['game_id'].astype(str)

            combined_df = dfs['boxscore'].merge(
                dfs['landing'], on='game_id', how='left', suffixes=('', '_landing')
            ).merge(
                dfs['right_rail'], on='game_id', how='left', suffixes=('', '_right_rail')
            ).merge(
                dfs['schedule_by_date'], on='game_id', how='left', suffixes=('', '_schedule')
            )

            # Заполняем NaN для всех числовых столбцов
            numeric_cols = combined_df.select_dtypes(include=['float64', 'int64']).columns
            combined_df[numeric_cols] = combined_df[numeric_cols].fillna(0)

            scratches_cols = [col for col in combined_df.columns if 'scratches_' in col]
            for col in scratches_cols:
                combined_df[col] = combined_df[col].fillna('No More Scratches')

            # Создаем новые столбцы в отдельном DataFrame
            new_columns = {}
            if 'home_powerplay' in combined_df.columns:
                new_columns['home_powerplay_chances'] = combined_df['home_powerplay'].apply(
                    lambda x: int(x.split('/')[1]) if isinstance(x, str) and '/' in x else 0
                ).astype(int)
                new_columns['away_powerplay_chances'] = combined_df['away_powerplay'].apply(
                    lambda x: int(x.split('/')[1]) if isinstance(x, str) and '/' in x else 0
                ).astype(int)

            if 'shots_home_p1' in combined_df.columns:
                new_columns['total_shots_home'] = combined_df[
                    [c for c in combined_df.columns if c.startswith('shots_home_p')]].sum(axis=1).round(0).astype(int)
                new_columns['total_shots_away'] = combined_df[
                    [c for c in combined_df.columns if c.startswith('shots_away_p')]].sum(axis=1).round(0).astype(int)

            # Объединяем новые столбцы с combined_df
            if new_columns:
                new_columns_df = pd.DataFrame(new_columns)
                combined_df = pd.concat([combined_df, new_columns_df], axis=1)

            # Удаляем исходные столбцы powerplay, если они есть
            combined_df = combined_df.drop(columns=['home_powerplay', 'away_powerplay'], errors='ignore')

            duplicate_cols = [
                'home_id_landing', 'home_abbrev_landing', 'home_score_landing', 'home_sog_landing',
                'home_name_landing', 'home_city_landing', 'away_id_landing', 'away_abbrev_landing',
                'away_score_landing', 'away_sog_landing', 'away_name_landing', 'away_city_landing',
                'game_type_schedule', 'game_state_schedule', 'venue_schedule',
                'game_date_landing', 'game_type_landing', 'game_state_landing',
                'venue_landing', 'period_type_landing', 'period_number_landing', 'game_date_schedule',
                'home_sog_right_rail', 'away_sog_right_rail', 'game_state'
            ]
            combined_df = combined_df.drop(columns=[col for col in duplicate_cols if col in combined_df.columns])

            numeric_cols = combined_df.select_dtypes(include=['float64', 'int64']).columns
            combined_df[numeric_cols] = combined_df[numeric_cols].fillna(0)

            text_cols = combined_df.select_dtypes(include=['object']).columns
            combined_df[text_cols] = combined_df[text_cols].fillna('N/A')

            if 'star_3_points' in combined_df.columns:
                combined_df['star_3_points'] = combined_df['star_3_points'].fillna(0)
            else:
                combined_df['star_3_points'] = 0

            if 'play_by_play' in dfs and 'boxscore' in dfs:
                durations = []
                for game_id in dfs['play_by_play']['game_id'].unique():
                    game_pbp = dfs['play_by_play'][dfs['play_by_play']['game_id'] == game_id]
                    game_type = dfs['boxscore'].loc[dfs['boxscore']['game_id'] == game_id, 'game_type'].values[0] if not dfs['boxscore'][dfs['boxscore']['game_id'] == game_id].empty else 2
                    duration = calculate_game_duration(game_pbp, game_type)
                    durations.append({'game_id': game_id, 'game_duration': duration})
                if durations:
                    durations_df = pd.DataFrame(durations)
                    combined_df = combined_df.merge(durations_df, on='game_id', how='left')
                    combined_df['game_duration'] = combined_df['game_duration'].fillna(60.0)

            # Добавляем вызов функции загрузки в PSQL
            insert_data_to_postgres(combined_df)

        except Exception as e:
            print(f"\nОшибка при создании комбинированной таблицы: {e}")
    else:
        print("\nНе удалось создать комбинированную таблицу (отсутствуют некоторые данные)")
