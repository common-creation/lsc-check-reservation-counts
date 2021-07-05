import subprocess
import json
import traceback
import datetime
from itertools import groupby
import collections
import argparse
import logging
import os


# 引数設定
parser = argparse.ArgumentParser(description='カレンダー実予約数チェック') 
parser.add_argument('-p',  '--profile',
                    default='lsc-dev-shibazaki',
                    help='AWS profile名')
parser.add_argument('--surveyCalendars',
                    default='lsc-shibazaki-survey-static-SurveyCalendars-FI2FQDEAO8M6',
                    help='例：lsc-dev-#####-survey-static-SurveyCalendars-############')
parser.add_argument('--surveyResults',
                    default='lsc-shibazaki-survey-static-SurveyResults-174JYZI0EURAP',
                    help='例：lsc-dev-#####-survey-static-SurveyResults-############')
parser.add_argument('--surveyId',
                    default='',
                    help='例：d7cf####-3a##-40##-9e##-e1ac09######',
                    nargs='*'
                    )
parser.add_argument('--categoryId',
                    default='category#007385',
                    help='例：category#000000')
parser.add_argument('--log',
                    default='diff_calendar.log',
                    help='ログ名')
args = parser.parse_args()

# 引数/定数定義
table_name_survey_results = args.surveyResults
table_name_survey_calendars = args.surveyCalendars
survey_ids = args.surveyId
category_id = args.categoryId
profile_name = args.profile
QUOTAS = 'quotas'
RESERVATION_COUNTS = 'reservationCounts'
VALUE = 'value'
SURVEY_ID = 'surveyId'
PARTITION_KEY = 'partitionKey'
CHECK = 'check'
USER_ID = 'userId'
GRATER_THAN_RESULTS = 'カレンダー予約数 > 帳票予約数'
GRATER_THAN_CALENDARS = 'カレンダー予約数 < 帳票予約数'
TAG_1 = 'tag1'
TAG_2 = 'tag2'
TAG_3 = 'tag3'


# ログ設定
logger  = logging.getLogger("logger")             #logger名loggerを取得
current_path =  os.path.dirname(__file__)
file_name = f'{profile_name}_{category_id}_{args.log}'
file_path = os.path.join(current_path, file_name)
handler = logging.FileHandler(filename=file_path, encoding='utf-8')  #ファイル出力
logger.setLevel(logging.DEBUG)                    #loggerとしてはDEBUGで
handler.setFormatter(logging.Formatter("%(asctime)s,%(message)s"))
logger.addHandler(handler)


def run_aws_cli_command(command):
    result = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]
    try:
        # 実行PCがwindows(デフォルトの文字コードがcp932)の場合
        result = result.decode('cp932')
    except UnicodeDecodeError:
        # 実行PCがmac(デフォルトの文字コードがutf-8)の場合
        result = result.decode('utf-8')
    return result


def get_items(data):
    try:
        parsed_data = json.loads(data)
        items = parsed_data.get('Items')
        return items
    except:
        traceback.print_exc()
        logger.info('【Error】: データの取得ができておりません。DB名、surveyId、categoryId、profile名に間違いがないことを確認してください。')


def count_list_element(list):
    counter = collections.Counter(list)
    return counter.most_common()


def get_calendar_id(category_id):
    """
    categoryIdを元に検索対象のcalendarIdを取得

    :calendar_id 引数のcategoryIdを元に取得されたcalendarId
    
    :return calendar_id. type is string.
    """
    search_attribute_values = json.dumps({
        ':pkey': {
            "S": 'categories'
        },
        ':skey': {
            'S': category_id
        }
    })
    command = []
    command.append('aws')
    command.append('dynamodb')
    command.append('query')
    command.append('--table-name')
    command.append(table_name_survey_calendars)
    command.append('--key-condition-expression')
    command.append('partitionKey=:pkey and sortKey=:skey')
    command.append('--expression-attribute-values')
    command.append(search_attribute_values)
    command.append('--profile')
    command.append(profile_name)

    try:
        # コマンドを実行/Itemsを取得
        result = run_aws_cli_command(command)
        items = get_items(result)
        if len(items) > 0:
            calendar_id = items[0]['calendarId']['S']
            return calendar_id
        else:
            return None
    except:
        traceback.print_exc()
        return None


def get_category_tag(category_id):
    """
    categoryIdを元に検索対象のtag1 ~ 3を取得
    """
    search_attribute_values = json.dumps({
        ':pkey': {
            "S": 'categories'
        },
        ':skey': {
            'S': category_id
        }
    })
    command = []
    command.append('aws')
    command.append('dynamodb')
    command.append('query')
    command.append('--table-name')
    command.append(table_name_survey_calendars)
    command.append('--key-condition-expression')
    command.append('partitionKey=:pkey and sortKey=:skey')
    command.append('--expression-attribute-values')
    command.append(search_attribute_values)
    command.append('--profile')
    command.append(profile_name)

    try:
        # コマンドを実行/Itemsを取得
        result = run_aws_cli_command(command)
        items = get_items(result)
        if len(items) > 0:
            tag1 = items[0][TAG_1]['S'].encode('utf-8').decode('utf-8')
            tag2 = items[0][TAG_2]['S'].encode('utf-8').decode('utf-8')
            tag3 = items[0][TAG_3]['S'].encode('utf-8').decode('utf-8')
            return f'{tag1} > {tag2} > {tag3}'
        else:
            return None
    except:
        traceback.print_exc()
        return None


def get_reservation_info(calendar_id):
    """
    SurveyCalendarsからcalendarIdを元に現在の予約数(reservationCounts)・予約の限度数(quotas)・予約日(date)を取得

    :calendar_id 引数のcategoryIdを元に取得されたcalendarId
    
    :return dictionary. 
    {
        
        "quotas": {
            date: {
                coma: int
            }
        },
        "reservationCounts": {
            date: {
                coma: int
            }
        }
    }
    """
    search_attribute_values = json.dumps({
        ':pkey': {
            "S": calendar_id
        }
    })
    command = []
    command.append('aws')
    command.append('dynamodb')
    command.append('query')
    command.append('--table-name')
    command.append(table_name_survey_calendars)
    command.append('--key-condition-expression')
    command.append('partitionKey=:pkey')
    command.append('--expression-attribute-values')
    command.append(search_attribute_values)
    command.append('--profile')
    command.append(profile_name)

    try:
        # コマンドを実行/Itemsを取得
        result = run_aws_cli_command(command)
        items = get_items(result)
        return_value = {
            QUOTAS: {},
            RESERVATION_COUNTS: {}
        }

        # そのまま返すと分かりにくいので整形する
        for item in items:
            quotas = item[QUOTAS]['M']
            reservation_counts = item[RESERVATION_COUNTS]['M']
            date = item['date']['S']
            arranged_quotas = {}
            arranged_reservation = {}

            for coma, value in quotas.items():
                arranged_quotas[coma] = int(value['N'])

            for coma, value in reservation_counts.items():
                arranged_reservation[coma] = int(value['N'])
            
            return_value[QUOTAS][date] = arranged_quotas
            return_value[RESERVATION_COUNTS][date] = arranged_reservation

        return return_value
    except:
        traceback.print_exc()


def get_survey_ids():
    """
    categoryIdを元に、予約データのsurveyIdを取得する
    """
    command = []
    command.append('aws')
    command.append('dynamodb')
    command.append('scan')
    command.append('--table-name')
    command.append(table_name_survey_results)
    command.append('--profile')
    command.append(profile_name)
    return_value = []

    try:
        result = run_aws_cli_command(command)
        items = get_items(result)

        for item in items:
            value = None
            survey_id = None

            if VALUE in item and 'S' in item[VALUE]:
                value = item[VALUE]['S']
                if not category_id in value:
                    continue
            else:
                continue

            if SURVEY_ID in item\
            and 'S' in item[SURVEY_ID]\
            and item[SURVEY_ID]['S'] != SURVEY_ID:
                survey_id = item[SURVEY_ID]['S']
            else:
                continue

            if value and survey_id and not survey_id in return_value:
                return_value.append(survey_id)
    except:
        traceback.print_exc()
    finally:
        logger.info('surveyId:'+ str(return_value))
        return return_value


def get_reservation_record():
    """
    SurveyResultsから実予約のレコードを検索・返却
    :return reservation_records for SurveyResults. type is list.
    """
    start_datetime = datetime.datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')
    logger.info(f'検索開始日時 : {start_datetime}')
    logger.info('検索中...')
    items = []
    reservation_records = []
    global survey_ids

    # surveyIdが引数で入力されていなかった場合
    if len(survey_ids) == 0:
        survey_ids = get_survey_ids()

    if len(survey_ids) != 0:
        for survey_id in survey_ids:
            # surveyId毎にレコードを取得する
            search_attribute_values = json.dumps({
                ":pkey": {
                    "S": survey_id
                },
                ":skey": {
                    "S": survey_id
                }
            })
            command = []
            command.append('aws')
            command.append('dynamodb')
            command.append('query')
            command.append('--table-name')
            command.append(table_name_survey_results)
            command.append('--index-name')
            command.append('surveyId-partitionKey-index')
            command.append('--key-condition-expression')
            command.append('surveyId=:pkey and begins_with(partitionKey, :skey)')
            command.append('--expression-attribute-values')
            command.append(search_attribute_values)
            command.append('--profile')
            command.append(profile_name)

            try:
                # コマンドを実行/Itemsを取得
                result = run_aws_cli_command(command)
                items.extend(get_items(result))
            except:
                traceback.print_exc()

    try:
        if len(items) > 0:
            # partitionKey毎にソート
            items.sort(key=lambda item: item[PARTITION_KEY]['S'])

            # partitionKey毎にグループ化してループ
            for pkey, items in groupby(items, key=lambda item: item[PARTITION_KEY]['S']):
                # 値渡しでitemsをコピーしておく
                copy_items = [item for item in items]

                # checkの値を取り出す
                check_list = []
                for item in copy_items:
                    # checkに値が保存されていない場合エラーになるのでバリデーション
                    if 'check' in item:
                        check_list.append(item[CHECK]['S'])

                # checkが混在している場合はキャンセルもしくは取り消しされたpartitionKeyとする = 予約数にカウントしない
                check_counted = count_list_element(check_list)
                if len(check_counted) > 1:
                    continue

                # checkがキャンセル,取り消し以外のレコードを取得
                for item in copy_items:
                    # キーエラーにならないようにバリデーション
                    if CHECK in item and 'S' in item[CHECK]:
                        check = item['check']['S']

                    # キーエラーにならないようにバリデーション
                    if VALUE in item and 'S' in item['value']:
                        value = item['value']['S']

                    if check != 'キャンセル' and check != '取り消し' and category_id in value:
                        reservation_records.append(item)
    except:
        traceback.print_exc()
    finally:
        if len(reservation_records) == 0:
            return None
        else:
            return reservation_records


def comparsion_reservation():
    """
    実予約数とsurveyCalendarの予約数を比較する
    """
    count_greater_than_results = 0
    count_greater_than_calendars = 0
    greater_than_results_list = []
    greater_than_calendars_list = []
    category_tags = get_category_tag(category_id)
    
    try:
        # 各日付・コマ毎の予約数を整形する
        reservations_for_survey_results = {}
        diff_info = {}
        reservation_records = get_reservation_record()
        separate = '|'

        if reservation_records:
            # partitionKey毎にソート
            reservation_records.sort(key=lambda record: record[PARTITION_KEY]['S'])

            # partitionKey毎にグループ化してループ
            for pkey, records in groupby(reservation_records, key=lambda record: record[PARTITION_KEY]['S']):
                # 値渡し
                copy_records = [record for record in records]
                user_id = None

                # userIdの取得
                for record in copy_records:
                    if 'userId' in record and 'S' in record['userId'] and user_id == None:
                        user_id = record['userId']['S']
                        break

                for record in copy_records:
                    # 予約データかどうかを判別
                    if 'check' in record and 'S' in record['check']:
                        
                        if VALUE in record and 'S' in record[VALUE]:
                            # valueの例：category#000000_0|20210522|1
                            # categoryId_0|予約日|コマ という形式
                            value = record['value']['S']
                        else:
                            value = None

                        if value and separate in value and category_id in value:
                            # '|'で区切る
                            split_value = value.split(separate)
                            date = split_value[1]
                            coma = split_value[2]

                            # keyErrorにならないようにvalidation
                            if not date in reservations_for_survey_results:
                                reservations_for_survey_results[date] = {
                                    coma: 0
                                }

                            if not date in diff_info:
                                diff_info[date] = {
                                    coma: []
                                }
                                
                            # keyErrorにならないようにvalidation
                            if date in reservations_for_survey_results\
                            and not coma in reservations_for_survey_results[date]:
                                reservations_for_survey_results[date][coma] = 0

                            if date in diff_info\
                            and not coma in diff_info[date]:
                                diff_info[date][coma] = []

                            # 実予約数を増やす
                            reservations_for_survey_results[date][coma] += 1
                            # 実予約の接種券番号を追加
                            diff_info[date][coma].append({
                                PARTITION_KEY: pkey,
                                USER_ID: user_id
                            })

            # SurveyCalendarsから予約数と予約上限数を取得
            calendar_id = get_calendar_id(category_id)
            if calendar_id:
                reservations_for_survey_calendars = get_reservation_info(calendar_id)
                current_reservations = reservations_for_survey_calendars[RESERVATION_COUNTS]
            
                # SurveyResultsの予約数と、SurveyCalendarsの予約数が一致しない場合はログに表示する
                input('SurveyResultsの予約数とSurveyCalendarsの予約数が一致しないデータをログファイルに保存します。Enterを押して下さい。 > ')
                
                for date in reservations_for_survey_results.keys():
                    diff_reservations = {}

                    if not date in current_reservations:
                        # キーエラーにならないようにバリデーション
                        current_reservations[date] = {}

                    if date in current_reservations:
                        # 日付単位で一致しているか判断
                        if reservations_for_survey_results[date] != current_reservations[date]:
                            # コマ単位で一致しているか判断
                            for coma in reservations_for_survey_results[date]:
                                # キーエラーにならないようにバリデーション
                                if not coma in current_reservations[date]:
                                    current_reservations[date][coma] = 0

                                if reservations_for_survey_results[date][coma] != current_reservations[date][coma]:
                                    diff_reservations = {
                                        'date': date,
                                        'coma':coma,
                                        'calendarCounts': current_reservations[date][coma],
                                        'resultsCounts': reservations_for_survey_results[date][coma],
                                    }
                                    # どちらが大きいかを比較する
                                    if reservations_for_survey_results[date][coma] < current_reservations[date][coma]:
                                        count_greater_than_results += 1
                                        greater_than_results_list.append(diff_reservations)
                                    elif reservations_for_survey_results[date][coma] > current_reservations[date][coma]:
                                        count_greater_than_calendars += 1
                                        greater_than_calendars_list.append(diff_reservations)
            else:
                logger.info('calendarIdが取得できませんでした。カレンダーが存在しない可能性があります。')

        logger.info('検索が完了しました。')
        logger.info(f'{category_id}:{category_tags}')
        
        logger.info(f'{GRATER_THAN_RESULTS}:{count_greater_than_results}件')
        if count_greater_than_results > 0:
            greater_than_results_list.sort(key=lambda data: (data['date'], data['coma']))

            for data in greater_than_results_list:
                date = data['date']
                coma = data['coma']
                calendar_counts = data['calendarCounts']
                results_counts = data['resultsCounts']
                log_string = f'{date}:{coma} カレンダー予約数:帳票予約数 = {calendar_counts}:{results_counts}'
                
                logger.info(log_string)

        logger.info(f'{GRATER_THAN_CALENDARS}:{count_greater_than_calendars}件')
        if count_greater_than_calendars > 0:
            greater_than_calendars_list.sort(key=lambda data: (data['date'], data['coma']))

            for data in greater_than_calendars_list:
                date = data['date']
                coma = data['coma']
                calendar_counts = data['calendarCounts']
                results_counts = data['resultsCounts']
                log_string = f'{date}:{coma} カレンダー予約数:帳票予約数 = {calendar_counts}:{results_counts}'
                
                logger.info(log_string)
    except:
        traceback.print_exc()
    finally:
        end_datetime = datetime.datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')
        logger.info(f'検索終了日時 : {end_datetime}')


comparsion_reservation()