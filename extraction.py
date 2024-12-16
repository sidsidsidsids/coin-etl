from datetime import datetime, timedelta
import requests
import time
from supabase import Client

def clean_table_current_coins(client: Client):
    try:
        # DB 연결
        supabase = client
        # clean_table_current_coins 함수 실행
        '''
        현재 조회 가능한 코인 이름과 코드명을 저장하는 테이블을 값이 없는 상태로 만드는 함수
        function clean_table_current_coins()

        CREATE TABLE IF NOT EXISTS current_coins (
            korean_name TEXT, 
            name_code TEXT
        );
        TRUNCATE TABLE current_coins;
        '''
        supabase \
        .rpc("clean_table_current_coins") \
        .execute()
        print('[O] clean_table_current_coins executed successfully')
    except Exception as e:
        print('[X] clean_table_current_coins execution occurred error')
        print(e)
        return None

def get_current_coins(client: Client):
    try:
        # DB 연결
        supabase = client
        # UPbit API로부터 데이터 추출
        url = "https://api.upbit.com/v1/market/all?is_details=false"
        headers = {"accept": "application/json"}
        res = requests.get(url, headers=headers)
        # 데이터 전처리 (json to python list)
        res_list = list(res.json())
        # supabase 클라이언트는 python list 형태로 데이터 삽입 가능
        coin_columns = [{"korean_name": info["korean_name"], "name_code": info["market"][4:]} for info in res_list if info["market"].startswith("KRW")]
        # current_coins 테이블에 데이터 삽입
        supabase.table("current_coins") \
        .insert(coin_columns) \
        .execute()
        print('[O] get_current_coins executed successfully')
        return coin_columns
    except Exception as e:
        print('[X] get_current_coins execution occurred error')
        print(e)
        return []

def get_coin_candles(client: Client, coin_columns: list = []):
    try:
        # DB 연결
        supabase = client
        # input이 빈 배열인 경우
        if not coin_columns:
            current_coins = supabase.table("current_coins") \
                    .select('*') \
                    .execute()
            coin_columns = list(current_coins)[0][1]
        # 데이터 추출 환경 설정
        url = "https://api.upbit.com/v1/candles/days"
        headers = {"accept": "application/json"}
        max_day = 200
        current_date = str(datetime.today() + timedelta(hours=9))[:10]
        # 코인 코드 리스트를 순회하며 해당 코인의 데이터 추출
        for coin in coin_columns:
            params = {
                'market': f'KRW-{coin["name_code"]}',
                'count': max_day,
                'to': f'{current_date} 00:00:00'
            }
            res = requests.get(url, params=params, headers=headers)
            res_list = list(res.json())
            # UPbit API로부터 비정상적인 응답을 받는 경우 처리
            if not res_list or res_list[0] == 'name':
                continue
            # UPbit API로부터 정상적인 응답을 받는 경우
            for elem in res_list:
                # 해당 데이터가 DB에 없는 새로운 데이터인지 확인
                name_code = elem["market"][4:]
                candle_date = elem["candle_date_time_kst"][:10]
                response = supabase.table("coin_candles") \
                            .select('*') \
                            .eq("name_code", name_code) \
                            .eq("candle_date", candle_date) \
                            .execute()
                res_data = str(response).split("count")[0].split("=")[-1].strip()
                # 새로운 데이터인 경우 DB에 insert
                if res_data == "[]":
                    supabase.table("coin_candles") \
                            .insert({
                                "name_code": name_code,
                                "candle_date": candle_date,
                                "opening_price": elem["opening_price"],
                                "high_price": elem["high_price"],
                                "low_price": elem["low_price"],
                                "trade_price": elem["trade_price"],
                                "candle_acc_trade_price": elem["candle_acc_trade_price"],
                                "candle_acc_trade_volume": elem["candle_acc_trade_volume"],
                                "change_price": elem["change_price"],
                                "change_rate": elem["change_rate"]
                                }
                            ) \
                            .execute()
                # 새로운 데이터가 아닌 경우 그 이전 데이터도 존재한다고 판단하여 break
                else:
                    break
            # API 요청 수 제한(초당 10회) 회피
            time.sleep(0.1)       
        print('[O] get_coin_candles executed successfully')
    except Exception as e:
        print('[X] get_coin_candles execution occurred error')
        print(e)