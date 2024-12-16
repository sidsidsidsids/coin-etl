from connect_db import *
from extraction import *
from transformation import *
from loading import *
import time
import datetime

try:
    print("데이터 파이프라인 실행 시작 : ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("----------------------------------------------")

    # Step 1: DB 연결
    print("Step 1: DB 연결")
    t0 = time.time()
    supabase_client = connect_supabase()
    t1 = time.time()
    print("---> DB 연결 소요 시간", str(t1-t0), "seconds", "\n")

    # Step 2: 데이터 추출 (extraction)
    print("Step 2: 데이터 추출")
    t0 = time.time()
    clean_table_current_coins(supabase_client)
    coin_columns = get_current_coins(supabase_client)
    get_coin_candles(supabase_client, coin_columns)
    t1 = time.time()
    print("---> 데이터 추출 소요 시간", str(t1-t0), "seconds", "\n")

    # Step 3: 데이터 변형 (transformation)
    print("Step 3: 데이터 변형")
    t0 = time.time()
    transform_data(supabase_client)
    t1 = time.time()
    print("---> 데이터 변형 소요 시간", str(t1-t0), "seconds", "\n")

    # Step 4: 데이터 적재 (loading)
    print("Step 4: 데이터 적재")
    t0 = time.time()
    load_data(supabase_client)
    t1 = time.time()
    print("---> 데이터 적재 소요 시간", str(t1-t0), "seconds", "\n")
except Exception as e:
    print("에러 발생")
    print(e)
print("----------------------------------------------")
print("데이터 파이프라인 실행 완료 : ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))