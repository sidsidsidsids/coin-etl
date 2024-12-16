from supabase import Client

def load_data(client: Client):
    try:
        supabase = client
        '''
        코인 현황 테이블, 코인 캔들 테이블, 스테이지 테이블을 JOIN하여 시각화에 사용할 테이블에 필요한 값들을 저장하는 함수
        function load_datas()

        CREATE TABLE IF NOT EXISTS daily_coin_stats (
            id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            korean_name TEXT,
            name_code TEXT,
            candle_date DATE,
            trade_price FLOAT8,
            change_price FLOAT8,
            ma_7 FLOAT8,
            ma_14 FLOAT8,
            ma_50 FLOAT8,
            rsi FLOAT8,
            candle_acc_trade_price FLOAT8,
            score INT
        );
        TRUNCATE daily_coin_stats;

        -- 테이블에 저장
        INSERT INTO daily_coin_stats (korean_name, name_code, candle_date, trade_price, change_price, ma_7, ma_14, ma_50, rsi, candle_acc_trade_price, score)
        SELECT 
            cur.korean_name,
            ccs.name_code,
            ccs.candle_date,
            ccs.trade_price,
            ccs.change_price,
            ccs.ma_7,
            ccs.ma_14,
            ccs.ma_50,
            ccs.rsi,
            cc.candle_acc_trade_price,
            ccs.score
        FROM 
            coin_candles_stage AS ccs
        INNER JOIN
            coin_candles AS cc
        ON
            ccs.name_code = cc.name_code 
            AND ccs.candle_date = cc.candle_date    
        INNER JOIN
            current_coins AS cur
        ON
            ccs.name_code = cur.name_code
        ORDER BY 
            score DESC,
            candle_acc_trade_price DESC;
        '''
        supabase \
        .rpc("load_coin_datas") \
        .execute()
        print('[O] load_data executed successfully')
    except Exception as e:
        print('[X] load_data execution occurred error')
        print(e)
        return None