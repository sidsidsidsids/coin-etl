from datetime import datetime, timedelta
from supabase import Client

def transform_data(client: Client):
    try:
        supabase = client
        input_date = (datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%d')
        '''
        수집한 코인 데이터를 기반으로 이동평균선(MA), RSI와 같은 지표를 계산하여 coin_candles_stage 테이블에 저장하는 함수
        function transform_coin_datas(input_date)

        CREATE TABLE IF NOT EXISTS coin_candles_stage (
            id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name_code TEXT,
            candle_date DATE,
            trade_price FLOAT8,
            change_price FLOAT8,
            ma_7 FLOAT8,
            ma_14 FLOAT8,
            ma_50 FLOAT8,
            rsi FLOAT8,
            score INT
        );
        TRUNCATE coin_candles_stage;

        WITH 
        coin_data AS (
            SELECT
                name_code,
                candle_date,
                trade_price,
                change_price
            FROM
                coin_candles
            WHERE
                current_date - candle_date <= 50
            ORDER BY
                candle_date DESC
        ),
        -- 이동평균선(ma_7, ma_14, ma_50) 계산 테이블
        moving_averages AS (
            SELECT
                name_code,
                candle_date,
                trade_price,
                change_price,
                -- 7일 이동평균선
                CASE 
                    WHEN COUNT(*) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) < 7 THEN NULL
                    ELSE ROUND(AVG(trade_price) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    )::numeric, 5)
                END AS ma_7,
                -- 14일 이동평균선
                CASE 
                    WHEN COUNT(*) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) < 14 THEN NULL
                    ELSE ROUND(AVG(trade_price) OVER (
                        PARTITION BY name_code            
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    )::numeric, 5)
                END AS ma_14,
                -- 50일 이동평균선
                CASE 
                    WHEN COUNT(*) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) < 50 THEN NULL
                    ELSE ROUND(AVG(trade_price) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    )::numeric, 5)
                END AS ma_50
            FROM coin_data
        ),
        -- RSI 계산 테이블
        rsi_calculations AS (
            SELECT
                name_code,
                candle_date,
                trade_price,
                change_price,
                ma_7,
                ma_14,
                ma_50,
                -- RSI 계산
                CASE
                    WHEN COUNT(*) OVER (
                        PARTITION BY name_code
                        ORDER BY candle_date ASC
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) < 14 THEN NULL
                    ELSE ROUND(100 - (100 / (1 + 
                        (SUM(CASE WHEN change_price > 0 THEN change_price ELSE 0 END) OVER (
                            PARTITION BY name_code 
                            ORDER BY candle_date 
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        )) /
                        NULLIF(SUM(CASE WHEN change_price < 0 THEN ABS(change_price) ELSE 0 END) OVER (
                            PARTITION BY name_code 
                            ORDER BY candle_date 
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ), 0)
                    ))::numeric, 2)
                END AS rsi
            FROM moving_averages
        ),
        --전날 날짜 필터 및 점수 계산 테이블
        scored_datas AS (
            select
                name_code,
                candle_date,
                trade_price,
                change_price,
                ma_7,
                ma_14,
                ma_50,
                rsi,
                (CASE WHEN trade_price > ma_50 THEN 1 ELSE 0 END) +
                (CASE WHEN trade_price < ma_50 THEN -1 ELSE 0 END) +
                (CASE WHEN ma_7 > ma_14 THEN 1 ELSE 0 END) +
                (CASE WHEN ma_7 < ma_14 THEN -1 ELSE 0 END) +
                (CASE WHEN rsi <= 30 THEN 1 ELSE 0 END) +
                (CASE WHEN rsi >= 70 THEN -1 ELSE 0 END) AS score
            from
                rsi_calculations
            where
                candle_date = input_date - 1
        )

        -- 최종 결과를 스테이지 테이블에 저장
        INSERT INTO coin_candles_stage (name_code, candle_date, trade_price, change_price, ma_7, ma_14, ma_50, rsi, score)
        SELECT
            name_code,
            candle_date,
            trade_price,
            change_price,
            ma_7,
            ma_14,
            ma_50,
            rsi,
            score
        FROM 
            scored_datas;
        '''
        supabase \
        .rpc("transform_coin_datas", { "input_date": input_date }) \
        .execute()
        print('[O] trasnform_data executed successfully')
    except Exception as e:
        print('[X] transform_data execution occurred error')
        print(e)
        return None