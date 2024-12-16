import os
from supabase import create_client, Client

def connect_supabase():
    try:
        # supabase clinet 환경 설정 및 생성
        url: str = os.getenv("SUPABASE_URL")
        key: str = os.getenv("SUPABASE_KEY")
        supabase: Client = create_client(url, key)
        print('[O] connect_supabase executed successfully')
        return supabase
    except Exception as e:
        print('[X] connect_supabase execution occurred error')
        print(e)
        return None
