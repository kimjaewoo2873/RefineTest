import psycopg2

# DB 접속 정보
DB_CONFIG = {
    "dbname": "Qpoll_Data2",
    "user": "kjw8567",
    "password": "8567",
    "host": "localhost",
    "port": "5432"
}

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG) # DB 연결
        cur = conn.cursor() # cursor 객체 생성
        print("데이터베이스에 성공적으로 연결되었습니다.")
        return conn, cur # 연결과 커서 반환
    except psycopg2.Error as e: 
        print(f"데이터베이스 연결 오류: {e}")
        return None, None

def close_db(conn, cur): 
    if cur: 
        cur.close()
    if conn: 
        conn.close()
        print("데이터베이스 연결을 종료했습니다.")

if __name__ == "__main__":
    conn, cur = connect_db()
    # 연결이 성공적으로 이루어졌는지 확인
    if conn and cur:
        # 연결 테스트 후 바로 종료
        close_db(conn, cur)
