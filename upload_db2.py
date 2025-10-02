import pandas as pd
import psycopg2
import re
from datetime import date

# ## ----------------- 1. 설정 부분 (사용자 환경에 맞게 수정) ----------------- ##

# PostgreSQL 데이터베이스 연결 정보 (이전에 생성한 7개 테이블이 있는 DB)
DB_CONFIG = {
    "dbname": "Qpoll_Data2",      # 데이터베이스 이름
    "user": "kjw8567",      # 사용자 이름
    "password": "8567",  # 비밀번호
    "host": "localhost",          # DB 주소 (로컬이면 localhost)
    "port": "5432"                # DB 포트
}

# 처리할 엑셀 파일 정보
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250224.xlsx'

# POLLS 테이블에 저장될 설문(POLL)의 제목
POLL_TITLE = '여러분은 본인을 위해 소비하는 것 중 가장 기분 좋아지는 소비는 무엇인가요?'

# 엑셀 파일에서 응답 값이 들어있는 컬럼의 이름
# 엑셀 파일의 '문항 1'과 같은 실제 컬럼명을 정확히 입력해야 합니다.
ANSWER_COLUMN_NAME = '문항1'

# 엑셀 파일의 데이터 시작 행 설정 (0부터 시작, 2번째 행이면 1)
HEADER_ROW_INDEX = 1

# ## --------------------------------------------------------------------- ##


def parse_birthdate(text):
    """ 'YYYY년 MM월 DD일 ...' 형식의 문자열에서 날짜만 추출 """
    try:
        # 'YYYY년 MM월 DD일' 부분만 추출
        date_str = re.match(r'\d{4}년 \d{2}월 \d{2}일', str(text))
        if date_str:
            return pd.to_datetime(date_str.group(), format='%Y년 %m월 %d일').date()
    except (ValueError, TypeError):
        pass
    return None

def process_poll_data_to_db(file_path, poll_title):
    """ 
    엑셀 파일을 읽어 POLLS, USERS, USER_POLL_RESPONSES 테이블에 데이터를 저장하는 메인 함수 
    """
    conn = None
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")

        # 1. POLLS 테이블에 설문 정보 저장하고 poll_id 가져오기
        # 오늘 날짜를 poll_date로 사용
        poll_date = date.today()
        cur.execute(
            """
            INSERT INTO POLLS (poll_title, poll_date) 
            VALUES (%s, %s) 
            RETURNING poll_id;
            """,
            (poll_title, poll_date)
        )
        poll_id = cur.fetchone()[0]
        print(f"-> POLLS 테이블: '{poll_title}' 등록 완료 (poll_id: {poll_id})")

        # 2. 엑셀 파일 읽기
        df = pd.read_excel(file_path, header=HEADER_ROW_INDEX)
        print(f"'{file_path}' 파일에서 총 {len(df)}개의 행을 읽었습니다.")
        
        # 3. 엑셀 행(row)을 하나씩 읽으며 USERS, USER_POLL_RESPONSES 테이블에 저장
        inserted_users_count = 0
        updated_users_count = 0
        responses_count = 0

        for index, row in df.iterrows():
            # USERS 테이블에 필요한 필수 컬럼 목록
            essential_cols = ['고유번호', '성별', '나이', '지역']

            # 필수 컬럼 중 하나라도 비어있으면 해당 행 전체를 건너뜀
            if any(pd.isna(row.get(col)) or not str(row.get(col)).strip() for col in essential_cols):
                print(f"INFO: {index + HEADER_ROW_INDEX + 2}번째 행의 사용자 필수 정보가 비어있어 건너뜁니다.")
                continue

            # 3a. USERS 테이블 처리
            user_id = str(row['고유번호']).strip()
            gender = str(row['성별']).strip()
            region = str(row['지역']).strip()
            birth_date = parse_birthdate(row['나이'])

            if birth_date is None:
                print(f"INFO: {index + HEADER_ROW_INDEX + 2}번째 행의 '나이' 형식이 잘못되어 건너뜁니다.")
                continue
            
            # user_id가 이미 존재하면 정보를 업데이트하고, 없으면 새로 추가
            cur.execute(
                """
                INSERT INTO USERS (user_id, gender, birth_date, region, updated_at) 
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) 
                DO UPDATE SET
                    gender = EXCLUDED.gender,
                    birth_date = EXCLUDED.birth_date,
                    region = EXCLUDED.region,
                    updated_at = NOW();
                """,
                (user_id, gender, birth_date, region)
            )
            if cur.rowcount > 0:
                responses_count += 1


            # 3b. USER_POLL_RESPONSES 테이블 처리
            responded_at = row['설문일시']
            answer_value = row.get(ANSWER_COLUMN_NAME) # .get()으로 안전하게 접근
            
            # 응답 값이 비어있지 않은 경우에만 처리
            if pd.notna(answer_value) and str(answer_value).strip():
                final_answer = str(answer_value).strip()
                
                # 숫자형 답변일 경우 '.0' 제거 (예: 4.0 -> '4')
                try:
                    float_val = float(final_answer)
                    if float_val.is_integer():
                        final_answer = str(int(float_val))
                except ValueError:
                    pass # 텍스트 답변은 그대로 사용

                # 동일한 유저가 동일한 설문에 중복 응답한 경우, 최신 응답으로 업데이트
                cur.execute(
                    """
                    INSERT INTO USER_POLL_RESPONSES (user_id, poll_id, response_value, responded_at) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, poll_id)
                    DO UPDATE SET
                        response_value = EXCLUDED.response_value,
                        responded_at = EXCLUDED.responded_at;
                    """,
                    (user_id, poll_id, final_answer, responded_at)
                )

        # 모든 작업이 성공하면 변경사항을 DB에 최종 반영
        conn.commit()
        print("\n" + "="*50)
        print(f"'{file_path}' 파일의 모든 데이터가 성공적으로 처리되었습니다.")
        print(f"총 {responses_count}개의 유효한 응답이 처리되었습니다.")
        print("="*50)


    except Exception as e:
        # 에러 발생 시 모든 작업을 취소
        if conn:
            conn.rollback()
        print(f"\n오류가 발생하여 작업을 취소하고 롤백했습니다.\n에러: {e}")

    finally:
        # 연결 종료
        if conn:
            cur.close()
            conn.close()
            print("데이터베이스 연결을 종료했습니다.")


# ## ----------------- 스크립트 실행 ----------------- ##
if __name__ == "__main__":
    process_poll_data_to_db(EXCEL_FILE_PATH, POLL_TITLE)