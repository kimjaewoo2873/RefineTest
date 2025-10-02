import pandas as pd
import psycopg2
import re

# ## ----------------- 1. 설정 부분 (사용자 환경에 맞게 수정) ----------------- ##

# PostgreSQL 데이터베이스 연결 정보
DB_CONFIG = {
    "dbname": "Qpoll_Data2",      # 데이터베이스 이름
    "user": "kjw8567",      # 사용자 이름
    "password": "8567",  # 비밀번호
    "host": "localhost",          # DB 주소
    "port": "5432"                # DB 포트
}

# 처리할 엑셀 파일 경로
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250224.xlsx'

# 시트 이름 또는 번호 (0부터 시작)
SHEET_RESPONSES = 0
SHEET_QUESTION_INFO = 1

# --- 시트1 (응답 데이터) 설정 ---
HEADER_ROW_RESPONSES = 1
ANSWER_COLUMN_NAME = '문항1' # 쉼표로 구분된 다중 답변이 있는 컬럼

# --- 시트2 (질문 정보) 설정 ---
QUESTION_CELL_LOCATION = (1, 0)
OPTIONS_START_CELL_LOCATION = (1, 1)

# ## --------------------------------------------------------------------- ##


def parse_birthdate(text):
    """ 'YYYY년 MM월 DD일 ...' 형식의 문자열에서 날짜만 추출 """
    try:
        date_str = re.match(r'\d{4}년 \d{2}월 \d{2}일', str(text))
        if date_str:
            return pd.to_datetime(date_str.group(), format='%Y년 %m월 %d일').date()
    except (ValueError, TypeError):
        pass
    return None

def process_profile_data_to_db(file_path):
    """ 
    엑셀 파일의 두 시트를 읽어 다중 답변을 처리하여 DB에 저장하는 메인 함수
    """
    conn = None
    try:
        # --- 1. 데이터베이스 연결 ---
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")

        # --- 2. 시트2에서 질문(Question) 및 선택지(Options) 정보 읽기 ---
        df_info = pd.read_excel(file_path, sheet_name=SHEET_QUESTION_INFO, header=None)
        
        # 2a. PROFILE_QUESTIONS 테이블에 질문 저장
        question_text = df_info.iloc[QUESTION_CELL_LOCATION[0], QUESTION_CELL_LOCATION[1]]
        question_id = "Q_" + "".join(filter(str.isalnum, question_text))[:30]

        cur.execute(
            """
            INSERT INTO PROFILE_QUESTIONS (question_id, question_text, question_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (question_id) DO NOTHING;
            """,
            (question_id, question_text, 'MULTIPLE') # <--- 다중 선택이므로 'MULTIPLE'로 변경
        )
        print(f"-> PROFILE_QUESTIONS 테이블 처리 완료 (ID: {question_id})")

        # 2b. PROFILE_ANSWER_OPTIONS 테이블에 선택지 저장 (변경 없음)
        options_row = df_info.iloc[OPTIONS_START_CELL_LOCATION[0]]
        options_start_col = OPTIONS_START_CELL_LOCATION[1]
        
        option_count = 0
        for i, option_text in enumerate(options_row[options_start_col:]):
            if pd.isna(option_text) or not str(option_text).strip():
                break
            option_count += 1
            option_code = str(i + 1)
            cur.execute(
                """
                INSERT INTO PROFILE_ANSWER_OPTIONS (question_id, option_code, option_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (question_id, option_code) DO NOTHING;
                """,
                (question_id, option_code, str(option_text).strip())
            )
        print(f"-> PROFILE_ANSWER_OPTIONS 테이블에 {option_count}개의 선택지 처리 완료.")
        
        # --- 3. 시트1에서 사용자 응답(Responses) 정보 읽고 DB에 저장 ---
        df_responses = pd.read_excel(file_path, sheet_name=SHEET_RESPONSES, header=HEADER_ROW_RESPONSES)
        print(f"'{file_path}' (시트: {SHEET_RESPONSES}) 파일에서 총 {len(df_responses)}개의 응답 행을 읽었습니다.")

        for index, row in df_responses.iterrows():
            # 3a. USERS 테이블 처리 (변경 없음)
            essential_cols = ['고유번호', '성별', '나이', '지역']
            if any(pd.isna(row.get(col)) for col in essential_cols):
                continue

            user_id = str(row['고유번호']).strip()
            birth_date = parse_birthdate(row['나이'])
            if birth_date is None:
                continue

            cur.execute(
                """
                INSERT INTO USERS (user_id, gender, birth_date, region, updated_at) 
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    gender = EXCLUDED.gender, birth_date = EXCLUDED.birth_date,
                    region = EXCLUDED.region, updated_at = NOW();
                """,
                (user_id, str(row['성별']).strip(), birth_date, str(row['지역']).strip())
            )

            # ####################################################################
            # ### 3b. USER_PROFILE_ANSWERS 테이블 처리 (핵심 변경 부분) ###
            # ####################################################################
            answer_value_raw = row.get(ANSWER_COLUMN_NAME)
            answered_at = row.get('설문일시')

            if pd.notna(answer_value_raw) and str(answer_value_raw).strip():
                # 쉼표(,)를 기준으로 답변들을 분리하여 리스트로 만듭니다. (예: '2, 1' -> ['2', '1'])
                answers = str(answer_value_raw).split(',')

                # 분리된 각 답변에 대해 반복 작업을 수행합니다.
                for single_answer in answers:
                    final_answer = single_answer.strip() # 답변 앞뒤의 공백 제거
                    
                    if not final_answer: # 값이 비어있으면 건너뛰기
                        continue

                    # 각 답변을 별도의 행으로 INSERT 합니다.
                    cur.execute(
                        """
                        INSERT INTO USER_PROFILE_ANSWERS (user_id, question_id, answer_value, answered_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, question_id, answer_value) DO NOTHING;
                        """,
                        (user_id, question_id, final_answer, answered_at)
                    )

        # --- 4. 최종 커밋 ---
        conn.commit()
        print("\n" + "="*50)
        print("모든 데이터가 성공적으로 처리되었습니다.")
        print("="*50)

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n오류가 발생하여 작업을 취소하고 롤백했습니다.\n에러: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print("데이터베이스 연결을 종료했습니다.")

if __name__ == "__main__":
    process_profile_data_to_db(EXCEL_FILE_PATH)