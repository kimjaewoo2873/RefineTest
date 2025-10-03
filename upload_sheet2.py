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

# --- 시트2 (질문 정보) 설정 ---
# 읽어올 시트 이름 또는 번호 (0부터 시작, 두 번째 시트는 1)
SHEET_QUESTION_INFO = 1 # 또는 실제 시트 이름(예: 'Sheet2')

# 질문 제목이 있는 셀 위치 (행, 열 인덱스. 0부터 시작)
# 예: 2행 A열 -> (1, 0)
QUESTION_CELL_LOCATION = (1, 0)

# 선택지(보기)들이 시작되는 셀 위치
# 예: 2행 B열 -> (1, 1)
OPTIONS_START_CELL_LOCATION = (1, 1)

# ## --------------------------------------------------------------------- ##

def process_question_sheet_to_db(file_path):
    """ 
    엑셀 파일의 두 번째 시트를 읽어 PROFILE_QUESTIONS, PROFILE_ANSWER_OPTIONS 테이블에
    질문과 선택지 정보를 저장하는 함수
    """
    conn = None
    try:
        # --- 1. 데이터베이스 연결 ---
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")

        # --- 2. 엑셀 파일의 두 번째 시트 읽기 ---
        # header=None: 시트의 첫 줄을 컬럼명으로 인식하지 않고, 데이터 그대로 읽어옴
        df_info = pd.read_excel(file_path, sheet_name=SHEET_QUESTION_INFO, header=None)
        print(f"'{file_path}' 파일의 시트(인덱스: {SHEET_QUESTION_INFO})를 성공적으로 읽었습니다.")
        
        # --- 3. PROFILE_QUESTIONS 테이블에 질문 저장 ---
        # .iloc[행, 열]을 사용하여 특정 셀의 값을 가져옴
        question_text = df_info.iloc[QUESTION_CELL_LOCATION[0], QUESTION_CELL_LOCATION[1]]
        
        # 질문 텍스트를 기반으로 고유한 question_id 생성
        question_id = "Q_" + "".join(filter(str.isalnum, question_text)) 

        cur.execute(
            """
            INSERT INTO PROFILE_QUESTIONS (question_id, question_text, question_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (question_id) DO NOTHING;
            """,
            (question_id, question_text, 'MULTIPLE') # 질문 유형을 MULTIPLE로 가정
        )
        print(f"-> PROFILE_QUESTIONS 테이블 처리 완료 (ID: {question_id})")

        # --- 4. PROFILE_ANSWER_OPTIONS 테이블에 선택지 저장 ---
        # 선택지들이 있는 행 전체를 가져옴
        options_row = df_info.iloc[OPTIONS_START_CELL_LOCATION[0]]
        # 선택지가 시작되는 열 번호
        options_start_col = OPTIONS_START_CELL_LOCATION[1]
        
        option_count = 0
        # 선택지 시작 열부터 끝까지 순회
        for i, option_text in enumerate(options_row[options_start_col:]):
            # 셀이 비어있으면 선택지가 끝난 것으로 간주하고 중단
            if pd.isna(option_text) or not str(option_text).strip():
                break 

            # 셀 내용에 'CNT' 또는 '참여자' 라는 텍스트가 포함되어 있으면,
            # 인원수 데이터로 간주하고 반복을 즉시 중단합니다.
            if 'CNT' in str(option_text) or '참여자' in str(option_text):
                print("-> 인원수 관련 컬럼을 만나 문항 읽기를 중단합니다.")
                break
            
            option_count += 1
            option_code = str(i + 1) # 보기 1, 2, 3... 에 해당하는 코드를 순서대로 생성
            
            cur.execute(
                """
                INSERT INTO PROFILE_ANSWER_OPTIONS (question_id, option_code, option_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (question_id, option_code) DO NOTHING;
                """,
                (question_id, option_code, str(option_text).strip())
            )
        print(f"-> PROFILE_ANSWER_OPTIONS 테이블에 {option_count}개의 선택지 처리 완료.")
        
        # --- 5. 최종 커밋 ---
        conn.commit()
        print("\n" + "="*50)
        print("질문/선택지 정보가 성공적으로 DB에 저장되었습니다.")
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


# ## ----------------- 스크립트 실행 전 DB 제약조건 추가! ----------------- ##
# ON CONFLICT 구문이 정상 작동하도록 UNIQUE 제약조건을 추가해야 합니다.
# (이전에 PROFILE_QUESTIONS 테이블 관련 코드를 실행했다면 이미 추가되어 있을 수 있습니다.)

# ALTER TABLE PROFILE_ANSWER_OPTIONS ADD CONSTRAINT question_option_code_unique UNIQUE (question_id, option_code);

# ## ----------------- 스크립트 실행 ----------------- ##
if __name__ == "__main__":
    process_question_sheet_to_db(EXCEL_FILE_PATH)