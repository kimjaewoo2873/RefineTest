import pandas as pd
import psycopg2
import re
from datetime import date

# PostgreSQL 데이터베이스 연결 정보
DB_CONFIG = {
    "dbname": "Qpoll_Data2",
    "user": "kjw8567",
    "password": "8567",
    "host": "localhost",
    "port": "5432"
}

# 처리할 엑셀 파일 경로
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250304.xlsx'

# 실행 시트 
# 'PROFILE': 프로필 질문/답변 저장 (시트 1, 2 모두 사용)
# 'POLL'   : 단일 설문(Poll) 결과 저장 (시트 1만 사용)
IMPORT_MODE = 'POLL' 

# --- 엑셀 시트 정보 PROFILE모드 ---
SHEET_RESPONSES = 0       # 사용자 응답이 있는 시트 (첫 번째 시트)
SHEET_QUESTION_INFO = 1   # 질문과 선택지 정보가 있는 시트 (두 번째 시트)
HEADER_ROW_RESPONSES = 1  # 응답 시트에서 데이터(헤더)가 시작되는 행 번호 (0부터 시작)

# 문항과 액셀 컬럼 매핑 
QUESTION_COLUMN_MAPPING = {
    '문항1': '여러분은본인을위해소비하는것중가장기분좋아지는소비는무엇인가요', # question_id
    # '문항2': '새로운질문ID',  # <- 문항2가 있다면 이런 식으로 추가
}

# --- 'POLL' 모드 설정 ---
POLL_TITLE = '여러분은 본인을 위해 소비하는 것 중 가장 기분 좋아지는 소비는 무엇인가요?'
POLL_ANSWER_COLUMN = '문항1' # Poll 답변이 있는 컬럼

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ
def parse_birthdate(text):
    """ 'YYYY년 MM월 DD일 ...' 형식의 문자열에서 날짜만 추출 """
    try:
        date_str = re.match(r'\d{4}년 \d{2}월 \d{2}일', str(text))
        if date_str:
            return pd.to_datetime(date_str.group(), format='%Y년 %m월 %d일').date()
    except (ValueError, TypeError):
        pass
    return None

def process_all_data_to_db(cur, file_path):
    """
    엑셀의 두 시트에서 모든 질문, 선택지, 사용자, 다중 답변을 읽어 DB에 저장합니다.
    """
    print("--- [프로필 모드 - 여러 문항]으로 임포트를 시작합니다. ---")
    
    # --- 1. 시트2에서 모든 질문/선택지 정보 처리 ---
     # skiprows=[0] 옵션을 추가하여 첫 번째 행(헤더)을 건너뜁니다.
    df_info = pd.read_excel(file_path, sheet_name=SHEET_QUESTION_INFO, header=None, skiprows=[0])
    
    
    print("-> 시트2에서 질문 및 선택지 정보를 읽어 DB에 저장합니다...")
    for index, row in df_info.iterrows():
        question_text = row.iloc[0] # A열을 질문 내용으로 가정
        if pd.isna(question_text) or not str(question_text).strip():
            continue

        # 질문 내용으로 고유한 ID 생성
        question_id = "".join(filter(str.isalnum, question_text))
        
        # PROFILE_QUESTIONS 테이블에 질문 저장
        cur.execute(
            "INSERT INTO PROFILE_QUESTIONS (question_id, question_text, question_type) VALUES (%s, %s, %s) ON CONFLICT (question_id) DO NOTHING;",
            (question_id, question_text, 'MULTIPLE')
        )
        print(f"  - 질문 처리: {question_text[:30]}... (ID: {question_id})")
        
        # PROFILE_ANSWER_OPTIONS 테이블에 선택지 저장 (B열부터)
        for i, option_text in enumerate(row[1:]):
            text_to_check = str(option_text).strip()

            if pd.isna(option_text) or not text_to_check:
                break
    
            # 셀 내용이 숫자인지 확인하는 로직 추가
            is_numeric = False
            try:
                float(text_to_check)
                is_numeric = True
            except ValueError:
                pass # 숫자로 변환 안되면 텍스트로 간주

            # 키워드 또는 숫자인 경우 반복 중단
            if 'CNT' in text_to_check or '참여자' in text_to_check or '총계' in text_to_check or is_numeric:
                print(f"-> 총계 또는 숫자 데이터('{text_to_check}')를 만나 문항 읽기를 중단합니다.")
                break
            #
            
            option_code = str(i + 1)
            cur.execute(
                "INSERT INTO PROFILE_ANSWER_OPTIONS (question_id, option_code, option_text) VALUES (%s, %s, %s) ON CONFLICT (question_id, option_code) DO NOTHING;",
                (question_id, option_code, text_to_check)
            )
    print("-> 모든 질문/선택지 정보 처리를 완료했습니다.")

    # --- 2. 시트1에서 모든 사용자/답변 정보 처리 ---
    df_responses = pd.read_excel(file_path, sheet_name=SHEET_RESPONSES, header=HEADER_ROW_RESPONSES)
    print(f"-> 응답 시트에서 총 {len(df_responses)}개의 응답 행을 읽었습니다.")

    for index, row in df_responses.iterrows():
        # 2a. USERS 테이블 처리
        if any(pd.isna(row.get(col)) for col in ['고유번호', '성별', '나이', '지역']):
            continue
        user_id = str(row['고유번호']).strip()
        birth_date = parse_birthdate(row['나이'])
        if birth_date is None: continue

        cur.execute(
            "INSERT INTO USERS (user_id, gender, birth_date, region, updated_at) VALUES (%s, %s, %s, %s, NOW()) ON CONFLICT (user_id) DO UPDATE SET gender = EXCLUDED.gender, birth_date = EXCLUDED.birth_date, region = EXCLUDED.region, updated_at = NOW();",
            (user_id, str(row['성별']).strip(), birth_date, str(row['지역']).strip())
        )
        
        # 2b. 여러 문항에 대한 답변 처리
        answered_at = row.get('설문일시')
        
        # 설정된 매핑 정보를 기준으로 반복 작업
        for column_name, question_id in QUESTION_COLUMN_MAPPING.items():
            answer_value_raw = row.get(column_name)
            
            if pd.notna(answer_value_raw) and str(answer_value_raw).strip():
                # 쉼표로 답변 분리
                answers = str(answer_value_raw).split(',')
                for single_answer in answers:
                    final_answer = single_answer.strip()
                    if final_answer:
                        # USER_PROFILE_ANSWERS 테이블에 각 답변을 별도 행으로 저장
                        cur.execute(
                            "INSERT INTO USER_PROFILE_ANSWERS (user_id, question_id, answer_value, answered_at) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, question_id, answer_value) DO NOTHING;",
                            (user_id, question_id, final_answer, answered_at)
                        )


def run_poll_import(cur, file_path):
    """'POLL' 모드: POLL 관련 3개 테이블에 데이터를 저장합니다."""
    print("--- [POLL 모드]로 임포트를 시작합니다. ---")
    
    # 1. POLLS 테이블에 설문 정보 저장
    cur.execute(
        "INSERT INTO POLLS (poll_title, poll_date) VALUES (%s, %s) RETURNING poll_id;",
        (POLL_TITLE, date.today())
    )
    poll_id = cur.fetchone()[0]
    print(f"-> POLLS 테이블 처리 완료 (ID: {poll_id})")

    # 2. 시트1에서 사용자/응답 정보 처리
    df_responses = pd.read_excel(file_path, sheet_name=SHEET_RESPONSES, header=HEADER_ROW_RESPONSES)
    print(f"-> 응답 시트에서 총 {len(df_responses)}개의 응답 행을 읽었습니다.")

    for index, row in df_responses.iterrows():
        if any(pd.isna(row.get(col)) for col in ['고유번호', '성별', '나이', '지역']):
            continue
        user_id = str(row['고유번호']).strip()
        birth_date = parse_birthdate(row['나이'])
        if birth_date is None: continue

        cur.execute(
            "INSERT INTO USERS (user_id, gender, birth_date, region, updated_at) VALUES (%s, %s, %s, %s, NOW()) ON CONFLICT (user_id) DO UPDATE SET gender = EXCLUDED.gender, birth_date = EXCLUDED.birth_date, region = EXCLUDED.region, updated_at = NOW();",
            (user_id, str(row['성별']).strip(), birth_date, str(row['지역']).strip())
        )
        
        answer_value = row.get(POLL_ANSWER_COLUMN)
        responded_at = row.get('설문일시')
        if pd.notna(answer_value) and str(answer_value).strip():
            final_answer = str(answer_value).strip()
            cur.execute(
                "INSERT INTO USER_POLL_RESPONSES (user_id, poll_id, response_value, responded_at) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, poll_id) DO UPDATE SET response_value = EXCLUDED.response_value, responded_at = EXCLUDED.responded_at;",
                (user_id, poll_id, final_answer, responded_at)
            )

if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")
        
        if IMPORT_MODE == 'PROFILE':
            process_all_data_to_db(cur, EXCEL_FILE_PATH)
        elif IMPORT_MODE == 'POLL':
            run_poll_import(cur, EXCEL_FILE_PATH)
        else:
            print(f"오류: 잘못된 IMPORT_MODE 입니다. ('PROFILE' 또는 'POLL'만 가능)")
        
        conn.commit()
        print("\n" + "="*50)
        print(f"'{IMPORT_MODE}' 모드의 모든 데이터가 성공적으로 처리되었습니다.")
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