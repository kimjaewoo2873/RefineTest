import pandas as pd
import psycopg2
import re
from datetime import date

# --- 설정 (이 부분은 그대로 사용) ---
DB_CONFIG = {
    "dbname": "Qpoll_Data2",
    "user": "kjw8567",
    "password": "8567",
    "host": "localhost",
    "port": "5432"
}
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250624.xlsx'
SHEET_RESPONSES = 0      # 사용자 응답이 있는 시트 (첫 번째 시트)
SHEET_QUESTIONS = 1      # 질문/선택지 정보가 있는 시트 (두 번째 시트)
HEADER_ROW_RESPONSES = 1 # 응답 시트의 헤더 행

# 날짜 
def parse_birthdate(text):
    try:
        date_str = re.match(r'\d{4}년 \d{2}월 \d{2}일', str(text))
        if date_str:
            return pd.to_datetime(date_str.group(), format='%Y년 %m월 %d일').date()
    except (ValueError, TypeError):
        pass
    return None

def process_dynamic_survey_from_excel(cur, file_path):
    """
    하나의 시트 안에 여러 질문 블록이 있는 엑셀 구조를 동적으로 처리
    """    
    # === 1. 질문/선택지 시트(두 번째 시트) 처리 ===
    df_questions = pd.read_excel(file_path, sheet_name=SHEET_QUESTIONS, header=0)
    
    # 질문 텍스트를 key로, PROFILE 질문 ID와 POLL ID를 값으로 저장하는 딕셔너리
    question_text_to_ids_map = {}
    
    # 질문 순서를 세기 위한 카운터 변수 추가
    question_counter = 0
    
    print("-> 질문/선택지 시트에서 정보를 읽어 DB에 저장합니다...")
    for index, row in df_questions.iterrows():
        question_text = row.get('설문제목')

        # '설문제목' 이라는 글자 자체는 헤더이므로 질문으로 처리하지 않고 건너뜁니다.
        if pd.isna(question_text) or not str(question_text).strip() or question_text == '설문제목':
            continue
        
        # 유효한 질문을 찾음, 카운터 1 증가
        question_counter += 1
        
        # --- PROFILE 테이블 관련 처리 ---
        profile_question_id = "".join(filter(str.isalnum, question_text))
        cur.execute(
            "INSERT INTO PROFILE_QUESTIONS (question_id, question_text, question_type) VALUES (%s, %s, %s) ON CONFLICT (question_id) DO NOTHING;",
            (profile_question_id, question_text, 'MULTIPLE')
        )
        print(f"  - [PROFILE] 질문 처리: {question_text}...") 

        cur.execute(
        "INSERT INTO POLLS (poll_title, poll_date) VALUES (%s, %s) ON CONFLICT (poll_title) DO UPDATE SET poll_date = EXCLUDED.poll_date RETURNING poll_id;",
        (question_text, date.today())
        )
        poll_id = cur.fetchone()[0]
        print(f"  - [POLL] 설문 처리: {question_text[:30]}... (Poll ID: {poll_id})")

        # ✨ 변경점 3: '문항1', '문항2' ... 와 같은 키(key)를 생성하여 ID 저장
        question_key = f'문항{question_counter}'
        question_text_to_ids_map[question_key] = {
            'profile_id': profile_question_id,
            'poll_id': poll_id
        }
        
        # PROFILE_ANSWER_OPTIONS 테이블 처리 (기존과 동일)
        for i in range(1, 100):
            option_col_name = f'보기{i}'
            if option_col_name not in row or pd.isna(row[option_col_name]):
                break
            option_text = str(row[option_col_name]).strip()
            option_code = str(i)
            cur.execute(
                "INSERT INTO PROFILE_ANSWER_OPTIONS (question_id, option_code, option_text) VALUES (%s, %s, %s) ON CONFLICT (question_id, option_code) DO NOTHING;",
                (profile_question_id, option_code, option_text)
            )

    print("-> 모든 질문/선택지 정보 처리를 완료했습니다.")

    # === 2. 사용자 응답 시트(첫 번째 시트) 처리 ===
    df_responses = pd.read_excel(file_path, sheet_name=SHEET_RESPONSES, header=HEADER_ROW_RESPONSES)
    print(f"-> 응답 시트에서 총 {len(df_responses)}개의 응답 행을 읽었습니다.")
    
    # --- ✨ 원인 파악을 위한 디버깅 코드 추가 ✨ ---   
    print("\n" + "="*20 + " [디버깅 정보] " + "="*20)
    print("1. [답변 시트]의 컬럼 목록:")
    print(list(df_responses.columns))
    print("\n2. [질문 시트]에서 추출한 질문 목록 (매핑 키):")
    print(list(question_text_to_ids_map.keys()))
    print("="*55 + "\n")
    # --- 디버깅 코드 끝 ---
    
    answer_columns = [col for col in df_responses.columns if col in question_text_to_ids_map]
    print(f"-> 처리할 답변 컬럼: {answer_columns}")

    for index, row in df_responses.iterrows():
        # USERS 테이블 처리 (기존과 동일)
        if any(pd.isna(row.get(col)) for col in ['고유번호', '성별', '나이', '지역']):
            continue
        user_id = str(row['고유번호']).strip()
        birth_date = parse_birthdate(row['나이'])
        if birth_date is None: continue
        cur.execute(
            "INSERT INTO USERS (user_id, gender, birth_date, region, updated_at) VALUES (%s, %s, %s, %s, NOW()) ON CONFLICT (user_id) DO UPDATE SET gender = EXCLUDED.gender, birth_date = EXCLUDED.birth_date, region = EXCLUDED.region, updated_at = NOW();",
            (user_id, str(row['성별']).strip(), birth_date, str(row['지역']).strip())
        )
        
        answered_at = row.get('설문일시')
        
        for col_name in answer_columns:
            ids = question_text_to_ids_map[col_name]
            profile_question_id = ids['profile_id']
            poll_id = ids['poll_id'] # ✨ poll_id 가져오기
            
            answer_value_raw = row.get(col_name) # 나누기 전 원본 답변
            
            if pd.notna(answer_value_raw) and str(answer_value_raw).strip():
                # --- USER_PROFILE_ANSWERS 테이블 처리 (기존과 동일) ---
                answers = str(answer_value_raw).split(',')
                for single_answer in answers:
                    final_answer_str = single_answer.strip()
                    if final_answer_str:
                        final_answer_for_db = final_answer_str  # 기본값은 원래 문자열

                try:
                    # 숫자 형태의 문자열(예: '4.0')을 정수 문자열(예: '4')로 변환 시도
                    final_answer_for_db = str(int(float(final_answer_str)))
                except ValueError:
                    # 변환에 실패하면(예: '기타' 같은 텍스트 답변) 원래 값 사용
                    pass

                cur.execute(
                    "INSERT INTO USER_PROFILE_ANSWERS (user_id, question_id, answer_value, answered_at) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, question_id, answer_value) DO NOTHING;",
                    (user_id, profile_question_id, final_answer_for_db, answered_at) # final_answer 대신 final_answer_for_db 사용
                )
                # --- ✨ 변경점: USER_POLL_RESPONSES 테이블 처리 ---
                raw_parts = str(answer_value_raw).split(',')
                clean_parts = []
                for part in raw_parts:
                    part_str = part.strip()
                    if part_str:
                        try:
                            # 각 부분을 정수 문자열로 변환 (예: '4.0' -> '4')
                            clean_part = str(int(float(part_str)))
                            clean_parts.append(clean_part)
                        except ValueError:
                            # 텍스트 답변인 경우 그대로 추가
                            clean_parts.append(part_str)

                # 변환된 부분들을 다시 쉼표로 합침 (예: ['4', '3'] -> '4, 3')
                poll_response_value = ", ".join(clean_parts)

                cur.execute(
                    "INSERT INTO USER_POLL_RESPONSES (user_id, poll_id, response_value, responded_at) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, poll_id) DO UPDATE SET response_value = EXCLUDED.response_value, responded_at = EXCLUDED.responded_at;",
                    (user_id, poll_id, poll_response_value, answered_at)
                )

# --- 메인 실행 로직 (기존과 동일) ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")
        
        process_dynamic_survey_from_excel(cur, EXCEL_FILE_PATH)
        
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