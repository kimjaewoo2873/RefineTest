import pandas as pd
import psycopg2
import re
from datetime import date

# --- 설정 (사용자 환경에 맞게 수정) ---
DB_CONFIG = {
    "dbname": "Welcome_Data",
    "user": "kjw8567",
    "password": "8567",
    "host": "localhost",
    "port": "5432"
}
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/프캡/paneldata/paneldata/Welcome/welcome_2nd.xlsx'
SHEET_RESPONSES = 'data'   # 사용자 응답 시트 이름
SHEET_LABEL = 'label'    # 질문/선택지 정보 시트 이름

# --- 처리할 질문 목록 ---
# 이 리스트에 원하는 질문 ID를 추가하세요. (예: ['Q1', 'Q5', 'Q8'])
# 리스트가 비어있으면 모든 질문을 처리합니다.
QUESTIONS_TO_PROCESS = ['Q1', 'Q5', 'Q5_1', 'Q6','Q7','Q9_1','Q9_2','Q10','Q11_1','Q11_2'] 

def process_survey_from_excel(cur, file_path):
    """
    세로 블록 형태의 질문/응답 엑셀 파일을 읽어 정규화된 DB에 저장합니다.
    다중 선택 답변(쉼표 구분)을 처리하여 개별 행으로 저장하고, 진행 상황을 표시합니다.
    """
    
    # === 1. 질문/선택지 시트('label') 처리 ===
    print("-> [1/2] 'label' 시트에서 질문/선택지 정보를 읽어 DB에 저장합니다...")
    try:
        # --- [최종 수정] 세 번째 열('문항 유형')까지 읽도록 수정 ---
        df_label = pd.read_excel(
            file_path, 
            sheet_name=SHEET_LABEL, 
            skiprows=1, 
            header=None, 
            names=['id', 'text', 'type'], # 컬럼 이름에 'type' 추가
            usecols=[0, 1, 2],           # 3개의 열(A, B, C)을 읽도록 지정
            index_col=False,
            dtype=str
        ).fillna('')
    except Exception as e:
        print(f"\n[치명적 오류] 'label' 시트를 읽는 중 오류가 발생했습니다: {e}")
        return

    question_map = {} 
    code_map = {}
    current_question_id = None

    for index, row in df_label.iterrows():
        question_id = row['id'].strip()
        question_text = row['text'].strip()
        question_type = row['type'].strip() # 문항 유형 데이터 읽기

        if question_id:
            current_question_id = question_id

            if QUESTIONS_TO_PROCESS and current_question_id not in QUESTIONS_TO_PROCESS:
                current_question_id = None
                continue

            if not question_text:
                print(f"  [경고] 질문 ID '{current_question_id}'에 해당하는 질문 내용이 비어있어 건너뜁니다.")
                current_question_id = None
                continue

            current_question_text = question_text
            
            # --- [최종 수정] question_type 컬럼에 실제 문항 유형 값을 저장 ---
            final_question_type = question_type if question_type else 'UNKNOWN' # 유형 값이 없으면 'UNKNOWN'으로
            cur.execute(
                "INSERT INTO PROFILE_QUESTIONS (question_id, question_text, question_type) VALUES (%s, %s, %s) ON CONFLICT (question_id) DO NOTHING;",
                (current_question_id, current_question_text, final_question_type)
            )
            
            cur.execute(
                "INSERT INTO POLLS (poll_title, poll_date) VALUES (%s, %s) ON CONFLICT (poll_title) DO UPDATE SET poll_date = EXCLUDED.poll_date RETURNING poll_id;",
                (current_question_text, date.today())
            )
            poll_id = cur.fetchone()[0]

            question_map[current_question_id] = {
                'text': current_question_text,
                'poll_id': poll_id
            }
            print(f"  - 질문/설문 등록: {current_question_id} ({current_question_text}) [유형: {final_question_type}] -> Poll ID: {poll_id}")

        elif current_question_id and question_text:
            match = re.match(r'^(\d+)\s+(.*)', question_text)
            if match:
                option_code = match.group(1).strip()
                option_text = match.group(2).strip()
                
                cur.execute(
                    "INSERT INTO PROFILE_ANSWER_OPTIONS (question_id, option_code, option_text) VALUES (%s, %s, %s) ON CONFLICT (question_id, option_code) DO NOTHING;",
                    (current_question_id, option_code, option_text)
                )
                code_map[(current_question_id, option_text)] = option_code

    print("-> 'label' 시트 처리 완료.\n")
    
    if QUESTIONS_TO_PROCESS:
        processed_questions = set(question_map.keys())
        requested_questions = set(QUESTIONS_TO_PROCESS)
        not_found_questions = requested_questions - processed_questions
        if not_found_questions:
            print(f"  [정보] 다음 질문들은 'QUESTIONS_TO_PROCESS' 목록에 있었지만, 'label' 시트에 존재하지 않아 건너뛰었습니다:")
            print(f"  -> {sorted(list(not_found_questions))}\n")

    # === 2. 사용자 응답 시트('data') 처리 ===
    print("-> [2/2] 'data' 시트에서 사용자 응답을 읽어 DB에 저장합니다...")
    df_responses = pd.read_excel(file_path, sheet_name=SHEET_RESPONSES, header=0, dtype=str).fillna('')
    df_responses.columns = [str(col).lower() for col in df_responses.columns]
    question_columns = [col.upper() for col in df_responses.columns if col.upper() in question_map]
    
    print(f"-> 총 {len(df_responses)}개의 응답, {len(question_columns)}개의 질문 컬럼을 처리합니다.")
    if question_columns:
        print(f"-> 처리 대상 질문: {question_columns}")

    total_rows = len(df_responses)
    for index, row in df_responses.iterrows():
        if (index + 1) % 1000 == 0:
            print(f"  ... {index + 1} / {total_rows} 행 처리 중 ...")

        user_id = row.get('mb_sn', '').strip()
        if not user_id: continue

        cur.execute("INSERT INTO USERS (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING;", (user_id,))

        for question_id_upper in question_columns:
            question_id_lower = question_id_upper.lower()
            answer_value_raw = row.get(question_id_lower, '').strip()

            if not answer_value_raw: continue

            answers = [ans.strip() for ans in answer_value_raw.split(',')]
            poll_id = question_map[question_id_upper]['poll_id']

            cur.execute(
                "INSERT INTO USER_POLL_RESPONSES (user_id, poll_id, response_value, responded_at) VALUES (%s, %s, %s, NOW()) ON CONFLICT (user_id, poll_id) DO UPDATE SET response_value = EXCLUDED.response_value;",
                (user_id, poll_id, answer_value_raw)
            )

            for single_answer in answers:
                if not single_answer: continue
                clean_answer = single_answer
                
                if (question_id_upper, clean_answer) in code_map:
                    clean_answer = code_map[(question_id_upper, clean_answer)]
                
                try:
                    clean_answer = str(int(float(clean_answer)))
                except ValueError:
                    pass
                
                cur.execute(
                    """
                    INSERT INTO USER_PROFILE_ANSWERS (user_id, question_id, answer_value) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (user_id, question_id, answer_value) DO NOTHING;
                    """,
                    (user_id, question_id_upper, clean_answer)
                )

    if total_rows > 0:
      print(f"-> 'data' 시트 처리 완료. {index + 1}번째 행까지 처리했습니다.")
    else:
      print("-> 'data' 시트에 처리할 데이터가 없습니다.")


# --- 메인 실행 로직 ---
if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.\n")
        
        process_survey_from_excel(cur, EXCEL_FILE_PATH)
        
        conn.commit()
        print("\n" + "="*50)
        print("모든 데이터가 성공적으로 처리 및 커밋되었습니다.")
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

