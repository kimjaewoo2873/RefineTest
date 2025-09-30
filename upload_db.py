import pandas as pd
import psycopg2
import re

# ## ----------------- 1. 설정 부분 (사용자 환경에 맞게 수정) ----------------- ##

# PostgreSQL 데이터베이스 연결 정보
DB_CONFIG = {
    "dbname": "Qpoll_Data",    # 데이터베이스 이름
    "user": "?",    # 사용자 이름 (예: postgres)
    "password": "?",  # 비밀번호
    "host": "localhost",        # DB 주소 (로컬이면 localhost)
    "port": "5432"              # DB 포트
}

# 처리할 엑셀 파일 정보
# 예시: 'C:/Users/MyUser/Documents/survey_A.xlsx'
EXCEL_FILE_PATH = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250304.xlsx'
SURVEY_NAME = '다음 중 가장 스트레스를 많이 느끼는 상황은 무엇인가요?' # 이 설문의 이름

# 엑셀 파일의 데이터 시작 행 설정 (0부터 시작)
HEADER_ROW_INDEX = 1  # 엑셀 파일 컬럼명 행

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


def process_survey_to_db(file_path, survey_name):
    """ 엑셀 파일을 읽어 4개 테이블에 데이터를 저장하는 메인 함수 """
    conn = None
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("데이터베이스에 성공적으로 연결되었습니다.")

        # 1. surveys 테이블에 설문 정보 저장하고 survey_id 가져오기
        cur.execute(
            "INSERT INTO surveys (survey_name, file_name) VALUES (%s, %s) RETURNING id;",
            (survey_name, file_path)
        )
        survey_id = cur.fetchone()[0]
        print(f"-> Surveys 테이블: '{survey_name}' 등록 완료 (survey_id: {survey_id})")

        # 2. 엑셀 파일 읽기
        df = pd.read_excel(file_path, header=HEADER_ROW_INDEX)
        
        # 3. questions 테이블에 문항 정보 저장하고, {컬럼명: question_id} 맵핑 만들기
        question_map = {}
        question_columns = [col for col in df.columns if '문항' in str(col)]

        for i, col_name in enumerate(question_columns):
            question_text = f"{col_name} ({survey_name})" # 간단한 예시. 실제 문항은 별도로 가져와야 할 수 있음
            question_order = i + 1
            
            cur.execute(
                "INSERT INTO questions (survey_id, question_order, question_text) VALUES (%s, %s, %s) RETURNING id;",
                (survey_id, question_order, col_name) # 문항 텍스트를 컬럼명으로 저장
            )
            question_id = cur.fetchone()[0]
            question_map[col_name] = question_id
        print(f"-> Questions 테이블: 총 {len(question_map)}개 문항 등록 완료.")

        # 4. 엑셀 행(row)을 하나씩 읽으며 respondents, answers 테이블에 저장
        for index, row in df.iterrows():
            # respondents 테이블에 들어갈 필수 컬럼 목록
            essential_cols = ['고유번호', '성별', '나이', '지역']

            # 필수 컬럼 중 하나라도 비어있거나 공백이면 해당 행 전체를 건너뜀
            if any(pd.isna(row.get(col)) or not str(row.get(col)).strip() for col in essential_cols):
                print(f"INFO: {index + HEADER_ROW_INDEX + 2}번째 행의 필수 정보가 비어있어 건너뜁니다.")
                continue  # 다음 행으로 바로 이동

            # 4a. respondents 테이블 처리 (있으면 넘어가고 없으면 추가)
            respondent_uid = row['고유번호']
            gender = row['성별']
            region = row['지역']
            birthdate = parse_birthdate(row['나이'])

            # birthdate가 정상적으로 파싱되지 않으면(None이면) 해당 행 건너뛰기
            if birthdate is None:
                print(f"INFO: {index + HEADER_ROW_INDEX + 2}번째 행의 '나이' 형식이 잘못되어 건너뜁니다.")
                continue
            
            cur.execute(
                "INSERT INTO respondents (respondent_uid, gender, birthdate, region) VALUES (%s, %s, %s, %s) ON CONFLICT (respondent_uid) DO NOTHING;",
                (respondent_uid, gender, birthdate, region)
            )
            cur.execute("SELECT id FROM respondents WHERE respondent_uid = %s;", (respondent_uid,))
            respondent_id = cur.fetchone()[0]

            # 4b. answers 테이블 처리
            surveyed_at = row['설문일시']
            for col_name, question_id in question_map.items():
                answer_value = row[col_name]
                
                # 값이 비어있거나 공백만 있는지 확인
                if pd.notna(answer_value) and str(answer_value).strip():
                    
                    final_answer = str(answer_value).strip()
                    try:
                        # 값을 float으로 변환 후, 정수인지 확인
                        float_val = float(final_answer)
                        if float_val.is_integer():
                            final_answer = str(int(float_val)) # 4.0 -> 4 -> '4'
                    except ValueError:
                        # '매우 만족'과 같이 숫자로 변환할 수 없는 텍스트는 그대로 둠
                        pass

                    cur.execute(
                        "INSERT INTO answers (survey_id, respondent_id, question_id, answer_value, surveyed_at) VALUES (%s, %s, %s, %s, %s);",
                        (survey_id, respondent_id, question_id, final_answer, surveyed_at)
                    )
        
        # 모든 작업이 성공하면 변경사항을 DB에 최종 반영
        conn.commit()
        print(f"\n'{file_path}' 파일의 모든 데이터가 성공적으로 처리되었습니다.")

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
    process_survey_to_db(EXCEL_FILE_PATH, SURVEY_NAME)