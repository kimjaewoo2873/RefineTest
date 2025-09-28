import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
import langdetect
from langdetect import detect, DetectorFactory
import re
import logging

# 언어 감지 결과의 일관성을 위해 시드 설정
DetectorFactory.seed = 0

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExcelDataCleaner:
    def __init__(self, db_config, stress_mappings=None):
  
        self.db_config = db_config
        self.engine = None
        self.stress_mappings = stress_mappings or {}
        self.connect_db()
    
    def connect_db(self):
        """PostgreSQL 데이터베이스 연결"""
        try:
            connection_string = f"postgresql://{self.db_config['username']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            logger.info("데이터베이스 연결 성공")
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise
    
    def load_excel_file(self, file_path, sheet_name=None, header=0):
        """엑셀 파일 로드"""
        try:
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)
            else:
                df = pd.read_excel(file_path, header=header)
            logger.info(f"엑셀 파일 로드 완료: {len(df)}행, {len(df.columns)}열")
            return df
        except Exception as e:
            logger.error(f"엑셀 파일 로드 실패: {e}")
            raise
    
    def map_stress_values(self, df):
        """문항1, 문항2의 숫자를 의미 있는 텍스트로 변환"""
        logger.info("스트레스 문항 매핑 시작...")
        
        if not self.stress_mappings:
            logger.warning("매핑 정보가 없습니다. 매핑을 건너뜁니다.")
            return df
        
        mapped_df = df.copy()
        
        for column, mapping in self.stress_mappings.items():
            if column in mapped_df.columns:
                try:
                    # 숫자형으로 변환 후 매핑 적용
                    mapped_df[column] = pd.to_numeric(mapped_df[column], errors='coerce')
                    mapped_df[column] = mapped_df[column].map(mapping)
                    logger.info(f"'{column}' 컬럼 매핑 완료")
                except Exception as e:
                    logger.warning(f"'{column}' 컬럼 매핑 실패: {e}")
            else:
                logger.warning(f"'{column}' 컬럼이 데이터에 존재하지 않습니다.")
        
        logger.info("스트레스 문항 매핑 완료")
        return mapped_df
    
    def basic_cleaning(self, df):
        """1단계: 기본 정제 - 공백 제거, 결측치 처리"""
        logger.info("기본 정제 시작...")
        
        cleaned_df = df.copy()
        
        # 문자열 컬럼에 대해 앞뒤 공백 제거
        string_columns = cleaned_df.select_dtypes(include=['object']).columns
        for col in string_columns:
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
            # 빈 문자열을 NaN으로 변환
            cleaned_df[col] = cleaned_df[col].replace('', np.nan)
            cleaned_df[col] = cleaned_df[col].replace('nan', np.nan)
        
        logger.info(f"기본 정제 완료: {len(cleaned_df)}행 남음")
        return cleaned_df
    
    def remove_duplicates(self, df):
        """2단계: 중복 데이터 제거"""
        logger.info("중복 데이터 제거 시작...")
        
        initial_count = len(df)
        cleaned_df = df.drop_duplicates(keep='first')
        removed_count = initial_count - len(cleaned_df)
        
        logger.info(f"중복 제거 완료: {removed_count}개 중복 행 제거, {len(cleaned_df)}행 남음")
        return cleaned_df
    
    def detect_language(self, text):
        """텍스트의 언어 감지"""
        try:
            if pd.isna(text) or text.strip() == '':
                return 'unknown'
            
            # 텍스트가 너무 짧으면 한국어 문자 패턴으로 판단
            if len(str(text).strip()) < 3:
                korean_pattern = re.compile(r'[가-힣]')
                if korean_pattern.search(str(text)):
                    return 'ko'
                else:
                    return 'unknown'
            
            detected_lang = detect(str(text))
            return detected_lang
        except:
            return 'unknown'
    
    def language_filtering(self, df, text_columns=None, target_language='ko'):
        """3단계: 언어 감지 및 필터링"""
        logger.info("언어 감지 및 필터링 시작...")
        
        if text_columns is None:
            text_columns = df.select_dtypes(include=['object']).columns.tolist()
        
        df_with_lang = df.copy()
        language_results = []
        
        for idx, row in df_with_lang.iterrows():
            row_languages = []
            for col in text_columns:
                lang = self.detect_language(row[col])
                row_languages.append(lang)
            
            # 한국어가 하나라도 있으면 한국어로 분류
            if target_language in row_languages:
                language_results.append(target_language)
            elif 'unknown' in row_languages and len(set(row_languages)) == 1:
                language_results.append('unknown')
            else:
                lang_counts = pd.Series(row_languages).value_counts()
                language_results.append(lang_counts.index[0])
        
        df_with_lang['detected_language'] = language_results
        
        # 목표 언어 데이터만 필터링
        if target_language == 'ko':
            filtered_df = df_with_lang[
                (df_with_lang['detected_language'] == 'ko') | 
                (df_with_lang['detected_language'] == 'unknown')
            ].copy()
        else:
            filtered_df = df_with_lang[df_with_lang['detected_language'] == target_language].copy()
        
        # 언어 감지 컬럼 제거
        filtered_df = filtered_df.drop('detected_language', axis=1)
        
        logger.info(f"언어 필터링 완료: {len(filtered_df)}행 남음")
        return filtered_df
    
    def clean_data(self, file_path, text_columns=None, sheet_name=None, target_language='ko', header=0):
        """전체 데이터 정제 프로세스 실행"""
        logger.info("데이터 정제 프로세스 시작")
        
        # 1. 엑셀 파일 로드
        df = self.load_excel_file(file_path, sheet_name, header)
        
        # 2. 스트레스 문항 매핑 (문항1, 문항2만)
        df = self.map_stress_values(df)
        
        # 3. 기본 정제
        df = self.basic_cleaning(df)
        
        # 4. 중복 제거
        df = self.remove_duplicates(df)
        
        # 5. 언어 필터링
        df = self.language_filtering(df, text_columns, target_language)
        
        logger.info("데이터 정제 프로세스 완료")
        return df
    
    def save_to_db(self, df, table_name, if_exists='replace'):
        """정제된 데이터를 PostgreSQL에 저장"""
        try:
            # 컬럼명 정리 (PostgreSQL 호환)
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # 데이터베이스에 저장
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            
            logger.info(f"데이터베이스 저장 완료: {table_name} 테이블에 {len(df)}행 저장")
            
            # 저장된 데이터 확인
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                logger.info(f"저장 확인: {table_name} 테이블에 총 {count}행 존재")
                
        except Exception as e:
            logger.error(f"데이터베이스 저장 실패: {e}")
            raise
    
    def process_and_save(self, file_path, table_name, text_columns=None, sheet_name=None, 
                        target_language='ko', if_exists='replace', header=0):
        """전체 프로세스: 정제 + 저장"""
        # 데이터 정제
        cleaned_df = self.clean_data(file_path, text_columns, sheet_name, target_language, header)
        
        # 데이터베이스 저장
        self.save_to_db(cleaned_df, table_name, if_exists)
        
        return cleaned_df


# 사용 예제
def main():
    # 데이터베이스 설정
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': '??',
        'username': '??',
        'password': '??'
    }
    
    # 문항1, 문항2만 매핑
    stress_mappings = {
        '문항1': {
            1: '업무/학업',
            2: '출퇴근',
            3: '인간관계',
            4: '경제적 문제',
            5: '건강 문제',
            6: '기타'
        },
        '문항2': {
            1: '수면',
            2: '음식 섭취',
            3: '운동',
            4: '명상/휴식',
            5: '마사지/스파',
            6: '기타'
        }
    }
    
    # 데이터 클리너 초기화
    cleaner = ExcelDataCleaner(db_config, stress_mappings)
    
    # 엑셀 파일 경로 및 설정
    excel_file_path = '??'
    table_name = 'cleaned_data'
    
    # 텍스트 분석할 컬럼 지정
    text_columns = ['구분', '고유번호', '성별', '나이', '지역', '설문일시']
    
    try:
        # 데이터 정제 및 저장
        result_df = cleaner.process_and_save(
            file_path=excel_file_path,
            table_name=table_name,
            text_columns=text_columns,
            sheet_name=None,
            target_language='ko',
            if_exists='replace',
            header=1  # 두 번째 행을 헤더로 사용
        )
        
        print(f"정제 완료! 최종 데이터 행 수: {len(result_df)}")
        print("\n매핑 적용 후 데이터 미리보기:")
        available_cols = [col for col in ['구분', '문항1', '문항2'] if col in result_df.columns]
        if available_cols:
            print(result_df[available_cols].head())
        else:
            print(result_df.head())
        
    except Exception as e:
        logger.error(f"프로세스 실행 중 오류 발생: {e}")


if __name__ == "__main__":
    main()