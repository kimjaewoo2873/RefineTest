# pipeline.py
"""
전체 데이터 처리 파이프라인
"""
import logging
from cleaners.data_cleaner import DataCleaner
from cleaners.language_filter import LanguageFilter
from utils.db_manager import DatabaseManager
from utils.data_loader import DataLoader

logger = logging.getLogger(__name__)


class DataPipeline:
    """전체 데이터 처리 파이프라인 클래스"""
    
    def __init__(self, db_config, question_mappings=None):
        self.db_manager = DatabaseManager(db_config)
        self.data_loader = DataLoader(question_mappings)
        self.data_cleaner = DataCleaner()
        self.language_filter = LanguageFilter()
    
    def process(self, file_path, text_columns=None, sheet_name=None, 
                target_language='ko', header=1):
        """전체 데이터 정제 프로세스 실행"""
        logger.info("데이터 정제 프로세스 시작")
        
        # 1. 엑셀 파일 로드
        df = self.data_loader.load_excel(file_path, sheet_name, header)
        
        # 2. 스트레스 문항 매핑
        df = self.data_loader.map_stress_values(df)
        
        # 3. 기본 정제
        df = self.data_cleaner.basic_cleaning(df)
        
        # 4. 중복 제거
        df = self.data_cleaner.remove_duplicates(df)
        
        # 5. 언어 필터링
        df = self.language_filter.filter_by_language(df, text_columns, target_language)
        
        logger.info("데이터 정제 프로세스 완료")
        return df
    
    # 기본값 None -> main.py에서 지정한 값이 전달됨
    def process_and_save(self, file_path, table_name, text_columns=None, 
                        sheet_name=None, target_language='ko',   
                        if_exists='replace', header=0):
        """전체 프로세스: 정제 + 저장"""
        # 데이터 정제
        cleaned_df = self.process(
            file_path, 
            text_columns, 
            sheet_name, 
            target_language, 
            header
        )
        
        # 데이터베이스 저장
        self.db_manager.save_dataframe(cleaned_df, table_name, if_exists)
        
        return cleaned_df