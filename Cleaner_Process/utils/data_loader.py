# data_loader.py
"""
엑셀 파일 로드 및 매핑 처리
"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """데이터 로드 및 초기 변환 클래스"""
    
    def __init__(self, question_mappings=None):
        self.question_mappings = question_mappings or {}
    
    def load_excel(self, file_path, sheet_name=None, header=0):
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
        """문항의 숫자를 의미 있는 텍스트로 변환"""
        logger.info("스트레스 문항 매핑 시작...")
        
        if not self.question_mappings:
            logger.warning("매핑 정보가 없습니다. 매핑을 건너뜁니다.")
            return df
        
        mapped_df = df.copy()
        
        for column, mapping in self.question_mappings.items():
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