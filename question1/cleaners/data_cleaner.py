# data_cleaner.py
"""
데이터 정제 로직 (기본 정제, 중복 제거)
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class DataCleaner:
    """데이터 정제 처리 클래스"""
    
    @staticmethod
    def basic_cleaning(df):
        """기본 정제 - 공백 제거, 결측치 처리"""
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
    
    @staticmethod
    def remove_duplicates(df):
        """중복 데이터 제거"""
        logger.info("중복 데이터 제거 시작...")
        
        initial_count = len(df)
        cleaned_df = df.drop_duplicates(keep='first')
        removed_count = initial_count - len(cleaned_df)
        
        logger.info(f"중복 제거 완료: {removed_count}개 중복 행 제거, {len(cleaned_df)}행 남음")
        return cleaned_df