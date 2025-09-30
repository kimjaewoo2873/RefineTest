# language_filter.py
"""
언어 감지 및 필터링 로직
"""
import pandas as pd
import re
import logging
from langdetect import detect, DetectorFactory

# 언어 감지 결과의 일관성을 위해 시드 설정
DetectorFactory.seed = 0

logger = logging.getLogger(__name__)


class LanguageFilter:
    """언어 감지 및 필터링 클래스"""
    
    @staticmethod
    def detect_language(text):
        """텍스트의 언어 감지"""
        try:
            if pd.isna(text) or str(text).strip() == '':
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
    
    @classmethod
    def filter_by_language(cls, df, text_columns=None, target_language='ko'):
        """언어 감지 및 필터링"""
        logger.info("언어 감지 및 필터링 시작...")
        
        if text_columns is None:
            text_columns = df.select_dtypes(include=['object']).columns.tolist()
        
        df_with_lang = df.copy()
        language_results = []
        
        for idx, row in df_with_lang.iterrows():
            row_languages = []
            for col in text_columns:
                lang = cls.detect_language(row[col])
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
        
        # 목표 언어 데이터만 필터링 / 한국어 컬럼이 하나라도 있는 행 & 모든 컬럼이 판단 불가능한 행(unknown)
        if target_language == 'ko':
            filtered_df = df_with_lang[
                (df_with_lang['detected_language'] == 'ko') | 
                (df_with_lang['detected_language'] == 'unknown')
            ].copy()
        else:
            filtered_df = df_with_lang[
                df_with_lang['detected_language'] == target_language
            ].copy()
        
        # 언어 감지 컬럼 제거,
        filtered_df = filtered_df.drop('detected_language', axis=1)
        
        logger.info(f"언어 필터링 완료: {len(filtered_df)}행 남음")
        return filtered_df