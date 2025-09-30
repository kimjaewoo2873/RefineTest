# main.py
"""
메인 실행 스크립트
"""
import logging
from config.db_config import DB_CONFIG, QUESTION_MAPPINGS, LOG_LEVEL
from pipeline import DataPipeline

# 로깅 설정
logging.basicConfig( # 프로그램 실행 중 발생하는 정보,경고,오류 메세지 기록
    level=LOG_LEVEL, # db_config.py에서  설정한 로그 레벨 사용
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__) # 현재 모듈 이름을 로거 이름으로 사용


def main():
    """메인 실행 함수"""
    
    # 데이터 파이프라인 초기화
    pipeline = DataPipeline(DB_CONFIG, QUESTION_MAPPINGS) # DB 설정과 매핑 정보 전달
    
    # 엑셀 파일 경로 및 설정
    excel_file_path = 'C:/Users/ecopl/Desktop/qpoll 데이터/필수/qpoll_join_250224.xlsx'  # 실제 파일 경로로 변경
    table_name = 'qpoll_250224'
    
    # 텍스트 분석할 컬럼 지정
    text_columns = ['구분', '고유번호', '성별', '나이', '지역', '설문일시']
    
    try:
        # 데이터 정제 및 저장
        result_df = pipeline.process_and_save(
            file_path=excel_file_path,
            table_name=table_name,
            text_columns=text_columns,
            sheet_name=0,  # 엑셀 파일 내 시트 번호 0 = 첫번째 시트 
            target_language='ko', # 한국어
            if_exists='replace', # 기존 테이블이 있으면 새로 만듬
            header=1  # 두 번째 행을 헤더로 사용
        )
        
        print(f"\n정제 완료! 최종 데이터 행 수: {len(result_df)}")
        print("\n매핑 적용 후 데이터 미리보기:")
        
        # 사용 가능한 컬럼만 출력
        available_cols = [
            col for col in ['구분','고유번호','성별','나이','문항1'] 
            if col in result_df.columns
        ]
        if available_cols:
            print(result_df[available_cols].head()) #
        else:
            print(result_df.head()) #
        
    except Exception as e:
        logger.error(f"프로세스 실행 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()