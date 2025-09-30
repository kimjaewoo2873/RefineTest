# db_manager.py
"""
데이터베이스 연결 및 저장 관리
"""
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL 데이터베이스 연결 및 저장 관리 클래스"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.engine = None
        self.connect()
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['username']}:"
                f"{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/"
                f"{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            logger.info("데이터베이스 연결 성공")
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise
    
    def save_dataframe(self, df, table_name, if_exists='replace'):
        """DataFrame을 데이터베이스에 저장"""
        try:
            # 컬럼명 정리 (PostgreSQL 호환)
            df_copy = df.copy()
            df_copy.columns = [
                col.lower().replace(' ', '_').replace('-', '_')  #공백,- 을  -> _ 로 변경
                for col in df_copy.columns
            ]
            
            # 데이터베이스에 저장
            df_copy.to_sql(
                table_name, 
                self.engine, # self.engine은 데이터베이스 연결 객체 
                if_exists=if_exists, # main.py에서 지정한 값 사용
                index=False, # 인덱스 컬럼 저장 안함
                method='multi' # 여러 행을 한번에 삽입(성능 향상)
            )
            
            logger.info(f"데이터베이스 저장 완료: {table_name} 테이블에 {len(df_copy)}행 저장")
            
            # 저장된 데이터 확인
            self._verify_save(table_name)
            
        except Exception as e:
            logger.error(f"데이터베이스 저장 실패: {e}")
            raise
    
    def _verify_save(self, table_name):
        """저장된 데이터 검증"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                logger.info(f"저장 확인: {table_name} 테이블에 총 {count}행 존재")
        except Exception as e:
            logger.warning(f"저장 검증 실패: {e}")