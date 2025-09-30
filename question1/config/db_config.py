# config.py
"""
데이터베이스 및 매핑 설정
"""

# 데이터베이스 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'Qpoll_Test_DB',
    'username': '?',
    'password': '?'
}

# 문항 매핑 , 숫자 -> 문자열
QUESTION_MAPPINGS = {
    '문항1': {
        1: '맛있는 음식 먹기',
        2: '여행 가기',
        3: '옷/패션관련 제품 구매하기',
        4: '취미관련 제품 구매하기',
        5: '기타'
    },
}

# 로깅 설정
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'