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

# 로깅 설정
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'