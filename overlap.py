import pandas as pd

# 엑셀 파일 읽기
file_path = r'C:/Users/ecopl/Desktop/qpoll 데이터/qpoll_join_250704.xlsx'
df = pd.read_excel(file_path, header=1, engine='openpyxl')
# 데이터프레임의 모든 열 이름을 리스트로 가져오기
all_columns = list(df.columns)

print("=== 비교 대상 컬럼 목록 (전체) ===")
print(all_columns)
print("=== 데이터 전체 행 수 ===")
print(f"총 {len(df)}개의 행이 있습니다.\n")


# 데이터 정규화 (대소문자 통일, 띄어쓰기 제거)
print("=== 데이터 전처리 중 ===")
print("- 대소문자를 소문자로 통일")
print("- 앞뒤 띄어쓰기 제거\n")

df_normalized = df.copy()
for col in df_normalized.columns:
    if df_normalized[col].dtype == 'object':  # 문자열 컬럼만
        df_normalized[col] = df_normalized[col].astype(str).str.lower().str.strip()

# 전체 행 기준 완전 중복 확인
print("=== 완전히 동일한 중복 행 확인 (대소문자/띄어쓰기 무시) ===")
print(f"총 {len(df.columns)}개의 컬럼을 모두 비교합니다.")
print(f"비교 중인 컬럼: {list(df.columns)}\n")
duplicates_mask = df_normalized.duplicated(keep=False)
duplicates_all = df[duplicates_mask]  # 원본 데이터로 표시

if len(duplicates_all) > 0:
    print(f"완전히 동일한 중복 행이 {len(duplicates_all)}개 있습니다.\n")
    
    # 중복된 행들을 그룹으로 묶어서 보여주기
    duplicate_groups = df_normalized[duplicates_mask].groupby(list(df_normalized.columns))
    
    group_num = 1
    for name, group in duplicate_groups:
        print(f"[중복 그룹 {group_num}] - {len(group)}개의 동일한 행:")
        # 원본 데이터로 표시 (정규화 전)
        print(df.loc[group.index])
        print(f"행 번호 (인덱스): {list(group.index)}")
        print("-" * 80)
        group_num += 1
        
    # 중복 제거 시 남을 행 수
    unique_count = len(df_normalized.drop_duplicates())
    removed_count = len(df) - unique_count
    print(f"\n중복 제거 시: {unique_count}개 행이 남고, {removed_count}개 행이 제거됩니다.")
    
else:
    print("완전히 동일한 중복 행이 없습니다.")

print("\n" + "="*50 + "\n")

