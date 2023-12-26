import ijson
import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy.exc import SQLAlchemyError
import time

# 시작 시간 기록
start_time = time.time()

# JSON 파일 로드
file_path = 'event.data.json'  # JSON 파일 경로
# df = pd.read_json(file_path, lines=True)

# 데이터 정제 및 전처리 (예시)
# df['new_column'] = df['existing_column'].apply(some_function)  # 새로운 컬럼 추가 또는 변환

# MySQL 데이터베이스 연결
database_username = 'root'
database_password = 'my-secret-pw'
database_ip       = 'localhost'
database_name     = 'mydatabase'
database_connection = create_engine(f'mysql+pymysql://{database_username}:{database_password}@{database_ip}/{database_name}')

table_name = 'user_song_count'


dtype={
    'ts': types.BigInteger,
    'userId': types.VARCHAR(length=255),
    'sessionId': types.Integer,
    'page': types.VARCHAR(length=255),
    'auth': types.VARCHAR(length=255),
    'method': types.VARCHAR(length=255),
    'status': types.Integer,
    'level': types.VARCHAR(length=255),
    'itemInSession': types.Integer,
    'location': types.VARCHAR(length=255),
    'userAgent': types.VARCHAR(length=255),
    'lastName': types.VARCHAR(length=255),
    'firstName': types.VARCHAR(length=255),
    'registration': types.BigInteger,
    'gender': types.VARCHAR(length=255),
    'tag': types.VARCHAR(length=255),
    'artist': types.VARCHAR(length=255),
    'song': types.VARCHAR(length=255),
    'length': types.Float
}


chunk_size = 1000

aggregated_chunks = []

# JSON 파일을 청크 단위로 읽고 집계
for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
    chunk['date'] = pd.to_datetime(chunk['ts'], unit='ms').dt.date
    aggregated_chunk = chunk.groupby(['date', 'level', 'location', 'gender']).agg({
        'userId': pd.Series.nunique,
        'sessionId': pd.Series.nunique,
        'song': 'count',
        'length': 'sum'
    }).rename(columns={
        'userId': 'unique_user_count',
        'sessionId': 'unique_session_count',
        'song': 'total_song_plays',
        'length': 'total_play_time'
    }).reset_index()
    aggregated_chunks.append(aggregated_chunk)

# 모든 청크의 집계 결과를 하나의 DataFrame으로 합침
final_aggregated_df = pd.concat(aggregated_chunks, ignore_index=True)

# 최종 집계 수행
final_aggregated_df = final_aggregated_df.groupby(['date', 'level', 'location', 'gender']).sum().reset_index()

# 데이터베이스에 저장
try:
    final_aggregated_df.to_sql(con=database_connection, name='user_daily_song_count2', if_exists='append', index=False)
except Exception as e:
    print(f"Error occurred: {e}")




# aggregated_data = []  # 집계된 데이터를 저장할 리스트

# aggregated_data = pd.DataFrame()

# # JSON 파일을 청크 단위로 읽고 집계
# for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
#     chunk['date'] = pd.to_datetime(chunk['ts'], unit='ms').dt.date
#     aggregated_chunk = chunk.groupby(['date', 'level', 'location', 'gender']).agg({
#         'userId': pd.Series.nunique,
#         'sessionId': pd.Series.nunique,
#         'song': 'count',
#         'length': 'sum'
#     }).rename(columns={
#         'userId': 'unique_user_count',
#         'sessionId': 'unique_session_count',
#         'song': 'total_song_plays',
#         'length': 'total_play_time'
#     }).reset_index()
#     # aggregated_data.append(aggregated_chunk)

#     aggregated_data = pd.concat([aggregated_data, aggregated_chunk], ignore_index=True)

# # 모든 청크의 집계 결과를 하나의 DataFrame으로 합침
# # final_aggregated_df = pd.concat(aggregated_data)

# # 데이터베이스에 저장
# try:
#     aggregated_data.to_sql(con=database_connection, name='user_daily_song_count2', if_exists='append', index=False)
# except Exception as e:
#     print(f"Error occurred: {e}")

# # JSON 파일을 청크 단위로 읽고 집계 후 데이터베이스에 저장
# for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
#     # 데이터 전처리
#     # 예: chunk['new_column'] = some_transformation(chunk['existing_column'])
#     chunk['date'] = pd.to_datetime(chunk['ts'], unit='ms').dt.date
#     # 집계 작업 수행
#     aggregated_chunk = chunk.groupby(['date', 'level', 'location', 'gender']).agg({
#         'userId': pd.Series.nunique,
#         'sessionId': pd.Series.nunique,
#         'song': 'count',
#         'length': 'sum'
#     }).rename(columns={
#         'userId': 'unique_user_count',
#         'sessionId': 'unique_session_count',
#         'song': 'total_song_plays',
#         'length': 'total_play_time'
#     }).reset_index()

#     # 위 코드는 현재 date, level, location, gender 가 안나오고 있다 -> 이걸 처리할 수 있는 방법은?
#     # print( aggregated_chunk.head() )

#     # # 데이터베이스에 저장
#     try:
#         aggregated_chunk.to_sql(con=database_connection, name='user_daily_song_count2', if_exists='append', index=False)
#     except Exception as e:
#         print(f"Error occurred: {e}")
#         continue


# 실행 완료 후 경과 시간 출력
end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

# 데이터베이스 연결 종료 (선택적)
# database_connection.dispose()