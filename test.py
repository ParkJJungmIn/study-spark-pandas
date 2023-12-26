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


# buffer = []  # 아이템을 저장할 버퍼
# buffer_size = 1000  # 버퍼 크기 설정


# with open(file_path, 'r') as file:
#     for item in ijson.items(file, 'item'):
#         buffer.append(item)
#         if len(buffer) >= buffer_size:
#             # DataFrame 생성
#             df = pd.DataFrame(buffer)
#             # 일괄 처리를 위한 SQL 문 생성
#             insert_statement = pd.io.sql.get_insert_statement(table_name='user_song_count', con=database_connection, schema=None, if_exists='append', index=False, dtype=dtype)
#             # 데이터베이스에 일괄 삽입
#             try:
#                 database_connection.execute(insert_statement, df.to_dict(orient='records'))
#             except SQLAlchemyError as e:
#                 print(f"Error occurred: {e}")
#             buffer = []  # 버퍼 초기화

#     # 남은 데이터 처리
#     if buffer:
#         df = pd.DataFrame(buffer)
#         insert_statement = pd.io.sql.get_insert_statement(table_name='user_song_count', con=database_connection, schema=None, if_exists='append', index=False, dtype=dtype)
#         try:
#             database_connection.execute(insert_statement, df.to_dict(orient='records'))
#         except SQLAlchemyError as e:
#             print(f"Error occurred: {e}")

# with open('event.data.json', 'r') as file:
#     for item in ijson.items(file, 'item'):
#         buffer.append(item)
#         if len(buffer) >= buffer_size:
#             df = pd.DataFrame(buffer)
#             df.to_sql(con=database_connection, name='user_song_count', if_exists='append', index=False)
#             buffer = []  # 버퍼 초기화

#     if buffer:  # 남은 아이템 처리
#         df = pd.DataFrame(buffer)
#         df.to_sql(con=database_connection, name='user_song_count', if_exists='append', index=False)

# with open('event.data.json', 'r') as file:
#     # ijson은 파일을 스트리밍하며 'item' 이벤트를 발생시킵니다.
#     for item in ijson.items(file, 'item'):
#         # 각 item을 DataFrame으로 변환
#         df = pd.DataFrame([item])
#         # 데이터베이스에 데이터 저장
#         df.to_sql(con=database_connection, name='user_song_count', if_exists='append', index=False)

# # 청크 크기 설정
chunk_size = 1000  # 적절한 청크 크기 설정

# JSON 파일을 청크 단위로 읽기
for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
    # 데이터베이스에 데이터 저장
    try:
        chunk.to_sql(con=database_connection, name=table_name, if_exists='append', index=False, dtype=dtype)
    except Exception as e:
        print(f"Error occurred: {e}")
        # 오류 발생 시 로그를 남기고 다음 청크로 계속 진행
        continue


# # 데이터베이스에 데이터 저장
# chunk_size = 100  # 적절한 청크 크기 설정
# for i in range(0, len(df), chunk_size):
#     chunk = df[i:i + chunk_size]
#     try:
#         chunk.to_sql(con=database_connection, name=table_name, if_exists='append', index=False, dtype=dtype, chunksize=chunk_size)
#     except Exception as e:
#         print(f"Error occurred with chunk starting at row {i}: {e}")
#         # 오류 발생 시 로그를 남기고 다음 청크로 계속 진행
#         continue


# 실행 완료 후 경과 시간 출력
end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

# 데이터베이스 연결 종료 (선택적)
# database_connection.dispose()