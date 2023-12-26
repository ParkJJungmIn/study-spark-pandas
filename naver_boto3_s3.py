# import boto3

# service_name = 's3'
# endpoint_url = 'https://kr.object.ncloudstorage.com'
# region_name = 'kr-standard'
# access_key = ''
# secret_key = ''

# if __name__ == "__main__":
    # s3 = boto3.client(service_name, endpoint_url=endpoint_url, aws_access_key_id=access_key,
    #                   aws_secret_access_key=secret_key)

#     bucket_name = 'sample-bucket'

#     s3.create_bucket(Bucket=bucket_name)

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import io


service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'kr-standard'
access_key = ''
secret_key = ''

# S3 클라이언트 초기화
s3_client = s3 = boto3.client(service_name, endpoint_url=endpoint_url, aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
bucket_name = 'data-lake'
csv_file_key = 'bronze/eventsim.csv'

# S3에서 CSV 스트리밍 읽기
response = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
lines = response['Body']._raw_stream

# 날짜별 데이터를 저장할 딕셔너리 초기화


# CSV 파일을 청크로 읽으면서 처리
chunk_size = 10000  # 적절한 청크 크기 설정
for chunk in pd.read_csv(lines, chunksize=chunk_size):
    # 'ts' 필드를 날짜 형식으로 변환
    data_by_date = {}

    chunk['date'] = pd.to_datetime(chunk['ts'], unit='ms').dt.date

    # 날짜별로 데이터 그룹화
    for date, group in chunk.groupby('date'):
        if date not in data_by_date:
            data_by_date[date] = []
        data_by_date[date].append(group)

    # 날짜별로 처리된 데이터를 Parquet으로 변환 및 업로드
    for date, groups in data_by_date.items():
        # 여러 청크를 하나의 DataFrame으로 결합
        df = pd.concat(groups)

        # Parquet으로 변환
        table = pa.Table.from_pandas(df)
        parquet_output = io.BytesIO()
        pq.write_table(table, parquet_output)

        # S3에 Parquet 파일 업로드
        parquet_file_key = f'silver/eventsim/date_id={date}/data.parquet'
        s3_client.put_object(Bucket=bucket_name, Key=parquet_file_key, Body=parquet_output.getvalue())


