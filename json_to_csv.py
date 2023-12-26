# import json
# import csv

# # JSON 파일 경로와 CSV 파일 경로 정의
# input_file_path = './event.data.json'
# output_file_path = './eventsim.csv'

# # JSON 파일을 열고 데이터를 읽는다
# with open(input_file_path, 'r', encoding='utf-8') as json_file:
#     json_data = json.load(json_file)

# # CSV 파일을 쓰기 모드로 열고 데이터를 쓴다
# with open(output_file_path, 'w', newline='', encoding='utf-8') as csv_file:
#     # CSV Writer 생성
#     writer = csv.writer(csv_file)

#     # 헤더 작성 (JSON의 키를 이용하여)
#     writer.writerow(json_data[0].keys())

#     # 각 JSON 객체를 순회하며 CSV 파일에 쓴다
#     for item in json_data:
#         writer.writerow(item.values())



import pandas as pd

input_file_path = './test.data.json'
output_file_path = './test_eventsim.csv'

chunksize = 1000  # 예를 들어 청크 크기를 1000으로 설정

# JSON 파일을 청크 단위로 읽기
json_reader = pd.read_json(input_file_path, lines=True, chunksize=chunksize)

# 청크를 순회하며 CSV 파일에 저장
for i, chunk in enumerate(json_reader):
    # 첫 번째 청크에는 헤더를 포함하고, 이후 청크는 헤더 없이 이어서 저장
    mode = 'w' if i == 0 else 'a'
    header = True if i == 0 else False

    # CSV 파일로 저장
    chunk.to_csv(output_file_path, mode=mode, header=header, index=False)


# import ijson
# import csv

# # JSON 파일 경로와 CSV 파일 경로 정의
# input_file_path = './event.data.json'
# output_file_path = './eventsim.csv'
# # JSON 파일을 스트리밍 방식으로 열기
# with open(input_file_path, 'r', encoding='utf-8') as json_file:
#     # CSV 파일을 쓰기 모드로 열기
#     with open(output_file_path, 'w', newline='', encoding='utf-8') as csv_file:
#         writer = None

#         # ijson으로 JSON 파일에서 객체 배열("items"라고 가정)의 각 객체를 순회
#         for item in ijson.items(json_file, 'items.item'):
#             if writer is None:
#                 # CSV Writer 생성 및 헤더 작성
#                 writer = csv.DictWriter(csv_file, fieldnames=item.keys())
#                 writer.writeheader()

#             # CSV 파일에 쓰기
#             writer.writerow(item)
