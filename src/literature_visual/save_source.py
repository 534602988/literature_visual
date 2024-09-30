import re

import pandas as pd
from pymongo import MongoClient

from xls2csv import read_csv_folder

CLIENT = MongoClient('mongodb://localhost:27017/')
# 创建或者获取名为 'cdl' 的数据库
DATABASE = CLIENT['cdl']


# <editor-fold desc="Description">
def import_csv_to_mongodb(filename: str,collection_name):
    collection = DATABASE[collection_name]
    data = pd.read_csv(filename)
    print(data.shape)
    data['filename'] = filename
    countrys_list = ['中国', '英国', '美国']


    # 构建正则表达式模式
    pattern = '|'.join(map(re.escape, countrys_list))

    # 使用正则表达式进行匹配
    matches = re.findall(pattern, filename)
    # 输出匹配结果
    if matches:
        country = matches[0]
    else:
        country = None
        print("未匹配到任何国家")
    data['country'] = country
    data_dict = data.to_dict(orient='records')
    i = 0
    for record in data_dict:
        doi = record['DOI']
        if collection.find_one({'DOI': doi}) is None or 'raw_data_country' not in DATABASE.list_collection_names():
            collection.insert_one(record)
            i=i+1
        else:
            print(record['DOI'])
    print(f'insert {i} pieces of data successfully')


# </editor-fold>

def main():
    folder = 'data/country'
    all_path = read_csv_folder(folder)
    for csv_file in all_path:
        print(csv_file)
        import_csv_to_mongodb(csv_file,'raw_data_country')


if __name__ == '__main__':
    main()
