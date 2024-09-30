import re

import pandas as pd
from pymongo import MongoClient

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def oa_reflection(oa_string):
    oa_string = oa_string.lower()
    color = ['bronze', 'green', 'hybrid', 'gold']
    pattern = r'\b(?:' + '|'.join(color) + r')\b'
    # 使用re.findall查找匹配的元素
    matches = re.findall(pattern, oa_string)
    if matches:
        if len(set(matches)) != 1:
            oa_list = oa_string.split(',')
            shortest_string = min(oa_list, key=len)
            return str(shortest_string).strip()
        else:
            return matches[0].strip()
    else:
        print("字符串不包含列表中的任何元素")
        return None


def oa_process():
    data = pd.DataFrame(DATABASE['academic'].find({})).to_dict(orient='records')
    for new_record in data:
        oa_string = new_record['Open Access Designations']
        doi = new_record['DOI']
        if type(oa_string) is str:
            new_oa_type = oa_reflection(oa_string)
            filter_query = {'DOI': doi}
            update_query = {'$set': {'Oa type': new_oa_type}}
            DATABASE['academic'].update_one(filter_query, update_query)
        else:
            print(new_record['Open Access Designations'])
            print(new_record['country'])
