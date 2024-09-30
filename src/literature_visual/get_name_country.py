'''
匹配作者地址信息中的作者姓名和国家，保存到mongodb数据库
address example:"[Miao, Jianjun] Boston Univ, Dept Econ, 270 Bay State Rd, Boston, MA 02215 USA;
 [Miao, Jianjun] Cent Univ Finance & Econ, Southwestern Univ Finance & Econ, CICFS, CEMA, Beijing, Peoples R China;
 [Wang, Pengfei] Hong Kong Univ Sci & Technol, Dept Econ, Hong Kong, Hong Kong, Peoples R China"
'''

import re
from pymongo import MongoClient
CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def match_country(inf: str) -> str:
    result = ''
    # 定义正则表达式模式
    pattern = r",\s*([^,]+)$"
    # 使用正则表达式进行匹配
    match = re.search(pattern, inf)
    if match:
        result = match.group(1)
    else:
        print(f"{inf},未找到匹配的信息")
    return result


def match_name(inf: str) -> list[str]:
    result = ''
    # 定义正则表达式模式
    pattern = r"\[(.*?)\]"
    # 使用正则表达式进行匹配
    match = re.search(pattern, inf)
    if match:
        result = match.group(1)
        if ';' in result:
            result = result.split(';')
        else:
            result = [result,]
    else:
        print(f"{inf},未找到匹配的信息")
    return result


def get_country_name(collection_name: str):
    collection = DATABASE[collection_name]
    data = collection.find({'country':'英国',"Addresses": {"$exists": True, "$ne": ""}}, {
                           "Addresses": 1, "_id": 0})
    addr_collection = DATABASE[f'author']
    for record in data:
        infos = str(record['Addresses']).split('; [')
        for inf in infos:
            if inf[0] != '[':
                inf = f'[{inf}'
            country = match_country(inf)
            all_names = match_name(inf)
            if all_names != '' and country != '':
                for name in all_names:
                    addr_collection.insert_one(
                        {'name': name, 'country': country})
                    # print(f'name:{name},country:{country}')


def main():
    get_country_name('raw_data_country')


if __name__ == '__main__':
    main()
