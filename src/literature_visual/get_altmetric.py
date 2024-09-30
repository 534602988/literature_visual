import random
import re
import time

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def get_Altmetric_doi(doi):
    base_url = 'https://api.altmetric.com/v1/'
    api_call = f'{base_url}doi/{doi}'
    try:
        r = requests.get(api_call)
        if r.status_code == 200:
            return r.json()
        else:
            print(f'{api_call} request failed')
            return None
    except BaseException as e:
        print(e)
        return None


def extract_text(text):
    # 使用正则表达式提取第一个空格前的所有字符
    match = re.match(r'(\S+)', text).group(1)
    parts = re.split(r'(\d+)', match)
    inf_list = [part for part in parts if part]
    if len(inf_list) == 2:
        return {inf_list[0]: inf_list[1]}
    else:
        return None


def soup_search(soup: BeautifulSoup, class_str: str) -> dict:
    content = soup.find_all(class_=class_str)
    result_dict = {}
    # 输出每个元素的文本内容
    for element in content:
        texts = element.text.split('\n')
        for text in texts:
            if text != '':
                inf = extract_text(text)
                result_dict.update(inf)
        return result_dict


def get_social_from_url(url: str) -> dict:
    response = requests.get(url)
    # 检查响应状态码，确保请求成功
    social_dict = {}
    if response.status_code == 200:
        # 使用 BeautifulSoup 解析页面内容
        soup = BeautifulSoup(response.content, 'html.parser')
        for class_str in ['mention-counts', 'scholarly-citation-counts', 'reader-counts']:
            value_dict = soup_search(soup, class_str)
            if value_dict is not None:
                social_dict.update(value_dict)
    else:
        print('Failed to retrieve the webpage.')
    return {'social_value': social_dict}


def run_get_Altmetric(country= None):
    # 连接 MongoDB 服务器
    collection = DATABASE['raw_data_country']
    if country is None:
        data = collection.find({})
    else:
        data = collection.find({'country': country})
    new_collection = DATABASE['social']
    start_time = time.time()
    i = 0
    for record in data:
        if new_collection.find_one({"doi": record['DOI']}) is not None:
            # print(f"DOI {record['DOI']} already exists in get_altmetric, skipping...")
            continue
        else:
            social_result = get_Altmetric_doi(record['DOI'])
            if social_result is not None:
                url = social_result['details_url']
                social_result.update(get_social_from_url(url))
                new_collection.insert_one(social_result)
                # print('success')
                if i % 100 == 0:
                    i = i+1
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(i,end=' ')
                    print(f"Execution time:{execution_time}seconds")
                    start_time = time.time()
            else:
                new_collection.insert_one({'doi':record['DOI']})
            time.sleep(3)



def main():
    for country in ['英国']:
        try:
            run_get_Altmetric(country)
        except:
            print('error')
    # run_get_Altmetric('德国')
    # run_get_Altmetric('美国')
    # get_social_from_url('https://www.altmetric.com/details/585373')


if __name__ == '__main__':
    main()
