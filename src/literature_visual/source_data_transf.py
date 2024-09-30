import json
import re
import time

import pandas as pd
import requests
from pymongo import MongoClient
from zhipuai import ZhipuAI

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def get_ifactor(journal_name):
    ifactor = None
    url = f"https://www.pubmed.pro/api/openjournal/findJournal?name={journal_name}&page=1"
    # Send a GET request to the URL
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # If successful, print the response content

        json_content = response.content
        # Convert JSON bytes to string
        json_string = json_content.decode('utf-8')

        # Parse the JSON string into a Python dictionary
        journal_info = json.loads(json_string)

        # Access the journal information
        journal = journal_info['data'][0]
        ifactor = journal['ifactor']
    else:
        # If not successful, print the status code
        print("Request failed with status code:", response.status_code)
    return ifactor


def publication(record: dict):
    month_dict = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                  'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12, 'SPR': 3, 'sun': '6', 'FAL': 9,
                  'WIN': 12}
    md = record['Publication Date']
    y = record['Publication Year']
    if len(md) != None:
        day = '01'
        year = str(y)
        month = str(month_dict[md])
        date = f'{year}-{month}-{day}'
    else:
        month = None
        date= None
    journal_name = record['Source Title']
    publication_type = record['Publication Type']
    oa = record['Open Access Designations']
    ifactor = get_ifactor(journal_name)
    publication_record = {
        'month': month,
        'date': date,
        'journal_ifactor': ifactor,
        'publication_type': publication_type,
        'oa_type': oa}
    return publication_record


def extract_consecutive_same_strings(str1, str2):
    # 将两个字符串分割成单词列表
    words1 = set(str1.split())
    words2 = set(str2.split())

    # 提取相同的单词
    common_words = words1.intersection(words2)

    # 将相同的单词组合成字符串
    common_str = ' '.join(common_words)

    return common_str


def get_fund_country(fund_name):
    client = ZhipuAI(
        api_key="01136e0a094db3cbc528c742938a184b.1opSrdcvCxaYNujD")  # 填写您自己的APIKey
    response = client.chat.completions.create(
        model="glm-4",  # 填写需要调用的模型名称
        messages=[
            {"role": "user", "content": f"{fund_name}是属于哪个国家或地区的"},
        ],
    )
    return response.choices[0].message.content


def get_fund_type(fund_list, country):
    if len(fund_list) == 0:
        fund_class = 0
    else:
        fund_single_list = []
        for fund_name in fund_list:
            fund_inf = get_fund_country(fund_name)
            if country in str(fund_inf):
                fund_single_list.append(1)
            else:
                fund_single_list.append(2)
        if len(set(fund_single_list)) != 1:
            fund_class = 2
        else:
            fund_class = fund_single_list[0]
    return fund_class


def fund(record: dict):
    fund_list = str(record['Funding Orgs']).split(';')
    count = len(fund_list)
    fund_class = get_fund_type(
        fund_list, str(record['country']))
    return {'fund_count': count, 'fund_type': fund_class}


def search_cooperate_way(author_list):
    cooperate_way = None
    # 0:独立作者，1：国内合作，2：国外合作
    if len(author_list) == 1:
        cooperate_way = 0
    else:
        country_list = []
        for author in author_list:
            author = author.strip()
            if author is not None:
                country = DATABASE['author'].find_one({"name": {"$regex": f".*{author}.*"}},
                                                      {'_id': 0, 'country': 1})
                if country is not None:
                    country_list.append(country['country'])
        for i in range(len(country_list) - 1):
            if len(extract_consecutive_same_strings(
                    country_list[i], country_list[i + 1])) < 3:
                cooperate_way = 2
            else:
                cooperate_way = 1
    return cooperate_way


def get_author_cited(author_list):
    author_count = len(author_list)
    sum_cited_count = 0
    for author in author_list:
        author = author.strip()
        search_f = f".*{author}.*"
        author_db = DATABASE['author']
        data = author_db.find_one({"name": {"$regex": search_f}},
                                  {'cited_count': 1})
        if data is not None:
            total_count = int(data['cited_count'])
            sum_cited_count += total_count
        return sum_cited_count, sum_cited_count / author_count


def author_stats(record: dict):
    author_count = len(str(record['Author Full Names']).split(';'))
    instituion_count = len(str(record['Affiliations']).split(';'))
    cooperate_way = search_cooperate_way(
        str(record['Author Full Names']).split(';'))
    author_list = str(record['Author Full Names']).split(';')
    author_cited, avg_author_cited = get_author_cited(author_list)
    publication_record = {'author_count': author_count, 'instituion_count': instituion_count,
                          'cooperate_way': cooperate_way, 'avg_author_cited': avg_author_cited,
                          'author_cited': author_cited}
    return publication_record


def literature_length(record: dict):
    length = None
    try:
        length = int(record['End Page']) - int(record['Start Page'])
    except Exception as e:
        print(e)
        print(record['DOI'])
    return {'literature_length': length}


def single_record(test_record, mode='insert'):
    new_record = {}
    if mode == 'insert':
        new_record['Language'] = test_record['Language']
        new_record['Cited Reference Count'] = test_record['Cited Reference Count']
        new_record['Times Cited, All Databases'] = test_record['Times Cited, All Databases']
        new_record['DOI'] = test_record['DOI']
        new_record['country'] = test_record['country']
        new_record.update(publication(test_record))
        new_record.update(fund(test_record))
        new_record.update(author_stats(test_record))
        new_record.update(literature_length(test_record))
    else:
        print('update')
    return new_record


def oa_reflection(oa_string):
    color = ['Bronze', 'Green', 'hybrid', 'gold']
    pattern = r'\b(?:' + '|'.join(color) + r')\b'

    # 使用re.findall查找匹配的元素
    matches = re.findall(pattern, oa_string)
    if matches:
        print(f"字符串中包含列表中的元素: {matches}")
        return matches
    else:
        print("字符串不包含列表中的任何元素")
        return None


def run_to_academic(country=None):
    if country is None:
        data = pd.DataFrame(DATABASE['raw_data_country'].find({})).to_dict(orient='records')
    else:
        data = pd.DataFrame(DATABASE['raw_data_country'].find(
            {'country': country})).to_dict(orient='records')
    new_collection = DATABASE['academic']
    i = 0
    for record in data:
        i = i + 1
        if new_collection.find_one({'DOI': record['DOI']}) is not None:
            continue
        else:
            result = single_record(record)
            print(result)
            new_collection.insert_one(result)
        if i % 300 == 1:
            print(
                i,
                ':',
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(
                        time.time())))


def update_one_field(field):
    output_collection = DATABASE['academic']
    input_collection = DATABASE['raw_data_country']
    update_count = 0
    if field == 'Cooperate_way':
        for record in output_collection.find({}):
            doi = record['DOI']
            search_result = input_collection.find_one({'DOI': doi}, {'Author Full Names': 1})
            if search_result is not None:
                cooperate_way = search_cooperate_way(
                    str(search_result['Author Full Names']).split(';'))
                output_collection.update_one({'DOI': doi}, {'$set': {field: cooperate_way}})
                update_count = update_count + 1
    if field == 'Instituion_count':
        for record in output_collection.find({}):
            doi = record['DOI']
            search_result = input_collection.find_one({'DOI': doi}, {'Affiliations': 1})
            if search_result is not None:
                instituion_count = len(
                    str(search_result['Affiliations']).split(';'))
                output_collection.update_one({'DOI': doi}, {'$set': {field: instituion_count}})
                update_count = update_count + 1
    print(f'update {update_count} records in {field} field')
    return update_count


def main():
    # print(search_cooperate_way(['Sun, Qi', 'Sun, Qi']))
    update_one_field('Instituion_count')
    # run_to_academic()
    return None


if __name__ == '__main__':
    main()
