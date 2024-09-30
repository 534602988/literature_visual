# 此文档用于将作者数据更新到academic数据库中，因为该部分已在source_data_transf.py文件中
# 故废弃该文件


import time
import pandas as pd
from pymongo import MongoClient

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def get_author_cited(author_list):
    author_count = len(author_list)
    sum_cited_count = 0
    for author in author_list:
        author = author.strip()
        search_f = f".*{author}.*"
        author_db = DATABASE['author']
        data = author_db.find_one({"name": {"$regex": search_f}},
                                  {'cited_count': 1, '_id': 0})
        if data is not None and data != {}:
            if data['cited_count'] is not None:
                total_count = int(data['cited_count'])
                sum_cited_count += total_count
        return sum_cited_count, sum_cited_count / author_count


def author_stats(record: dict):
    author_cited, avg_author_cited = None, None
    author_list = str(record['Author Full Names']).split(';')
    author_cited, avg_author_cited = get_author_cited(author_list)
    # try:
    #     author_list = str(record['Author Full Names']).split(';')
    #     author_cited, avg_author_cited = get_author_cited(author_list)
    # except Exception as e:
    #     print(e)
    #     print('author wrong')
    #     print(record['DOI'])
    return author_cited, avg_author_cited


def main():
    country = '英国'
    data = pd.DataFrame(DATABASE['raw_data_country'].find({'country': country})).to_dict(orient='records')
    new_collection = DATABASE['academic']
    i = 0
    for record in data:
        i = i + 1
        if new_collection.find_one({'DOI': record['DOI']}) is not None:
            author_cited, avg_author_cited = author_stats(record)
            update = {'$set': {'Author_cited': author_cited, 'Avg_author_cited': avg_author_cited}}
            new_collection.update_one({'DOI': record['DOI']}, update)
        else:
            doi = record['DOI']
            print(f'{doi} is None')
        if i % 300 == 1:
            print(i, ':', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    return None


if __name__ == '__main__':
    main()
