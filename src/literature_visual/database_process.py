import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']

import datetime


from datetime import datetime, timedelta

def create_backup_database(collection_name: str, backup_suffix: str):
    backup_db_name = f"{collection_name}_{backup_suffix}"
    backup_collection = DATABASE[backup_db_name]
    collection = DATABASE[collection_name]
    all_collections = DATABASE.list_collection_names()
    print(all_collections)
    # 遍历所有集合的名称
    for name in all_collections:
        # 检查集合名称是否符合要删除的模式
        if name.startswith(f'{collection_name}_'):
            # 删除符合条件的集合
            DATABASE.drop_collection(name)
            print(f"Collection '{name}' deleted.")
    # 使用find()获取集合中的所有文档，并插入到备份集合中
    backup_collection.insert_many(collection.find())

    print(f"Backup database '{backup_db_name}' created successfully.")



def delete_repeat_database(collection_name: str, field):
    data = pd.DataFrame(DATABASE[collection_name].find())
    new_data = data.drop_duplicates(subset=[field])
    print(data)
    DATABASE[f'{collection_name}_unrepeated'].insert_many(new_data.to_dict(orient='records'))


def delete_repeat_and_backup(collection_name: str, field=None):
    collection = DATABASE[collection_name]
    all_documents = list(collection.find())
    df = pd.DataFrame(all_documents)
    if field is None:
        new_df = df.drop_duplicates()
    else:
        new_df = df.drop_duplicates(subset=[field])
    create_backup_database(collection_name, datetime.now().strftime("%Y%m%d%H%M%S"))
    collection.delete_many({})
    collection.insert_many(new_df.to_dict(orient='records'))



def delete_all(collection_name: str):
    collection = DATABASE[collection_name]
    # 提示用户确认删除操作
    confirm = input(f"Are you sure you want to delete all data from '{collection_name}' collection? (y/n): ")

    if confirm.lower() == 'y':
        # 删除集合中的所有文档
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from '{collection_name}' collection.")
    elif confirm.lower() == 'n':
        print("Operation canceled. No data deleted.")
    else:
        print("Invalid input. Please enter 'y' to confirm or 'n' to cancel.")


def field_statistics(collection_name: str):
    collection = DATABASE[collection_name]
    documents = collection.find({"filename": {"$regex": ".*美国.*"}})
    unique_dois = set()
    for document in documents:
        if 'DOI' in document:
            unique_dois.add(document['DOI'])

    # 输出不同DOI字段的数量
    print(f"共有不同的DOI字段数量为：{len(unique_dois)}")



def out8country():
    collection0 = DATABASE['raw_data_country']
    collection1 = DATABASE['social']
    query = collection1.find({}, {})
    df = pd.DataFrame(list(query))
    # 对非字典值进行处理（假设用 None 填充）
    df['social_value'] = df['social_value'].apply(lambda x: {} if isinstance(x, float) else x)
    # 将字典数据展开成多列
    df_social_value = pd.DataFrame(df['social_value'].tolist())
    df_merge = pd.concat([df['doi'], df['score'], df_social_value], axis=1).to_dict(orient='records')
    for record in df_merge:
        for country in ['中国', '英国', '美国']:
            search_f = f".*{country}.*"
            collection_temp = DATABASE[country]
            print(search_f)
            filename = collection0.find_one({'DOI': record['doi'], 'filename': {"$regex": search_f}},
                                            {'filename': 1, '_id': 0})
            if filename is not None:
                record['country'] = country
                collection_temp.insert_one(record)
                print(country)
            else:
                print('None')


def get_country():
    collection = DATABASE['raw_data_country']
    for country in ['中国', '德国', '美国', '英国']:
        search_f = f".*{country}.*"
        filter_query = {'filename': {"$regex": search_f}}
        if country == 'china':
            country = '中国'
        update_data = {'$set': {'country': country}}
        result = collection.update_many(filter_query, update_data)
        print(f"Modified {result.modified_count} document")  # 输出更新的文档数


def get_author_cited(author):
    author = author.strip()
    all_literature = DATABASE['raw_data']
    data = pd.DataFrame(all_literature.find({"Author Full Names": {"$regex": f".*{author}.*", "$options": "i"}},
                                            {'Times Cited, All Databases': 1}))
    if len(data) != 0:
        total_count = data['Times Cited, All Databases'].sum()
        average_count = data['Times Cited, All Databases'].mean()
        return int(total_count), int(average_count)
    else:
        # print(f'{author} is not found in raw_data')
        return None, None


def update_author_cited():
    collection = DATABASE['author']
    data = pd.DataFrame(collection.find({}, {'name': 1})).to_dict(orient='records')
    i = 0
    for record in data:
        name = record['name']
        if collection.find_one({'name': name}) is None or collection.find_one({'name': name})['cited_count'] is None:
            count, avg_count = get_author_cited(name)
            collection.update_many({'name': name}, {"$set": {'cited_count': count, 'avg_cited_count': avg_count}})
            i = i + 1
        else:
            continue
            # print(f'{name} is existed in author database')
        if i % 1000 == 0:
            print(i)
    return None


def update_from_raw(field):
    data = pd.DataFrame(DATABASE['academic'].find({})).to_dict(orient='records')
    for record in data:
        if DATABASE['raw_data_country'].find_one({'DOI': record['DOI']}) is not None:
            field_value = DATABASE['raw_data_country'].find_one({'DOI': record['DOI']})[field]
            filter_query = {'DOI': record['DOI']}
            update_query = {'$set': {field: field_value}}
            DATABASE['academic'].update_one(filter_query, update_query)


def update_from_csv(insert_file_name, collection_name='academic', update_field='DOI', key_field='DOI'):
    df = pd.read_csv(insert_file_name)
    collection = DATABASE[collection_name]
    for record in collection.find({}):
        # 查找匹配的行并提取“期刊影响因子”字段的值
        if df[df[key_field] == record[key_field]].empty:
            print(f"未找到{key_field}为{record[key_field]}的数据")
        else:
            value = df.loc[df['DOI'] == record[key_field], update_field].values[0]
            print(f"{key_field}为{record[key_field]}的{update_field}为{value}")
            filter_query = {'DOI': record[key_field]}
            update_query = {'$set': {update_field: value}}
            collection.update_one(filter_query, update_query)
    return None


def rename(collection_name, old_name, new_name):
    collection = DATABASE[collection_name]
    collection.update_many({}, {'$rename': {old_name: new_name}})


def search_source_target_key_field(source: str, target: str, key: str, field: str):
    collection_db1 = DATABASE[source]
    collection_db2 = DATABASE[target]

    # 遍历 db2 中的每个文档
    for doc_db2 in collection_db2.find():
        # 获取 db1 中具有相同键值的文档
        doc_db1 = collection_db1.find_one({key: doc_db2[key.upper()]})

        # 如果找到匹配的文档，则更新字段值
        if doc_db1:
            new_field_value = doc_db2[field]
            collection_db1.update_one({key: doc_db2[key.upper()]}, {'$set': {field: new_field_value}})
            print(f"Updated field value for key '{doc_db2[key.upper()]}' in db1.")
        else:
            print(f"No matching document found for key '{doc_db2[key.upper()]}' in db1.")

    print("Update process completed.")
    return None


def collection_name_upper(collection_name):
    collection = DATABASE[collection_name]

    # 获取集合中的所有文档
    documents = list(collection.find())

    # 遍历每个文档并更新字段名称为首字母大写
    for doc in documents:
        updated_doc = {}
        for key, value in doc.items():
            updated_key = key.capitalize()  # 首字母大写
            updated_doc[updated_key] = value
        # 更新文档
        collection.update_one({'_id': doc['_id']}, {'$set': updated_doc})

    print("Field names updated successfully.")
    return None


def main():

    # delete_repeat_and_backup('author','name')
    # get_country()
    # update_author_cited()
    # new_column_names = ['Journal_ifactor', 'Publication_type', 'Fund_count', 'Fund_type', 'Author_count',
    #                     'Instituion_count', 'Cooperate_way', 'Avg_author_cited', 'Author_cited', 'Literature_length',
    #                     'Language', 'Cited reference count', 'Oa type']
    # old_column_names = ['journal_ifactor', 'publication_type', 'fund_count', 'fund_type', 'author_count',
    #                     'instituion_count', 'cooperate_way', 'avg_author_cited', 'update_author_cited', 'literature_length',
    #                     'Language',
    #                     'Cited Reference Count', 'OA Type']
    # name_dict = dict(zip(old_column_names,new_column_names))
    # for old_name in old_column_names:
    #     if old_name != name_dict[old_name]:
    #         rename('academic', old_name, name_dict[old_name])
    # update_from_csv('data/y_value.csv','academic','F_social','DOI')
    update_from_raw('Document Type')

    # print(get_author_cited('Bao, Cathy Ge'))
    # get_country()
    # delete_repeat_and_backup('addresses', 'name')
    # delete_all('social_simply')
    # field_statistics('raw_data')
    # out8country()
    return None


if __name__ == '__main__':
    main()
