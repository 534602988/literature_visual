from pymongo import MongoClient

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def split_funding(inf: str) -> list[str]:
    return inf.split(';')


def get_funding(collection_name: str):
    collection = DATABASE[collection_name]
    data = collection.find({"Funding Orgs": {"$exists": True, "$ne": ""}})
    fund_collection = DATABASE['fundings']
    for record in data:
        doi = record['DOI']
        fundings = record['Funding Orgs']
        if type(fundings) is not float:
            new_record = {'DOI': doi, 'fundings': split_funding(fundings)}
            fund_collection.insert_one(new_record)


def main():
    get_funding('raw_data')


if __name__ == '__main__':
    main()
