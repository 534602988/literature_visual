from database_process import collection_name_upper, update_from_csv, update_from_raw, rename
from oa import oa_process
from run_shap import shap_run


def main():
    # delete_repeat_and_backup('social')
    # folder = 'data/country'
    # all_path = read_csv_folder(folder)
    # for csv_file in all_path:
    #     print(csv_file)
    #     import_csv_to_mongodb(csv_file, 'raw_data_country')
    # update_author_cited()
    # for country in ['中国', '美国', '英国']:
    #     print(country)
    #     run_get_Altmetric(country)
    #     run_to_academic(country)
    # update_from_csv('data/y_data.csv','academic','AAS','DOI')
    # update_from_raw('Open Access Designations')
    # oa_process()

    # rename('academic','Oa_type','OA')
    # collection_name_upper('academic')
    for y_column in ['AAS', 'F_academic',
                     'F_social', 'Times Cited, All Databases']:
        x_column = ['Journal_ifactor', 'Publication_type', 'Fund_count', 'Fund_type', 'Author_count',
                    'Instituion_count', 'Cooperate_way', 'Avg_author_cited', 'Author_cited', 'Literature_length',
                    'Language', 'Cited reference count', 'Oa type', 'Document type']
        shap_run('academic', 'All', x_column, y_column)


if __name__ == '__main__':
    main()
