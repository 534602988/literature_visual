import os

import pandas as pd


def xls2csv(input_folder):
    output_folder = f'{input_folder}_csv'
    trans_count = 0
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 获取文件夹下所有的 xls 文件
    xls_files = [f for f in os.listdir(input_folder) if f.endswith('.xls')]

    for xls_file in xls_files:
        # 读取 xls 文件
        xls_path = os.path.join(input_folder, xls_file)
        if '._' in xls_path:
            continue
        print(xls_path)
        df = pd.read_excel(xls_path)
        # 构建输出 csv 文件路径
        csv_file = os.path.splitext(xls_file)[0] + '.csv'
        csv_path = os.path.join(output_folder, csv_file)
        # 将数据写入 csv 文件
        df.to_csv(csv_path, index=False)
        trans_count += 1
        print(f"Converted {xls_file} to {csv_file}")
    print(f'{trans_count} is 2csv')


def read_csv_folder(folder_path):
    # 用于存储所有 CSV 文件数据的列表
    all_path = []
    # 遍历文件夹及其子文件夹中的所有文件
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                # 拼接文件的完整路径
                file_path = os.path.join(root, file)
                # 使用 Pandas 读取 CSV 文件并添加到列表中
                all_path.append(file_path)
    return all_path


def main():
    folder_path = "../data/country"
    all_path = read_csv_folder(folder_path)
    print(all_path)
    # xls2csv("data/全部论文17-18")


if __name__ == '__main__':
    main()
