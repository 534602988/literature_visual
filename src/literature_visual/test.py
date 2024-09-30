def extract_common_substrings(str1, str2):
    # 将两个字符串分割成单词列表
    words1 = set(str1.split())
    words2 = set(str2.split())

    # 提取相同的单词
    common_words = words1.intersection(words2)

    # 将相同的单词组合成字符串
    common_str = ' '.join(common_words)

    return common_str


# 示例字符串
string1 = "Hello world Python"
string2 = "Python is a powerful language"

# 提取相同的部分
common_part = extract_common_substrings(string1, string2)
print("相同的部分:", common_part)
