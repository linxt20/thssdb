import os

def generate_tree(dir_path, level=0):
    """
    生成指定文件夹的层次结构树
    """
    # 获取当前文件夹中的所有文件和子文件夹
    entries = os.listdir(dir_path)

    for entry in entries:
        # 输出当前层级的缩进和文件/文件夹名称
        print("|   " * level + "|-- " + entry)

        # 如果当前entry是文件夹，则递归调用generate_tree函数
        if os.path.isdir(os.path.join(dir_path, entry)):
            generate_tree(os.path.join(dir_path, entry), level + 1)


# 指定文件夹路径
folder_path = "./src"

# 调用generate_tree函数
generate_tree(folder_path)