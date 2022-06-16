# -*- coding: UTF-8 -*-
"""
@time:2022/5/23
@author:simonzhang
@file:folder
"""
import os


def file_name_walk(file_dir):
    flies_list = []
    for root, dirs, files in os.walk(file_dir):
        for f_name in files:
            flies_list.append(os.path.join(root, f_name))  # 当前路径下所有非目录子文件

    return flies_list