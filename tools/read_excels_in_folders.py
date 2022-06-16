# -*- coding: UTF-8 -*-
"""
@time:2022/5/23
@author:simonzhang
@file:read_excels_in_folders
"""
from utils.folder_info import file_name_walk, os
from utils.files_operation import GetExcelInfo

file_list = file_name_walk(file_dir=r"D:\办公\项目备份\临安排水\数据资产\报表台账类")

for file_path in file_list:
    print(file_path)
    xls = GetExcelInfo.read_excel(file_path=file_path)
    if xls is not None:
        xls_sheet_names = GetExcelInfo.get_sheet_name(excel_object=xls)
        for sheet_name in xls_sheet_names:
            print("  --  %s" % sheet_name)
