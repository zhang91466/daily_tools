# -*- coding: UTF-8 -*-
"""
@time:2022/5/23
@author:simonzhang
@file:files_operation
"""

from xlrd import open_workbook, XLRDError


class GetExcelInfo:

    @classmethod
    def read_excel(cls, file_path):
        try:
            return open_workbook(file_path, on_demand=True)
        except XLRDError as e:
            print(e)

    @classmethod
    def get_sheet_name(cls, excel_object):
        return excel_object.sheet_names()
