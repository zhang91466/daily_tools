# -*- coding: UTF-8 -*-
"""
@time:2022/6/13
@author:simonzhang
@file:geo_data_load
"""
from sqlalchemy import create_engine
import os
import pandas as pd
import geopandas as gpd

from sqlalchemy import types
from datetime import datetime
from tqdm import tqdm

DATA_FILE_PATH = r"C:\Users\zhang\Desktop\点线表数据"
SRID_VALUE = 4326
TABLE_STAG_NAME = "stag"
GEOMETRY_COLUMN_NAME = "geometry"

POINT_FILE_NAME = "20220506数据迁移点数据%d.csv"
POINT_FILE_NUMBER = 6
POINT_TABLE_NAME = "SP_ORIGIN"
POINT_STAG_TABLE_NAME = "%s_%s" % (POINT_TABLE_NAME, TABLE_STAG_NAME)
POINT_COLUMN_MAP = {"测量点号": "id",
                    "横坐标": "x",
                    "纵坐标": "y",
                    "地面高程": "z",
                    "埋深": "d",
                    "口径": "caliber",
                    "状态": "status",
                    "类型": "d_type",
                    "工程编号": "project_id"}
POINT_COLUMN_INFO = {"id": types.Integer(),
                    "x": types.DECIMAL(38, 8),
                    "y": types.DECIMAL(38, 8),
                    "z": types.DECIMAL(38, 8),
                    "d": types.DECIMAL(38, 8),
                    "caliber": types.Integer(),
                    "status": types.String(50),
                    "d_type": types.String(100),
                    "project_id": types.String(100),
                    "geometry": types.String(8000)}

TABLE_DETAILED_LIST = {"point": {"name": POINT_TABLE_NAME,
                                 "stag": POINT_STAG_TABLE_NAME,
                                 "column_info": POINT_COLUMN_INFO,
                                 "column_map": POINT_COLUMN_MAP}}


def drop_table_if_exists(table_name_list, engine):
    """

    :param table_name_list: list
    :param conn:
    :return:
    """

    sql_stet = "DROP TABLE %s"

    with engine.connect().execution_options(autocommit=True) as conn:
        for t in table_name_list:
            try:
                print("Drop table if exists %s" % t)
                conn.execute(sql_stet % t)
            except Exception as e:
                pass


def change_str_to_geo(which_table, engine):
    sql_stet = """
    select 
    %(column_list)s, 
    geometry::STGeomFromText(%(geo_column)s,%(srid)s) as %(geo_column)s 
    into %(table_name)s 
    from %(stag)s"""

    table_info = TABLE_DETAILED_LIST[which_table]

    column_list = list(table_info["column_info"].keys())
    column_list.remove(GEOMETRY_COLUMN_NAME)

    sql_stet = sql_stet % {"column_list": ','.join(column_list),
                           "geo_column": GEOMETRY_COLUMN_NAME,
                           "srid": SRID_VALUE,
                           "table_name": table_info["name"],
                           "stag": table_info["stag"]}

    with engine.connect().execution_options(autocommit=True) as conn:
        conn.execute(sql_stet)

def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def insert_to_mssql(df, engine, table_name):
    chunk_size = int(len(df) * 0.1)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunk_size)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(table_name,
                       engine,
                       if_exists=replace,
                       index=False,
                       dtype=POINT_COLUMN_INFO)
            pbar.update(chunk_size)


start_time = datetime.now()

pd.set_option("max_rows", None)

point_df = gpd.GeoDataFrame()

print("Loads point data")

for i in range(1, POINT_FILE_NUMBER):
    new_gdf = gpd.read_file(os.path.join(DATA_FILE_PATH, POINT_FILE_NAME) % i)
    print("Show new point cnt: ", len(new_gdf.index))
    point_df = point_df.append(new_gdf)
    print("Show total point cnt: ", len(point_df.index))

point_df = point_df.rename(columns=POINT_COLUMN_MAP, errors="raise")

point_df["geometry"] = "POINT (" + point_df["x"] + " " + point_df["y"] + ")"

print("Connect Mssql")
# mssql_engine = create_engine("mssql+pymssql://sa:m?~9nfhqZR%TXzY@192.168.1.31:2433/Lm_TestXS")
mssql_engine = create_engine("mssql+pyodbc://sa:m?~9nfhqZR%TXzY@mssql?driver=ODBC+Driver+17+for+SQL+Server",
                             fast_executemany=True)

drop_table_if_exists(table_name_list=[POINT_TABLE_NAME, POINT_STAG_TABLE_NAME],
                     engine=mssql_engine)

print("Input point data to table")
insert_to_mssql(df=point_df, engine=mssql_engine, table_name=POINT_STAG_TABLE_NAME)

print("Change string to geo")
change_str_to_geo(which_table="point", engine=mssql_engine)

end_time = datetime.now()

print("Start time %s, end time %s, using %s" % (start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                str((end_time - start_time).total_seconds())))
