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
import numpy as np

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

LINE_FILE_NAME = "20220506数据迁移线数据%d.csv"
LINE_FILE_NUMBER = 6
LINE_TABLE_NAME = "SL_ORIGIN"
LINE_STAG_TABLE_NAME = "%s_%s" % (LINE_TABLE_NAME, TABLE_STAG_NAME)
LINE_COLUMN_MAP = {"测量点号": "start_id",
                   "上接点号": "end_id",
                   "类型": "d_type",
                   "工程编号": "project_id",
                   "原GISNO": "old_gis_no"}
LINE_COLUMN_INFO = {"start_id": types.Integer(),
                    "end_id": types.Integer(),
                    "d_type": types.String(100),
                    "project_id": types.String(100),
                    "old_gis_no": types.Integer(),
                    "start_point": types.String(500),
                    "end_point": types.String(500),
                    "geometry": types.String(8000)}

WATER_METER_FILE_NAME = "20220506数据迁移水表数据%d.csv"
WATER_METER_FILE_NUMBER = 3
WATER_METER_TABLE_NAME = "SP_WATER_METER"
WATER_METER_STAG_TABLE_NAME = "%s_%s" % (WATER_METER_TABLE_NAME, TABLE_STAG_NAME)
WATER_METER_COLUMN_MAP = {"水表箱编号": "water_meter_id",
                          "类型": "d_type",
                          "工程编号": "project_id",
                          "原GISNO": "old_gis_no"}
WATER_METER_COLUMN_INFO = {"water_meter_id": types.Integer(),
                           "d_type": types.String(100),
                           "project_id": types.String(100),
                           "old_gis_no": types.Integer(),
                           "geometry": types.String(8000)}

TABLE_DETAILED_LIST = {"point": {"name": POINT_TABLE_NAME,
                                 "stag": POINT_STAG_TABLE_NAME,
                                 "column_info": POINT_COLUMN_INFO,
                                 "column_map": POINT_COLUMN_MAP},
                       "line": {"name": LINE_TABLE_NAME,
                                "stag": LINE_STAG_TABLE_NAME,
                                "column_info": LINE_COLUMN_INFO,
                                "column_map": LINE_COLUMN_MAP},
                       "water_meter": {"name": WATER_METER_TABLE_NAME,
                                       "stag": WATER_METER_STAG_TABLE_NAME,
                                       "column_info": WATER_METER_COLUMN_INFO,
                                       "column_map": WATER_METER_COLUMN_MAP},
                       }


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


def insert_to_mssql(df, engine, table_name, data_type):
    chunk_size = int(len(df) * 0.05)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunk_size)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(table_name,
                       engine,
                       if_exists=replace,
                       index=False,
                       dtype=data_type)
            pbar.update(chunk_size)


print("##################################################")
print("############## GEO DATA LOAD START ###############")
print("##################################################")
start_time = datetime.now()

pd.set_option("max_rows", None)

print("##################################################")
print("############## Loads point data ##################")
print("##################################################")

point_df = gpd.GeoDataFrame()

for i in range(1, POINT_FILE_NUMBER):
    new_gdf = gpd.read_file(os.path.join(DATA_FILE_PATH, POINT_FILE_NAME) % i)
    print("Show new point cnt: ", len(new_gdf.index))
    point_df = point_df.append(new_gdf)
    print("Show total point cnt: ", len(point_df.index))

point_df = point_df.rename(columns=POINT_COLUMN_MAP, errors="raise")

point_df["geometry"] = "POINT (" + point_df["x"] + " " + point_df["y"] + ")"

print("##################################################")
print("############### Loads line data ##################")
print("##################################################")

line_df = gpd.GeoDataFrame()

for i in range(1, LINE_FILE_NUMBER):
    new_gdf = gpd.read_file(os.path.join(DATA_FILE_PATH, LINE_FILE_NAME) % i)
    print("Show new line cnt: ", len(new_gdf.index))
    line_df = line_df.append(new_gdf)
    print("Show total line cnt: ", len(line_df.index))

line_df = line_df.rename(columns=LINE_COLUMN_MAP, errors="raise")

# Merge start id
line_merge_df = line_df.merge(point_df, how='left', left_on="start_id", right_on="id")

line_df["start_point"] = line_merge_df["x"] + " " + line_merge_df["y"]

# Merge end id
line_merge_df = line_df.merge(point_df, how='left', left_on="end_id", right_on="id")

line_df["end_point"] = line_merge_df["x"] + " " + line_merge_df["y"]

line_df["geometry"] = np.where(((line_df["start_point"] == line_df["end_point"])
                                | (line_df["start_point"].str.len() == 1)
                                | (line_df["end_point"].str.len() == 1)),
                               np.nan,
                               "LINESTRING (" + line_df["start_point"] + " 0, " + line_df["end_point"] + " 0)")

print("##################################################")
print("########### Loads water meter data ###############")
print("##################################################")

water_meter_df = gpd.GeoDataFrame()

for i in range(1, WATER_METER_FILE_NUMBER):
    new_gdf = gpd.read_file(os.path.join(DATA_FILE_PATH, WATER_METER_FILE_NAME) % i)
    print("Show new water meter cnt: ", len(new_gdf.index))
    water_meter_df = water_meter_df.append(new_gdf)
    print("Show total water meter cnt: ", len(water_meter_df.index))

water_meter_df = water_meter_df.rename(columns=WATER_METER_COLUMN_MAP, errors="raise")

water_meter_df_merged = water_meter_df.merge(point_df, how='left', left_on="water_meter_id", right_on="id")

water_meter_df["geometry"] = water_meter_df_merged["geometry_y"]

water_meter_df["old_gis_no"] = np.where((water_meter_df["old_gis_no"] == "NULL"), 0, water_meter_df["old_gis_no"])

print("##################################################")
print("################ Connect Mssql ###################")
print("##################################################")

mssql_engine = create_engine("mssql+pyodbc://sa:m?~9nfhqZR%TXzY@mssql?driver=ODBC+Driver+17+for+SQL+Server",
                             fast_executemany=True)

drop_table_if_exists(table_name_list=[POINT_TABLE_NAME,
                                      POINT_STAG_TABLE_NAME,
                                      LINE_TABLE_NAME,
                                      LINE_STAG_TABLE_NAME,
                                      WATER_METER_TABLE_NAME,
                                      WATER_METER_STAG_TABLE_NAME
                                      ],
                     engine=mssql_engine)

print("Input point data to table")
insert_to_mssql(df=point_df,
                engine=mssql_engine,
                table_name=POINT_STAG_TABLE_NAME,
                data_type=POINT_COLUMN_INFO)

print("Input line data to table")
insert_to_mssql(df=line_df,
                engine=mssql_engine,
                table_name=LINE_STAG_TABLE_NAME,
                data_type=LINE_COLUMN_INFO)

print("Input line data to table")
insert_to_mssql(df=water_meter_df,
                engine=mssql_engine,
                table_name=WATER_METER_STAG_TABLE_NAME,
                data_type=WATER_METER_COLUMN_INFO)

print("Change string to geo")
change_str_to_geo(which_table="point", engine=mssql_engine)
change_str_to_geo(which_table="line", engine=mssql_engine)
change_str_to_geo(which_table="water_meter", engine=mssql_engine)

end_time = datetime.now()

print("Start time %s, end time %s, using %s" % (start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                str((end_time - start_time).total_seconds())))
