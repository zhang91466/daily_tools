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
from shapely import wkt
from geopandas import GeoDataFrame
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import types
from datetime import datetime
from tqdm import tqdm

DATA_FILE_PATH = r"C:\Users\zhang\Desktop\点线表数据"
POINT_FILE_NAME = "20220506数据迁移点数据%d.csv"
POINT_FILE_NUMBER = 2
SRID_VALUE = 4326

from geoalchemy2.types import _GISType
from geoalchemy2.elements import WKBElement
from sqlalchemy.sql import func
from sqlalchemy import text


class GeometryMSSQL(_GISType):
    """
    From geoalchemy2 types Geography type.
    Because of from text which particular in sql server.
    In sql server from text value is select geometry::STGeomFromText('POINT (524367.0059 3338128.386)',4326)
    """

    name = "geometry"
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = "geometry::STGeomFromText"
    """ The ``FromText`` geography constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = "ST_AsBinary"
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = WKBElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """

    cache_ok = False
    """ Disable cache for this type. """

    def get_col_spec(self):
        return self.name
        # if not self.geometry_type:
        #     return self.name
        # return '%s(%s,%d)' % (self.name, self.geometry_type, self.srid)

    def bind_expression(self, bindvalue):
        """Specific bind_expression that automatically adds a conversion function"""
        return text(f"GEOMETRY::STGeomFromText(:{bindvalue.key},%d).STAsText()" % SRID_VALUE).bindparams(bindvalue)
        # return getattr(func, self.from_text)(bindvalue, type_=self)


# point_df["geometry"] = point_df["geometry"].apply(wkt.loads)
#
# point_df["geometry"] = point_df["geometry"].apply(create_wkt_element)
# def create_wkt_element(geom):
#     return WKTElement(geom.wkt)


def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def insert_to_mssql(df):
    # mssql_engine = create_engine("mssql+pymssql://sa:m?~9nfhqZR%TXzY@192.168.1.31:2433/Lm_TestXS")
    mssql_engine = create_engine("mssql+pyodbc://sa:m?~9nfhqZR%TXzY@mssql?driver=ODBC+Driver+17+for+SQL+Server",
                                 fast_executemany=True)

    chunk_size = int(len(df) * 0.0005)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunk_size)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql("point_test",
                       mssql_engine,
                       if_exists=replace,
                       index=False,
                       dtype={"id": types.Integer(),
                              "x": types.DECIMAL(38, 8),
                              "y": types.DECIMAL(38, 8),
                              "z": types.DECIMAL(38, 8),
                              "d": types.DECIMAL(38, 8),
                              "caliber": types.Integer(),
                              "status": types.String(50),
                              "d_type": types.String(100),
                              "project_id": types.String(100),
                              "geometry": GeometryMSSQL()})
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

point_df = point_df.rename(columns={"测量点号": "id",
                                    "横坐标": "x",
                                    "纵坐标": "y",
                                    "地面高程": "z",
                                    "埋深": "d",
                                    "口径": "caliber",
                                    "状态": "status",
                                    "类型": "d_type",
                                    "工程编号": "project_id"}, errors="raise")

point_df["geometry"] = "POINT (" + point_df["x"] + " " + point_df["y"] + ")"

print("Input point data to table")
insert_to_mssql(df=point_df)

end_time = datetime.now()

print("Start time %s, end time %s, using %s" % (start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                                str((end_time - start_time).total_seconds())))
