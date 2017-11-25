import argparse
import os

import pandas as pd
import pymysql
import time

import config


def get_pymysql_connection():
    return pymysql.connect(**config.DB)


def fetch_dataframe(sql_path, params=None):
    with open(sql_path, "r") as query_file:
        connection = get_pymysql_connection()
        query = query_file.read()
        if params is not None:
            for key, value in params.items():
                query = query.replace("@" + key, str(value))
    return pd.read_sql(query, con=connection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sqlpath', type=str, help='Name of query file.')
    parser.add_argument('datapath', type=str, help='Path to output data.')
    args = parser.parse_args()

    data = fetch_dataframe(args.sqlpath)
    data.to_csv(args.datapath, index=False, encoding='utf-8')
