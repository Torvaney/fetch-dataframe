import argparse
import os

import pandas as pd
import pymysql
import time

import config


def get_pymysql_connection():
    return pymysql.connect(config.DB)


def get_connection():
    connection = peewee.MySQLDatabase(config.DB)
    connection.connect()
    return connection


def fetch_dataframe(query_file, params=None):
    sql_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sql')
    with open(sql_path + query_file, "r") as query_file:
        connection = get_pymysql_connection()
        query = query_file.read()
        if params is not None:
            for key, value in params.items():
                query = query.replace("@" + key, str(value))
    return pd.read_sql(query, con=connection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sql-file', type=str, help='Name of query file.')
    parser.add_argument('data-file', type=str, help='Path to output data.')
    args = parser.parge_args()

    data = fetch_dataframe(args.sql_file)
    data.to_csv(args.data_file)
