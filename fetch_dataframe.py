import argparse
import os

import pandas as pd
import sqlalchemy
import time

import config


def get_db_connection(name):
    return sqlalchemy.create_engine(
        '{dialect}://{username}:{password}@{host}:{port}/{database}'.format(**config.DB[name])
    )


def fetch_dataframe(sql_path, config_name, params=None):
    with open(sql_path, "r") as query_file:
        connection = get_db_connection(config_name)
        query = query_file.read()
        if params:
            for key, value in params.items():
                query = query.replace("@" + key, str(value))
    return pd.read_sql(query, con=connection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sqlpath', type=str, help='Name of query file.')
    parser.add_argument('datapath', type=str, help='Path to output data.')
    parser.add_argument('--config', default='mendieta', type=str,
                        help='Which config to use.')
    parser.add_argument('--params', nargs='*', type=str,
                        help='Query parameters as keyword:argument pairs')
    parser.add_argument('--print', action='store_true', help='Print a preview.')
    args = parser.parse_args()

    params = {}
    if args.params:
        for param in args.params:
            kw, arg = param.split(':')
            params[kw] = arg

    data = fetch_dataframe(args.sqlpath, args.config, params=params)

    if args.print:
        print(data.head())

    data.to_csv(args.datapath, index=False, encoding='utf-8')
