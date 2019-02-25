import click
import pandas as pd
import numpy as np
from os import listdir
from termcolor import colored
from measuring_tools.parsers import parser
from measuring_tools.skeleton_files import cols

@click.command()
@click.option('-p', '--path_file', type=str, help='Measures file one path')
@click.option('-b', '--by_agg', type=bool, help='Totalizer by aggregations',
              default=False)
@click.option('-t', '--by_tariff', type=bool, help='Totalizer by tariff',
              default=False)
@click.option('-w', '--write_csv', type=bool, help='Create csv with results in /tmp/clmag5a_results.csv', default=False)
def totalizer(path_file, by_agg, by_tariff, write_csv):
    df_file = pd.DataFrame(data={})
    if 'CLINME' in path_file:
        for _file in listdir(path_file):
            dummy_df = parser.read_CLINME(path_file + '/' + _file)
            if df_file.empty:
                df_file = dummy_df.copy()
            else:
                df_file = df_file.append(dummy_df)
    if 'CLMAG_' in path_file:
        df_file = parser.read_CLMAG(path_file)
    else:
        df_file = parser.read_CLMAG5A(path_file)
    if by_agg:
        res = df_file.groupby(
            cols.AGGREGATION_COLS
        ).aggregate({'measure': 'sum'}).reset_index()
        print(colored(res.to_string(index=False), 'green'))
    elif by_tariff:
        res = df_file.groupby(
            cols.AGGREGATION_TARIFF_COLS
        ).aggregate({'measure': 'sum'}).reset_index()
        res = res.replace('21', '2.1')
        res = res.replace('2A', '2.0')
        res = res.replace('30', '3.0')
        res = res.replace('31', '3.1')
        res['agree_tarifa'] = np.where(res['agree_dh'] == 'E2', res['agree_tarifa'] + ' DHA', res['agree_tarifa'] + ' A')
        res.drop(['agree_dh'], axis=1, inplace=True, errors='ignore')
        print(colored(res.to_string(index=False), 'green'))
    else:
        res = sum(list(df_file['measure']))
        print(colored(res, 'green'))
    if write_csv:
        res.to_csv('/tmp/clmag5a_results.csv', sep=';', index=None)


if __name__ == '__main__':
    totalizer()
