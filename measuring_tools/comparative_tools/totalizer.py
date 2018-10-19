import click
import pandas as pd
from termcolor import colored
from measuring_tools.parsers import parser
from measuring_tools.skeleton_files import cols

@click.command()
@click.option('--path_clmag5a', type=str, help='CLMAG5A one path')
@click.option('--by_agg', type=bool, help='Totalizer by aggregations',
              default=False)
def totalizer(path_clmag5a, by_agg):
    df_clmag5a = parser.read_CLMAG5A(path_clmag5a)
    if by_agg:
        res = df_clmag5a.groupby(
            cols.AGGREGATION_COLS
        ).aggregate({'measure': 'sum'}).reset_index()
        print(colored(res.to_string(index=False), 'green'))

    else:
        res = sum(list(df_clmag5a['measure']))
        print(colored(res, 'green'))


if __name__ == '__main__':
    totalizer()
