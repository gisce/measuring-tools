import click
import pandas as pd
from termcolor import colored
from measuring_tools.parsers import parser
from measuring_tools.skeleton_files import cols

@click.command()
@click.option('--path_clmag5a_one', type=str, help='CLMAG5A one path')
@click.option('--path_clmag5a_two', type=str, help='CLMAG5A two path')
def check_clmag5as(path_clmag5a_one, path_clmag5a_two):

    df_clmag5a_one = parser.read_CLMAG5A(path_clmag5a_one)
    df_clmag5a_two = parser.read_CLMAG5A(path_clmag5a_two)
    print(colored(df_clmag5a_one.to_string(index=False), 'yellow'))

    # Concat, groupby and aggregation
    df_clmag5a = pd.concat([df_clmag5a_one, df_clmag5a_two])
    df_clmag5a.drop_duplicates(keep=False, inplace=True)

    print(colored(df_clmag5a.to_string(index=False), 'yellow'))

if __name__ == '__main__':
    check_clmag5as()
