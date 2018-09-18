import click
import os
import pandas as pd
import numpy as np

@click.command()
@click.option('--path_clinmes_one', type=str,
              help='First path source with CLINMES')
@click.option('--path_clinmes_two', type=str,
              help='Second path source with CLINMES')
@click.option('--path_dest', type=str, help='Destination path CLINMES MERGED')
def merge_clinmes(path_clinmes_one, path_clinmes_two, path_dest):
    """ CLINMES_merge_toooool """

    header = [
        'cups', 'distribuidora', 'comercialitzadora', 'agree_tensio',
        'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia',
        'indicador_mesura', 'timestamp', 'timestamp_end', 'measure', 'r1', 'r4',
        'estimada', 'n_estimada', 'res'
    ]
    aggregation_cols = [
        'cups', 'distribuidora', 'comercialitzadora', 'agree_tensio',
        'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia',
        'indicador_mesura'
    ]

    for i, _file in enumerate(os.listdir(path_clinmes_one)):
        # Search our CLINME
        for _file_seda in os.listdir(path_clinmes_two):
            test_check = _file_seda[:-10]
            if test_check in _file:
                filename_seda = path_clinmes_two + _file_seda
                filename_erp = path_clinmes_one + _file
                df_seda = pd.read_csv(filename_seda, sep=';', names=header, dtype={'distribuidora': 'str', 'comercialitzadora': 'str', 'agree_tipo': 'str'})
                df_erp = pd.read_csv(filename_erp, sep=';', names=header, dtype={'distribuidora': 'str', 'comercialitzadora': 'str', 'agree_tipo': 'str'})
                df_seda.drop('res', axis=1, inplace=True)
                df_erp.drop('res', axis=1, inplace=True)
                df_clinme = pd.concat([df_seda, df_erp]).groupby(aggregation_cols).aggregate({'measure': 'sum', 'r1': 'sum', 'timestamp': 'min', 'timestamp_end': 'max'}).reset_index()
                for label in ('r4', 'estimada', 'n_estimada'):
                    df_clinme[label] = 0

                # Write CLINME
                filename_dest = path_dest + _file
                print('{0} vs {1} -> {2}'.format(_file_seda, _file, filename_dest))
                df_clinme.to_csv(
                    filename_dest, sep=';', header=False, columns=header, index=False,
                    line_terminator='\n'
                )


if __name__ == '__main__':
    merge_clinmes()
