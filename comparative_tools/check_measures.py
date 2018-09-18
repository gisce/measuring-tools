import click
import os
import pandas as pd
import numpy as np

@click.command()
@click.option('--path_clinmes', type=str, help='CLINMES path')
@click.option('--path_clmag5a', type=str, help='CLMAG5A path')
@click.option('--write_csv', type=bool, help='Write csv with results')
@click.option('--csv_path', type=str,
              help='csv result path, if it is not indicated, it is saved in: '
                   '/tmp/diffs.csv')
def check_measures(path_clinmes, path_clmag, path_clmag5a, write_csv=False,
                   csv_path=False):
    # clinme
    aggregation_cols = ['distribuidora', 'comercialitzadora', 'agree_tensio',
                        'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia']
    dfs = []
    for i, _file in enumerate(os.listdir(path_clinmes)):
        filename = path_clinmes + _file
        header = ['cups', 'distribuidora', 'comercialitzadora', 'agree_tensio',
                  'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia',
                  'indicador_mesura', 'timestamp', 'timestamp_end', 'measure',
                  'r1', 'r4', 'estimada', 'n_estimada', 'res']
        df = pd.read_csv(filename, sep=';', names=header,
                         dtype={'distribuidora': 'str',
                                'comercialitzadora': 'str',
                                'agree_tipo': 'str'})
        df.drop('res', axis=1, inplace=True)
        dfs.append(df)
    df_clinme = pd.concat(dfs)
    df_clinme['npoints_clinme'] = 0
    df_clinme = df_clinme.groupby(aggregation_cols).aggregate(
        {'measure': 'sum', 'npoints_clinme': 'count'}).reset_index()

    # clmag
    header = ['distribuidora', 'comercialitzadora', 'agree_tensio',
              'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia',
              'timestamp', 'estacio', 'magnitud', 'measure', 'npoints_clmag5a',
              'm_tg', 'n_tg', 'm_notg', 'n_notg', 'res']
    df_clmag5a = pd.read_csv(path_clmag5a, sep=';', names=header,
                             dtype={'distribuidora': 'str',
                                    'comercialitzadora': 'str',
                                    'agree_tipo': 'str'})

    # Concat, groupby and aggregation
    df_clmag5a = df_clmag5a.groupby(aggregation_cols).aggregate(
        {'measure': 'sum', 'npoints_clmag5a': 'max'}).reset_index()
    df_clmag5a = df_clmag5a.rename(columns={'measure': 'consumption_clmag5a'})
    df_clinme = df_clinme.rename(columns={'measure': 'consumption_clinme'})

    # Remove types 04 and 03
    df_clinme = df_clinme[df_clinme.agree_tipo != '03']
    df_clinme = df_clinme[df_clinme.agree_tipo != '04']

    df_cuadre_aggs = pd.merge(df_clmag5a, df_clinme)

    condition = (df_cuadre_aggs['consumption_clinme'] != df_cuadre_aggs[
        'consumption_clmag5a'])
    # condition_2 = ((df_cuadre_aggs['consumption_clinme'] - df_cuadre_aggs['consumption_clmag5a']) < -1)

    df_cuadre_aggs['cuadrado'] = np.where(condition, 'No', 'Si')
    first_df = df_cuadre_aggs.loc[
        df_cuadre_aggs['cuadrado'] == 'No'].sort_values(
        'comercialitzadora').sort_values(['comercialitzadora'])

    # df_cuadre_aggs['cuadrado'] = np.where(condition_2, 'No', 'Si')
    # second_df = df_cuadre_aggs.loc[df_cuadre_aggs['cuadrado'] == 'No'].sort_values('comercialitzadora').sort_values(['comercialitzadora'])

    # df_cuadre_aggs['diferencia'] = df_cuadre_aggs[df_cuadre_aggs['consumption_clinme'] - df_cuadre_aggs['consumption_clmag5a']]

    if write_csv:
        if not csv_path:
            csv_path = '/tmp/diff.csv'
        aggregation_cols.append('consumption_clinme')
        aggregation_cols.append('consumption_clmag5a')
        aggregation_cols.append('npoints_clinme')
        aggregation_cols.append('npoints_clmag5a')
        aggregation_cols.append('cuadrado')
        df_cuadre_aggs.to_csv(csv_path, sep=';', index=False,
                              line_terminator='\n', columns=aggregation_cols)

    return df_cuadre_aggs


if __name__ == '__main__':
    check_measures()
