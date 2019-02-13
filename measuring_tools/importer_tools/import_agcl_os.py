import click
import pandas as pd
import numpy as np
from termcolor import colored
from measuring_tools.skeleton_files import cols

@click.command()
@click.option('-p', '--path_file', type=str, help='AGCL from OS file one path')
@click.option('-l', '--batch_name', type=str, help='Batch name ex: 2019/01')
def import_agcl_os(path_file, batch_name):
    df = pd.read_csv(path_file, sep=';', names=cols.AGCL_OS, dtype={'distribuidora': str, 'comercialitzadora': str, 'agree_tensio': str, 'agree_tarifa': str, 'agree_tipo': str})
    df['agree_tipo'] = df['agree_tipo'].map(lambda x: x.zfill(2))
    df['magnitud'] = 'AE'
    df['consum'] = 0
    df['data_inici_ag'] = df['data_inici_ag'].map(lambda x: x[:10].replace('/', '-'))
    df['data_final_ag'] = df['data_final_ag'].map(lambda x: x[:10].replace('/', '-'))
    df['data_final_ag'] = np.where(df['data_final_ag'] == '3000-01-01', False, df['data_final_ag'])

    batch_id = False
    if batch_id:
        df['lot_id'] = _batch_id
        data = df.T.to_dict().values()
        for aggregation in data:
            aggr_obj.create(aggregation)
    else:
        print df.T.to_dict().values()

if __name__ == '__main__':
    import_agcl_os()
