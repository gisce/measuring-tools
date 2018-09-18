import click
import os
import csv
import pandas as pd
import numpy as np

@click.command()
@click.option('--path_agclos', type=str, help='Path with AGCL')
def create_aggs(path_agclos):
    """ AGGS Creator """

    # Preventive delete
    _batch_id = 2

    _agg_vals = []
    with open(path_agclos, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';')
        keys = ['distribuidora', 'comercialitzadora', 'agree_tensio',
                'agree_tarifa', 'agree_dh', 'agree_tipo', 'provincia',
                'data_inici_ag', 'data_final_ag']
        for row in spamreader:
            vals = dict(zip(keys, row))
            vals['lot_id'] = _batch_id
            vals['data_inici'] = '2018-01-01'
            vals['data_final'] = '2018-01-31'
            vals['magnitud'] = 'AE'
            vals['agree_tipo'] = str(vals['agree_tipo']).zfill(2)

            if vals['data_final_ag'] == '3000/01/01 00':
                del vals['data_final_ag']
            else:
                _year, _month, _day = vals['data_final_ag'].split('/')
                _day = day.split(' ')[0]
                vals['data_final_ag'] = '{}-{}-{}'.format(_year, _month, _day)
            year, month, day = vals['data_inici_ag'].split('/')
            day = day.split(' ')[0]
            vals['data_inici_ag'] = '{}-{}-{}'.format(year, month, day)
            # print vals
            _agg_vals.append(vals)
    return _agg_vals



if __name__ == '__main__':
    create_aggs()
