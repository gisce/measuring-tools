# -*- coding: utf-8 -*-
import click
import pandas as pd
import numpy as np
from termcolor import colored
from measuring_tools.skeleton_files import cols
from erppeek import Client
from os import path
from tqdm import tqdm

class ModuleERPError(Exception):
    pass

def c_lot(c, di, df):
    fact_obj = c.GiscedataFacturacioFactura
    liniese_obj = c.GiscedataFacturacioLecturesEnergia
    meter_obj = c.GiscedataLecturesComptador
    inv_lot_obj = c.GiscedataProfilesFacturaLot
    lot_obj = c.GiscedataProfilesLot
    year = di[:4]
    month = di[5:7]
    lot_id = lot_obj.search([('name', '=', '{}/{}'.format(month, year))])[0]
    # Preventive delete
    inv_lot_ids = inv_lot_obj.search([('lot_id', '=', lot_id)])
    inv_lot_obj.unlink(inv_lot_ids)

    factures_periode = fact_obj.search([('data_inici', '<=', df), ('data_final', '>=', di), ('tipo_rectificadora', '=', 'N')])
    for factura in tqdm(fact_obj.browse(factures_periode)):
        tech = list()
        for linia in liniese_obj.read(factura.lectures_energia_ids.id, []):
            comptador_data = meter_obj.read(linia['comptador_id'][0], ['name', 'technology_type'])
            tech.append(comptador_data['technology_type'])
        tech = list(set(tech))
        if len(tech) != 1:
            print('Diferents technologies!!! Manual resolve')
            continue
        else:
            tech = tech[0]
        if factura.polissa_id.tarifa.name == 'RE' or factura.polissa_id.tarifa.name == 'RE12':
            # 'Not RE'
            continue
        # Energy invoices
        if factura.tipo_factura != '01':
            continue
        if factura.potencia <= 450:
            # Type 5
            if factura.potencia <= 15:
                if factura.polissa_tg == '1':
                    # 'TG 5'
                    _type = 'tg'
                else:
                    _type = 'perfil'
                    # 'Perfil 5'
            else:
                if 'prime' in factura.comptadors.technology_type or 'smmweb' in factura.comptadors.technology_type:
                    # '3/4: TG'
                    _type = 'tg'
                elif 'electronic' in factura.comptadors.technology_type or 'telemeasure' in factura.comptadors.technology_type:
                    _type = 'tm'
                    # '3/4 TM'
                else:
                    _type = 'perfil'
                    # 'Perfil 3/4'
            inv_lot_obj.create({'factura_id': factura.id, 'lot_id': lot_id, 'state': 'draft', 'type': _type})
        else:
          # 'Tipo 1/2'
          continue

@click.command()
@click.option('-y', '--year', type=int, help='Integer year to create periods')
@click.option('-s', '--path_file', type=str, help='AGCL from OS file one path')
@click.option('-h', '--hostname', type=str, help='host default: localhost', default='localhost')
@click.option('-p', '--port', type=int, help='port default: 8069', default=8069)
@click.option('-d', '--dbname', type=str, help='DB Name')
@click.option('-u', '--username', type=str, help='User DB default: gisce', default='gisce')
def import_agcl_os(year, path_file, hostname, port, dbname, username):
    """This method:\n
    1- creates a measures periods by year\n
    2- import system invoices to measures_periods\n
    3- import aggregations by agcl_os
    """
    assert isinstance(year, int), 'Falta especificar Any!'
    from erppeek import Client
    # conexión
    try:
        # Due: https://www.python.org/dev/peps/pep-0476
        import ssl
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            pass
        else:
            # Handle target environment that doesn't support HTTPS verification
            ssl._create_default_https_context = _create_unverified_https_context
    except ImportError:
        pass

    # Conn
    c = Client('https://{host}:{port}'.format(host=hostname, port=port), dbname, username)
    # Crear periodes mesures
    try:
        lot_obj = c.GiscedataProfilesLot
        wiz_crear_lots_obj = c.WizardCrearLotsMesures
    except Exception as err:
        raise ModuleERPError("Moduls de mesures no instalats!!!!")
    wiz_id = wiz_crear_lots_obj.create({'year': int(year), 'state': 'init'})
    try:
        wiz = wiz_crear_lots_obj.browse(wiz_id.id)
        wiz.action_crear_lots()
    except Exception as err:
        lot_ids = lot_obj.search([('name', '=like', '%/{year}'.format(year=year))])
        assert len(lot_ids) == 12, 'Periodes incomplets!'

    filename = path.split(path_file)[1]
    period_name = filename[13:19]
    year_name = period_name[:4]
    month_name = period_name[-2:]
    period_name = '{}/{}'.format(month_name, year_name)
    df = pd.read_csv(path_file, sep=';', names=cols.AGCL_OS, dtype={'distribuidora': str, 'comercialitzadora': str, 'agree_tensio': str, 'agree_tarifa': str, 'agree_tipo': str})
    df['agree_tipo'] = df['agree_tipo'].map(lambda x: x.zfill(2))
    df['magnitud'] = 'AE'
    df['consum'] = 0
    df['data_inici_ag'] = df['data_inici_ag'].map(lambda x: x[:10].replace('/', '-'))
    df['data_final_ag'] = df['data_final_ag'].map(lambda x: x[:10].replace('/', '-'))
    df['data_final_ag'] = np.where(df['data_final_ag'] == '3000-01-01', False, df['data_final_ag'])

    batch_id = lot_obj.search([('name', '=', period_name)])
    assert batch_id != [], 'Periode de mesures {period_name} no trobat!'.format(period_name=period_name)
    df['lot_id'] = batch_id[0]
    data = df.T.to_dict().values()
    aggr_obj = c.GiscedataProfilesAggregations
    for aggregation in data:
        aggr_obj.create(aggregation)
    # Update all agregations
    batch_ids = lot_obj.search([('name', '=like', '%/{year}'.format(year=year))])
    for lot in lot_obj.read(batch_ids, ['data_inici', 'data_final']):
        c_lot(c, di=lot['data_inici'], df=lot['data_final'])
        lot_obj.update_profiling_progress([lot['id']])
        lot_obj.update_tm_tg_progress([lot['id']], context={'origin': 'tg'})
        lot_obj.update_tm_tg_progress([lot['id']], context={'origin': 'tm'})

if __name__ == '__main__':
    import_agcl_os()
