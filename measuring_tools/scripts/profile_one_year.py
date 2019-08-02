import click
from erppeek import Client
import logging
from tqdm import tqdm
from os import getcwd

def setup_logger():
    logger = logging.getLogger('PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    user_path = '{}/{}'.format(getcwd(), 'logs_perfilacio.log')
    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def setup_logger_fail():
    logger = logging.getLogger('NO PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    user_path = '{}/{}'.format(getcwd(), 'logs_perfilacio_no_perfilats.log')
    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

@click.command()
@click.option('--host', help='Database name', default='localhost')
@click.option('--port', help='Port', default=8069)
@click.option('--db', help='Database name')
@click.option('--user', help='User', default='gisce')
@click.option('--di', help='Date to start profiling YYY-MMM-DD')
@click.option('--df', help='Date to end profiling YYY-MMM-DD')
@click.option('--servertype', help='Distri or Comer (d or c)')
def profile(host, port, db, user, di, df, servertype):
    connection = ('http://{host}:{port}'.format(host=host, port=str(port)))
    c = Client(connection, db, user)
    if servertype == 'd':
        inv_type = 'out%'
    else:
        inv_type = 'in%'

    factura_obj = c.model('giscedata.facturacio.factura')
    factura_ids = factura_obj.search([
        ('data_inici', '<=', df), ('data_final', '>=', di),
        ('type', '=like', inv_type), ('tipo_rectificadora', 'in', ['N', 'R', 'RA']), ('state', '!=', 'draft')
    ], 0, 0, 'origin_date_invoice asc')

    print('# Factures: {}'.format(len(factura_ids)))
    print('From {} to {}'.format(di, df))

    logger = setup_logger()
    logger_fail = setup_logger_fail()
    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_id in tqdm(factura_ids):
        if servertype == 'd':
            if factura_obj.check_profilable_distri(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))
        if servertype == 'c':
            if factura_obj.check_profilable_comer(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))

if __name__ == '__main__':
    profile()
