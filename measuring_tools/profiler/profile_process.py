from erppeek import Client
from tqdm import tqdm
import logging
import click
from os import path

def conn(server, db, user, password):
    return Client(server, db, user, password)

def setup_logger(log_path):
    logger = logging.getLogger('PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if not log_path:
        user_path = r'/home/erp/tmp/profile/logs_perfilacio.log'
    else:
        user_path = path.join(log_path, 'logs_perfilacio.log')
    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def setup_logger_fail(log_path):
    logger = logging.getLogger('NO PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if not log_path:
        user_path = r'/home/erp/tmp/profile/logs_perfilacio_no_perfilats.log'
    else:
        user_path = path.join(log_path, 'logs_perfilacio_no_perfilats.log')

    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def profile_process(c, di=None, df=None, logger, logger_fail):
    factura_obj = c.model('giscedata.facturacio.factura')
    factura_ids = factura_obj.search([
        ('data_inici', '<=', df), ('data_final', '>=', di),
        ('type', '=like', 'in%'), ('tipo_rectificadora', 'in', ['N', 'R', 'RA']), ('state', '!=', 'draft'),
    ], 0, 0, 'origin_date_invoice asc')
    print('# Factures: {}'.format(len(factura_ids)))
    print('From {} to {}'.format(di, df))

    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_id in tqdm(factura_ids):
        try:
            if factura_obj.check_profilable_comer(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))
        except Exception as err:
            logger_fail.info('# ERROR: {}'.format(str(err)))

@click.command()
@click.option('-s', '--server', type=str, default='http://localhost:8069', help='')
@click.option('-u', '--user', type=str, default='gisce', help='ERP User (default:gisce)')
@click.option('-d', '--dbname', type=str, help='ERP Database'),
@click.option('-p', '--password', type=str, help='ERP Password to profile')
@click.option('--di', type=str, help='Init date to profile')
@click.option('--df', type=str, help='End date to profile')
@click.option('--anuladores', type=bool, default=False, help='Profile anuladores?')
@click.option('f', '--factura_mode', type=int, default=None, help='Profile one invoice specifiying invoice number or invoice id')
@click.option('c', '--cups_mode', type=str, default=None, help='Profile one CUPS')
@click.option('l', '--logs_path', type=str, default=None, help='Loggers path')
def profile(server, user, dbname, password, di, df, anuladores, factura_mode, cups_mode, logs_path):
    c = conn(server, user, dbname, password)
    logger = setup_logger(logs_path)
    logger_fail = setup_logger_fail(logs_path)
    if factura_mode:
        profile_one_invoice_process(c, factura_mode, logger, logger_fail)
    elif cups_mode:
        profile_one_cups_process(c, di, df, logger, logger_fail)
    else:
        profile_process(c, di, df, logger, logger_fail)

if __name__ == '__main__':
    profile()
