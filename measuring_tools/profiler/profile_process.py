from erppeek import Client

# Setup conn
host = ''
db = ''
user = '' 
c = Client(host, db, user)

# Data (setup dates or cups_ids)
di = '2017-01-01'
df = '2018-12-31'
cups_ids =  [459, 862, 917, 1145]
factura_obj = c.model('giscedata.facturacio.factura')


# All invoices by dates
"""factura_ids = factura_obj.search([
    ('data_inici', '<=', df), ('data_final', '>=', di), 
    ('type', '=like', 'in%'), ('tipo_rectificadora', 'in', ['N', 'R', 'RA'])
], 0, 0, 'origin_date_invoice asc')"""

# All invoices by CUPS
factura_ids = factura_obj.search([
    ('type', '=like', 'in%'), ('tipo_rectificadora', 'in', ['N', 'R', 'RA']), ('cups_id', 'in', cups_ids),
], 0, 0, 'origin_date_invoice asc')

print('# Factures: {}'.format(len(factura_ids)))
print('From {} to {}'.format(di, df))

import logging

def setup_logger():
    logger = logging.getLogger('PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    user_path = r'/tmp/logs_perfilacio.log'
    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def setup_logger_fail():
    logger = logging.getLogger('NO PROFILE')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    user_path = r'/tmp/logs_perfilacio_no_perfilats.log'
    hdlr = logging.FileHandler(user_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

logger = setup_logger()
logger_fail = setup_logger_fail()
logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
for factura_id in factura_ids:
    if factura_obj.check_profilable_comer(factura_id):
        factura_obj.encua_perfilacio([factura_id])
        logger.info('# Profiled factura_id: {}'.format(factura_id))
    else:
        logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))

