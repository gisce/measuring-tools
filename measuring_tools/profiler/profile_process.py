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


def chunks(_list, n):
    for i in range(0, len(_list), n):
        yield _list[i:i + n]


def profile_process(c, di, df, logger, logger_fail, server_type=None, anuladores=False, verbose=True):
    factura_obj = c.model('giscedata.facturacio.factura')
    search_params_base = [
        ('data_inici', '<=', df), ('data_final', '>=', di), ('state', '!=', 'draft')
    ]
    if server_type == 'comer':
        search_params_base.append(('type', '=like', 'in%'))
    elif server_type == 'distri':
        search_params_base.append(('type', '=like', 'out%'))

    search_params = search_params_base[:]
    search_params.append(('tipo_rectificadora', 'in', ['N', 'R', 'RA']))

    factura_ids = factura_obj.search(
        search_params,
        0, 0, 'origin_date_invoice asc'
    )
    print('# Factures: {}'.format(len(factura_ids)))
    print('From {} to {}'.format(di, df))
    if verbose:
        response = raw_input("Will be profiled {} invoices are you sure? ('y'/'n'):  ".format(len(factura_ids)))
        if response[0].lower() != 'y':
            return False

    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_id in tqdm(factura_ids):
        try:
            if factura_obj.check_profilable(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))
        except Exception as err:
            logger_fail.error('# ERROR: {}'.format(factura_id))
    if anuladores:
        search_params = search_params_base[:]
        search_params.append(('tipo_rectificadora', 'in', ['B', 'A', 'BRA']))

        factura_ids = factura_obj.search(
            search_params,
            0, 0, 'origin_date_invoice asc'
        )
        logger.info('#### START PROFILING ANULADORES #{} factures'.format(len(factura_ids)))
        print('# Factures: {}'.format(len(factura_ids)))
        print('From {} to {}'.format(di, df))
        for factura_id in tqdm(factura_ids):
            try:
                if factura_obj.check_profilable(factura_id):
                    factura_obj.encua_perfilacio([factura_id])
                    logger.info('# Profiled ANULADORA factura_id: {}'.format(factura_id))
                else:
                    logger_fail.info('# NO Profilable ANULADORA factura_id: {}'.format(factura_id))
            except Exception as err:
                logger_fail.error('# ERROR: {}'.format(factura_id))


def profile_process_chunks(c, di, df, logger, logger_fail, server_type=None, anuladores=False, verbose=True, n_chunks=1):
    factura_obj = c.model('giscedata.facturacio.factura')
    search_params_base = [
        ('data_inici', '<=', df), ('data_final', '>=', di), ('state', '!=', 'draft')
    ]
    if server_type == 'comer':
        search_params_base.append(('type', '=like', 'in%'))
    elif server_type == 'distri':
        search_params_base.append(('type', '=like', 'out%'))

    search_params = search_params_base[:]
    search_params.append(('tipo_rectificadora', 'in', ['N', 'R', 'RA']))

    factura_ids = factura_obj.search(
        search_params,
        0, 0, 'origin_date_invoice asc'
    )
    print('# Factures: {}'.format(len(factura_ids)))
    print('From {} to {}'.format(di, df))
    if verbose:
        response = raw_input(
            "Will be profiled {} invoices with {} chunks, are you sure? ('y'/'n'):  ".format(
                len(factura_ids), n_chunks)
        )
        if response[0].lower() != 'y':
            return False

    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_ids_chunks in tqdm(chunks(factura_ids, n_chunks)):
        try:
            factura_obj.encua_perfilacio(factura_ids_chunks)
            logger.info('# Profiled factures: {}'.format(factura_ids_chunks))
        except Exception as err:
            logger_fail.error('# ERROR: {}'.format(factura_ids_chunks))

    if anuladores:
        search_params = search_params_base[:]
        search_params.append(('tipo_rectificadora', 'in', ['B', 'A', 'BRA']))

        factura_ids = factura_obj.search(
            search_params,
            0, 0, 'origin_date_invoice asc'
        )
        logger.info('#### START PROFILING ANULADORES #{} factures'.format(len(factura_ids)))
        print('# Factures: {}'.format(len(factura_ids)))
        print('From {} to {}'.format(di, df))
        for factura_id in tqdm(factura_ids):
            try:
                if factura_obj.check_profilable(factura_id):
                    factura_obj.encua_perfilacio([factura_id])
                    logger.info('# Profiled ANULADORA factura_id: {}'.format(factura_id))
                else:
                    logger_fail.info('# NO Profilable ANULADORA factura_id: {}'.format(factura_id))
            except Exception as err:
                logger_fail.error('# ERROR: {}'.format(factura_id))


def profile_one_invoice_process(c, factura, logger, logger_fail):
    factura_obj = c.model('giscedata.facturacio.factura')
    factura_ids = factura_obj.search([('number', '=', factura)])
    if not factura_ids:
        factura_ids = [int(factura)]

    print('# Profiling Factura: {}'.format(factura_ids))
    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_id in tqdm(factura_ids):
        try:
            if factura_obj.check_profilable(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))
        except Exception as err:
            logger_fail.error('# ERROR: {}'.format(factura_id))


def profile_one_cups_process(c, cups, di, df, logger, logger_fail):
    factura_obj = c.model('giscedata.facturacio.factura')
    cups_obj = c.model('giscedata.cups.ps')
    cups_ids = cups_obj.search([('name', '=', cups)])
    if not cups_ids:
        cups_ids = [int(cups)]

    factura_ids = factura_obj.search([
        ('cups_id', 'in', cups_ids),
        ('data_inici', '<=', df),
        ('data_final', '>=', di),
        ('type', '=like', 'in%'),
        ('tipo_rectificadora', 'in', ['N', 'R', 'RA']),
        ('state', '!=', 'draft'),
    ], 0, 0, 'origin_date_invoice asc')
    print('# Factures: {}'.format(len(factura_ids)))
    print('From {} to {}'.format(di, df))

    print('# Profiling Factura: {}'.format(factura_ids))
    logger.info('#### START PROFILING #{} factures'.format(len(factura_ids)))
    for factura_id in tqdm(factura_ids):
        try:
            if factura_obj.check_profilable(factura_id):
                factura_obj.encua_perfilacio([factura_id])
                logger.info('# Profiled factura_id: {}'.format(factura_id))
            else:
                logger_fail.info('# NO Profilable factura_id: {}'.format(factura_id))
        except Exception as err:
            logger_fail.error('# ERROR: {}'.format(factura_id))


@click.command()
@click.option('-s', '--server', type=str, default='http://localhost:8069', help='ERP Server (default:localhost)')
@click.option('-u', '--user', type=str, default='gisce', help='ERP User (default:gisce)')
@click.option('-d', '--dbname', type=str, help='ERP Database')
@click.option('-p', '--password', type=str, help='ERP Password to profile')
@click.option('--di', type=str, help='Init date to profile')
@click.option('--df', type=str, help='End date to profile')
@click.option('--server_type', type=str, help='Server type: distri or comer')
@click.option('--anuladores', type=bool, default=False, help='Profile anuladores? (default:False)')
@click.option('-f', '--factura_mode', type=int, default=None, help='Profile one invoice, invoice_number or invoice_id')
@click.option('-c', '--cups_mode', type=str, default=None, help='Profile one CUPS, cups_number or cups_id')
@click.option('-l', '--logs_path', type=str, default=None, help='Loggers path')
@click.option('-v', '--verbose', type=bool, default=True, help='Verbose mode (default:True)')
@click.option('--chunks', type=int, default=0, help='Queue profiles with n chunks (default:0)')
def profile(server, user, dbname, password, di, df, server_type, anuladores, factura_mode, cups_mode, logs_path,
            verbose, chunks):
    c = conn(server, dbname, user, password)
    if c:
        print('Connected to {}'.format(c))
    logger = setup_logger(logs_path)
    logger_fail = setup_logger_fail(logs_path)
    assert server_type == 'comer' or server_type == 'distri'
    if factura_mode:
        profile_one_invoice_process(c, factura_mode, logger, logger_fail)
        return True
    elif cups_mode:
        profile_one_cups_process(c, cups_mode, di, df, logger, logger_fail)
        return True
    else:
        if chunks:
            profile_process_chunks(c, di, df, logger, logger_fail, server_type, anuladores, verbose, chunks)
        else:
            profile_process(c, di, df, logger, logger_fail, server_type, anuladores, verbose)


if __name__ == '__main__':
    profile()
