import pandas as pd
from measuring_tools.skeleton_files import cols


def read_CLMAG5A(path):
    df_clmag5a = pd.read_csv(
        path, sep=';', names=cols.CLMAG5A_COLS,
        dtype={
            'distribuidora': 'str',
            'comercialitzadora': 'str',
            'agree_tipo': 'str',
            'agree_tarifa': 'str',
        }
    )

    return df_clmag5a


def read_CLINME(path):
    df_clinme = pd.read_csv(
        open(path, 'rU'), sep=';', names=cols.CLINME_COLS,
        engine='c',
        dtype={
            'distribuidora': 'str',
            'comercialitzadora': 'str',
            'agree_tipo': 'str',
            'agree_tarifa': 'str',
        }
    )

    return df_clinme

def read_CLMAG(path):
    #CLMAG consumptions positions
    positions = [x for x in xrange(11, 107, 4)]
    R1_code = ';R1;'
    consumptions_clmag = []

    lines = []
    with open(path, 'r') as filename:
        for line in filename:
            lines.append(str(line))
    clmag_aggs = [agg[11:35] for agg in lines]
    df_aggs = pd.DataFrame(data=clmag_aggs).drop_duplicates()
    clmag_aggs = [x[0] for x in df_aggs.values.tolist()]
    aggregation_dict = dict.fromkeys(cols.CLMAG_COLS)
    for agg in clmag_aggs:
        consumption_iter = 0
        consumption_clmag = 0
        for x in lines:
            if agg in x and R1_code not in x:
                consumption_tmp = x.split(';')
                for pos in positions:
                    try:
                        consumption_iter += int(consumption_tmp[pos])
                    except:
                        break
                consumption_clmag += consumption_iter
                consumption_iter = 0
        aux = dict(zip(cols.CLMAG_COLS, agg.split(';')))
        aux['measure'] = consumption_clmag
        consumptions_clmag.append(aux)
    return pd.DataFrame(data=consumptions_clmag)

def read_MAGRE(path):
    df_magre = pd.read_csv(path, sep=';', header=None)
    df_magre[4] = df_magre[4].str.strip(' ')
    return df_magre
