import pandas as pd
from measuring_tools.skeleton_files import cols


def read_CLMAG5A(path):
    df_clmag5a = pd.read_csv(
        path, sep=';', names=cols.CLMAG5A_COLS,
        dtype={
            'distribuidora': 'str',
            'comercialitzadora': 'str',
            'agree_tipo': 'str',
        }
    )

    return df_clmag5a


def read_CLINME(path):
    df_clinme = pd.read_csv(
        path, sep=';', names=cols.CLINME_COLS,
        dtype={
            'distribuidora': 'str',
            'comercialitzadora': 'str',
            'agree_tipo': 'str',
        }
    )

    return df_clinme
