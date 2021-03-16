# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import DataFrame
from pandas import read_csv, concat


def create_br_dataset(year, metadata):
    br = DataFrame()

    for state in metadata['states']:
        file_path = Path(
            data_dir,
            'raw',
            'elections_1994_2018',
            'votacao_partido_munzona_{}'.format(year),
            'votacao_partido_munzona_{}_{}.{}'.format(
                year, state, metadata['file_extension'])
        ).resolve()
        uf = read_csv(file_path, sep=';', encoding='latin',
                      header=metadata['header'])
        br = concat([br, uf])

    br.columns = metadata['columns']
    br.sort_values(by=['sigla_uf', 'nome_mun'], inplace=True)
    return br


def make_br_dataset():
    logger = logging.getLogger(__name__)

    pre_2012_columns = ['data_geracao', 'hora_geracao', 'ano_eleicao', 'num_turno', 'descricao_eleicao', 'sigla_uf', 'sigla_ue', 'cod_mun', 'nome_mun', 'num_zona', 'cod_cargo', 'desc_cargo',
                        'tipo_legenda', 'nome_coligacao', 'composicao_legenda', 'sigla_partido', 'num_partido', 'nome_partido', 'qtde_votos_nominais', 'qtde_votos_legenda', 'sequencial_coligacao']
    post_2012_columns = ['data_geracao', 'hora_geracao', 'ano_eleicao', 'cod_tipo_eleicao', 'nome_eleicao', 'num_turno', 'cod_eleicao', 'descricao_eleicao', 'data_eleicao', 'tipo_abrangencia', 'sigla_uf', 'sigla_ue', 'nome_ue', 'cod_mun',
                         'nome_mun', 'num_zona', 'cod_cargo', 'desc_cargo', 'tipo_abrangencia', 'num_partido', 'sigla_partido', 'nome_partido', 'seq_coligacao', 'nome_coligacao', 'desc_comp_colig', 'voto_transito', 'qtde_votos_nominais', 'qtde_votos_legenda']

    metadata_by_year = {
        1998: {
            'states': ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'],
            'columns': pre_2012_columns,
            'file_extension': 'txt',
            'header': None
        },
        2002: {
            'states': ['AC', 'AL', 'AM', 'AP', 'BA', 'BR', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'],
            'columns': pre_2012_columns,
            'file_extension': 'txt',
            'header': None
        },
        2006: {
            'states': ['AC', 'AL', 'AM', 'AP', 'BA', 'BR', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'],
            'columns': pre_2012_columns,
            'file_extension': 'txt',
            'header': None
        },
        2010: {
            'states': ['AL', 'AC', 'AM', 'AP', 'BA', 'BR', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'],
            'columns': pre_2012_columns,
            'file_extension': 'txt',
            'header': None
        },
        2014: {
            'states': ['BRASIL'],
            'columns': post_2012_columns,
            'file_extension': 'csv',
            'header': 0
        },
        2018: {
            'states': ['BRASIL'],
            'columns': post_2012_columns,
            'file_extension': 'csv',
            'header': 0
        },
    }

    elections_1994_2018_dir = Path(
        data_dir, 'interim', 'brazil_congressmen').resolve()
    elections_1994_2018_dir.mkdir(exist_ok=True)
    for year in metadata_by_year:
        logger.info('starting to join {} data'.format(year))
        year_dir = Path(elections_1994_2018_dir, str(year))
        year_dir.mkdir(exist_ok=True)
        destination_path = Path(
            year_dir,
            'br.csv'
        )
        br_dataset = create_br_dataset(year, metadata_by_year[year])
        br_dataset.to_csv(destination_path, index=None)
        logger.info('finished joining {} data'.format(year))


def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        joined brazil voting datasets by year (saved in ../interim/brazil).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'joining states data into Brazil data... Saving at ../data/interim/brazil')
    make_br_dataset()
    logger.info(
        'done joining states data into Brazil data... Saved at ../data/interim/brazil')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
