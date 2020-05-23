# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, get_dummies


def make_profile_dataset():
    logger = logging.getLogger(__name__)

    correspondence_path = Path(
        data_dir, 'external', 'tse-ibge-correspondence.csv')
    correspondence = read_csv(correspondence_path, usecols=[
                              'COD_TSE'], index_col=0)

    profiles_dir = Path(data_dir, 'raw', 'profiles_1994_2018').resolve()
    processed_profiles_dir = Path(data_dir, 'processed', 'profiles').resolve()
    processed_profiles_dir.mkdir(exist_ok=True)

    columns_pre_2018 = ['PERIODO', 'UF',
                        'MUNICIPIO', 'CD_MUNICIPIO', 'NR_ZONA', 'DS_GENERO', 'DS_FAIXA_ETARIA', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL']
    columns_2018 = ['DT_GERACAO', 'HH_GERACAO', 'ANO_ELEICAO', 'SG_UF', 'CD_MUNICIPIO',
                    'NM_MUNICIPIO', 'CD_MUN_SIT_BIOMETRIA', 'DS_MUN_SIT_BIOMETRIA',
                    'NR_ZONA', 'CD_GENERO', 'DS_GENERO', 'CD_ESTADO_CIVIL',
                    'DS_ESTADO_CIVIL', 'CD_FAIXA_ETARIA', 'DS_FAIXA_ETARIA',
                    'CD_GRAU_ESCOLARIDADE', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL',
                    'QT_ELEITORES_BIOMETRIA', 'QT_ELEITORES_DEFICIENCIA',
                    'QT_ELEITORES_INC_NM_SOCIAL']
    columns_to_use = ['CD_MUNICIPIO', 'DS_GENERO',
                      'DS_FAIXA_ETARIA', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL']
    metadata_by_year = {
        1994: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        1998: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        2002: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        2006: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        2010: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        2014: {
            'file_extension': 'txt',
            'columns': columns_pre_2018,
            'header': None
        },
        2018: {
            'file_extension': 'csv',
            'columns': columns_2018,
            'header': 0
        }
    }

    for year in metadata_by_year:
        logger.info('starting to create {} profile data'.format(year))
        file_path = Path(
            profiles_dir, f'perfil_eleitorado_{year}', f'perfil_eleitorado_{year}.{metadata_by_year[year]["file_extension"]}')

        dataset = read_csv(file_path,
                           encoding='latin',
                           sep=';', names=metadata_by_year[year]['columns'],
                           usecols=columns_to_use,
                           header=metadata_by_year[year]['header'],
                           na_values='INFORMAÇÃO NÃO RECUPERADA')

        object_df = dataset.select_dtypes(include='object')
        dataset.loc[:, object_df.columns] = object_df.apply(
            lambda column: column.str.strip().str.replace(' ', '_').str.upper())

        dataset = get_dummies(dataset).groupby('CD_MUNICIPIO').apply(lambda group: group.apply(lambda column: (
            column * group.QT_ELEITORES_PERFIL).sum() if column.name != 'QT_ELEITORES_PERFIL' else column.sum()))

        dataset = dataset.drop(columns='CD_MUNICIPIO')
        dataset = dataset.loc[dataset.index.intersection(correspondence.index)]
        dataset.index.name = 'CD_MUNICIPIO'
        dataset.reset_index()

        year_dir = Path(processed_profiles_dir, str(year))
        year_dir.mkdir(exist_ok=True)
        file_path = Path(year_dir, 'profile.csv')

        dataset.to_csv(file_path)
        logger.info('done creating {} profile data'.format(year))


def main():
    """ Runs data processing scripts to turn brazil voting data from (../processed) into
        profile data (saved in ../processed/profile).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating profile dataset... Saving at ../data/processed/profile')
    make_profile_dataset()
    logger.info(
        'done creating profile dataset... Saved at ../data/processed/profile')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
