# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pandas import read_csv, get_dummies, DataFrame, concat
from unicodedata import normalize


def remove_accent(text):
    return normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')


def make_single_profile_dataset():
    profiles_dir = Path(data_dir, 'processed', 'profiles').resolve()
    metadata_by_year = {
        # 1994: {
        #     'columns_to_calculate': []
        # },
        1998: {
            'columns_to_calculate': []
        },
        2002: {
            'columns_to_calculate': [
                {
                    'name': 'DS_FAIXA_ETARIA_18_A_24_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_18_A_20_ANOS', 'DS_FAIXA_ETARIA_21_A_24_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_SUPERIOR_A_70_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_70_A_79_ANOS', 'DS_FAIXA_ETARIA_SUPERIOR_A_79_ANOS']
                }
            ]
        },
        2006: {
            'columns_to_calculate': [
                {
                    'name': 'DS_FAIXA_ETARIA_18_A_24_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_18_A_20_ANOS', 'DS_FAIXA_ETARIA_21_A_24_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_SUPERIOR_A_70_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_70_A_79_ANOS', 'DS_FAIXA_ETARIA_SUPERIOR_A_79_ANOS']
                }
            ]
        },
        2010: {
            'columns_to_calculate': [
                {
                    'name': 'DS_FAIXA_ETARIA_18_A_24_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_18_A_20_ANOS', 'DS_FAIXA_ETARIA_21_A_24_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_SUPERIOR_A_70_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_70_A_79_ANOS', 'DS_FAIXA_ETARIA_SUPERIOR_A_79_ANOS']
                }
            ]
        },
        2014: {
            'columns_to_calculate': [
                {
                    'name': 'DS_FAIXA_ETARIA_18_A_24_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_18_A_20_ANOS', 'DS_FAIXA_ETARIA_21_A_24_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_SUPERIOR_A_70_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_70_A_79_ANOS', 'DS_FAIXA_ETARIA_SUPERIOR_A_79_ANOS']
                }
            ]
        },
        2018: {
            'columns_to_calculate': [
                {
                    'name': 'DS_FAIXA_ETARIA_18_A_24_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_18_ANOS', 'DS_FAIXA_ETARIA_19_ANOS', 'DS_FAIXA_ETARIA_20_ANOS', 'DS_FAIXA_ETARIA_21_A_24_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_25_A_34_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_25_A_29_ANOS', 'DS_FAIXA_ETARIA_30_A_34_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_35_A_44_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_35_A_39_ANOS', 'DS_FAIXA_ETARIA_40_A_44_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_45_A_59_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_45_A_49_ANOS', 'DS_FAIXA_ETARIA_50_A_54_ANOS', 'DS_FAIXA_ETARIA_55_A_59_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_60_A_69_ANOS',
                    'columns_to_sum': ['DS_FAIXA_ETARIA_60_A_64_ANOS', 'DS_FAIXA_ETARIA_65_A_69_ANOS']
                },
                {
                    'name': 'DS_FAIXA_ETARIA_SUPERIOR_A_70_ANOS',
                    'columns_to_sum': [
                        'DS_FAIXA_ETARIA_70_A_74_ANOS',
                        'DS_FAIXA_ETARIA_75_A_79_ANOS',
                        'DS_FAIXA_ETARIA_80_A_84_ANOS',
                        'DS_FAIXA_ETARIA_85_A_89_ANOS',
                        'DS_FAIXA_ETARIA_90_A_94_ANOS',
                        'DS_FAIXA_ETARIA_95_A_99_ANOS',
                        'DS_FAIXA_ETARIA_100_ANOS_OU_MAIS'
                    ]
                },
            ]
        }
    }
    datasets = []

    for year, metadata in metadata_by_year.items():
        file_path = Path(profiles_dir, str(year), 'profile.csv')
        year_data = read_csv(file_path)

        year_data.columns = year_data.columns.map(remove_accent)

        for new_column in metadata['columns_to_calculate']:
            year_data[new_column['name']] = 0
            for column_to_sum in new_column['columns_to_sum']:
                year_data[new_column['name']] += year_data[column_to_sum]
                year_data.drop(columns=column_to_sum, inplace=True)

        datasets.append(year_data)

    dataset = concat(datasets)
    dataset = dataset.groupby('CD_MUNICIPIO').mean()

    destination_dir = Path(profiles_dir, 'br')
    destination_dir.mkdir(exist_ok=True)
    file_path = Path(destination_dir, 'profile.csv')
    dataset.to_csv(file_path)


def main():
    """ Runs data processing scripts to turn brazil voting data from (../processed) into
        profile data (saved in ../processed/profile).
    """
    logger = logging.getLogger(__name__)

    logger.info(
        'creating br profile dataset... Saving at ../data/processed/profile/br')
    make_single_profile_dataset()
    logger.info(
        'done creating br profile dataset... Saved at ../data/processed/profile/br')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    data_dir = Path(project_dir, 'data').resolve()

    main()
