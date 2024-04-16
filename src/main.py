#
# main.py
# Victor Juan Conde
#

from downloaders.external_data import download_data_from_simulator
from downloaders.internal_data import download_data_from_datalake

from uploaders.upload_data import upload_data_to_datalake

from analysis.data_cleaner import clean_downloaded_data
from analysis.feature_engineering import feature_downloaded_data
from analysis.graph_creator import create_graph

import logging
import os
import time

AWS_VICTOR_CONFIGURATION : dict = {
    "AWS_S3_BUCKET_NAME" : "victor-s3-aws-bucket",
    "AWS_REGION"         : "eu-south-2",
    "AWS_SERVICE"        : "s3",
    "AWS_ACCESS_KEY"     : "AKIASZCHUEDZZQOHKFWL",
    "AWS_SECRET_KEY"     : "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"
}

PROGRAM_CONFIGURATION : dict = {
    "DATE_TO_DOWNLOAD" : "2023-07-02",
    "NEXT_X_DAY"       : 7,
    "WORKERS"          : 7,
    "DOWNLOADS_DIR"    : "downloads",
    "CLEAN_DATA_DIR"   : "clean_data",
    "FEATURE_DATA_DIR" : "feature_data",
    "S3_DOWNLOAD_DIR"  : "s3_downloads",
    "GRAPH_DIR"        : "graphs"
}


def main():
    """
    Entrypoint program.
    """

    logging.basicConfig(level=logging.INFO)

    ####### Descargamos los datos. #######

    logging.info("Downloading data from external endpoint. (Destination dir -> {})".format(PROGRAM_CONFIGURATION["DOWNLOADS_DIR"]))

    downloaded_data : dict = download_data_from_simulator(
        start_date_str = PROGRAM_CONFIGURATION["DATE_TO_DOWNLOAD"],
        next_x_days = PROGRAM_CONFIGURATION["NEXT_X_DAY"],
        to_dir = PROGRAM_CONFIGURATION["DOWNLOADS_DIR"],
        workers = PROGRAM_CONFIGURATION["WORKERS"])

    ####### Limpiamos los datos descargados. #######

    logging.info("Cleaning downloaded data. (Destination dir -> {})".format(PROGRAM_CONFIGURATION["CLEAN_DATA_DIR"]))

    cleaned_data : list = []

    save_data_to_dir = os.path.join(PROGRAM_CONFIGURATION["CLEAN_DATA_DIR"], PROGRAM_CONFIGURATION["DATE_TO_DOWNLOAD"])

    for file in downloaded_data.values():

        cleaned_data.append(clean_downloaded_data(path = file, to_dir = save_data_to_dir))

    ####### Añadimos mas columnas al conjunto de datos. (Feature engineering) #######

    logging.info("Featuring downloaded data. (Destination dir -> {})".format(PROGRAM_CONFIGURATION["FEATURE_DATA_DIR"]))

    featured_data : list = []

    save_data_to_dir = os.path.join(PROGRAM_CONFIGURATION["FEATURE_DATA_DIR"], PROGRAM_CONFIGURATION["DATE_TO_DOWNLOAD"])

    for file in cleaned_data:

        featured_data.append(feature_downloaded_data(path = file, to_dir = save_data_to_dir))

    ####### Subimos a nuestro datalake los datos limpios. #######

    logging.info("Uploading data to S3 datalake. {}".format(AWS_VICTOR_CONFIGURATION["AWS_S3_BUCKET_NAME"]))

    resource_paths : list = []

    for file in featured_data:

        resource_paths.append(upload_data_to_datalake(file, aws_configuration=AWS_VICTOR_CONFIGURATION))

    ####### Descaragmos los datos de nuestro datalake. #######

    logging.info("Downloading data from S3.")

    s3_downloaded_data : list = []

    save_data_to_dir = os.path.join(PROGRAM_CONFIGURATION["S3_DOWNLOAD_DIR"], PROGRAM_CONFIGURATION["DATE_TO_DOWNLOAD"])

    for resource_path in resource_paths:

        s3_downloaded_data.append(download_data_from_datalake(resource_path, to_dir = save_data_to_dir, aws_configuration=AWS_VICTOR_CONFIGURATION))

    ####### Creamos los graficos. #######

    logging.info("Creating graphs. (Destination dir -> {})".format(PROGRAM_CONFIGURATION["GRAPH_DIR"]))

    save_data_to_dir = os.path.join(PROGRAM_CONFIGURATION["GRAPH_DIR"], PROGRAM_CONFIGURATION["DATE_TO_DOWNLOAD"])

    for file in s3_downloaded_data:

        create_graph(file, to_dir = save_data_to_dir)


main()
