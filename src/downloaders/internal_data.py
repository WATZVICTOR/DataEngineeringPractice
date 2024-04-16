import logging
import os
import boto3

def download_data_from_datalake(resource_to_download : str, aws_configuration : dict, to_dir : str = "download") -> str:
    """
    Download data from S3 datalake.
    """
    
    # Creamos las carpetas donde guardaremos los ficheros que vamos a limpiar.
    os.makedirs(to_dir, exist_ok=True)

    # Path relativo del fichero descargado.
    file = os.path.basename(resource_to_download)
    downloaded_file = os.path.join(to_dir, file)

    # Creamos un cliente de conexion para acceder a los servidores S3 de AWS.
    s3_instance = boto3.client(
        service_name          = aws_configuration["AWS_SERVICE"],
        region_name           = aws_configuration["AWS_REGION"],
        aws_access_key_id     = aws_configuration["AWS_ACCESS_KEY"],
        aws_secret_access_key = aws_configuration["AWS_SECRET_KEY"])
    
    # Abrimos un archivo en el que guardar los datos que descarguemos.
    with open(downloaded_file, "wb") as file:

        try:

            # Descargamos el archivo.
            response = s3_instance.download_fileobj(
                aws_configuration["AWS_S3_BUCKET_NAME"],
                resource_to_download,
                file)
            
            logging.info("Downloaded file from S3: {}".format(downloaded_file))
            
        except Exception as e:

            logging.error("Error descargando el archivo {}.".format(downloaded_file))
            logging.error("%s", e)

            downloaded_file = "Error"

    return downloaded_file