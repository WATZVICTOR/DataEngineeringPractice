import logging
import boto3

def upload_data_to_datalake(path : str, aws_configuration : dict) -> str:
    """
    Upload the file located at filepath to an S3 Instance.
    Args:
        path : direction of the file that contains the dataset.
        aws_configuration : Configurations which belong to the user who has access to the S3 service.
    Return:
        The path of the endpoint which contains the uploadad file.
    """

    # Creamos un cliente de conexion para acceder a los servidores S3 de AWS.
    s3_instance = boto3.client(
        service_name          = aws_configuration["AWS_SERVICE"],
        region_name           = aws_configuration["AWS_REGION"],
        aws_access_key_id     = aws_configuration["AWS_ACCESS_KEY"],
        aws_secret_access_key = aws_configuration["AWS_SECRET_KEY"])
    
    # Serializamos el archivo ya que no nos importa el tipo de datos que subamos y de esta manera
    # podemos tratarlos a todos bajo el mismo pretexto operacional.
    with open(path, "rb") as file:

        try:

            # Subimos el archivo.
            response = s3_instance.upload_fileobj(
                file,
                aws_configuration["AWS_S3_BUCKET_NAME"],
                path)
            
            logging.info("Uploaded file: {}".format(path))
            
        except Exception as e:

            logging.error("Error subiendo el archivo {}".format(path))
            logging.error("%s", e)

    return path
