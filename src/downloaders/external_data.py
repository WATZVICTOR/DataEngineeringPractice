import logging
import datetime
import concurrent.futures
import shutil
import os

from downloaders.simulator.API_Simulator import API_Simulator

def download_data_from_simulator(start_date_str: str, to_dir : str = "downloads", next_x_days: int = 7, workers: int = 7) -> dict:
    """
    Download data for the next x days from the API simulator.
    Args:
        start_date_str : YYYY-MM-DD datetime pattern used to select the desire check_in to download.
        to_dir : name of the dir, which is used to save the data.
        next_x_days : How many future datset do we want to download, starting from start_date_str.
        workers : Threads pool count.
    Return:
        The path of the file that contains the dataset.
    """
    
    # Inicializar el diccionario para mapear las fechas descargadas a sus archivos
    path_of_downloaded_files: dict = {}

    # Creamos las carpetas donde guardaremos los ficheros que vamos a descargar.
    destination_dir = os.path.join(to_dir, start_date_str)
    os.makedirs(destination_dir, exist_ok=True)
    
    # Convertir la fecha de inicio a un objeto datetime
    start_datetime: datetime.datetime = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    
    # Generar la lista de fechas a descargar
    datetimes_to_download: list = [start_datetime + datetime.timedelta(days=x) for x in range(next_x_days)]

    # Utilizar un pool de hilos para descargar los datos en paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        # Inicializar un controlador de tareas para monitorizar las descargas
        task_controller: dict = {}
        
        # Iterar sobre las fechas a descargar
        for dtd in datetimes_to_download:
            dtd_str: str = dtd.strftime('%Y-%m-%d')
            
            # Enviar la tarea de descarga para cada fecha al pool de hilos
            task_controller[dtd_str] = executor.submit(API_Simulator()._download_rates, start_datetime, dtd)

        # Iterar sobre el controlador de tareas
        for key, value in task_controller.items():

            # Obtenemos el path del endpoint.
            aux_pathfile = task_controller[key].result()

            # Path relativo del fichero descargado.
            file = os.path.basename(aux_pathfile)
            downloaded_file = os.path.join(destination_dir, file)

            # Copiar el archivo descargado al directorio de descargas
            executor.submit(shutil.copyfile, aux_pathfile, downloaded_file)

            # Mapear la fecha descargada a la ruta del archivo descargado
            path_of_downloaded_files[key] = downloaded_file

            logging.info("Downloaded file: {}".format(downloaded_file))

    return path_of_downloaded_files
