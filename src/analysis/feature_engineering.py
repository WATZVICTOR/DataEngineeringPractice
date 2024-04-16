#from pyspark import pandas
import pandas
import os
import logging

from sklearn.preprocessing import minmax_scale


def __factorize_meal_plan(meal_plan_data) -> pandas.Series:

    MEAL_PLAN_FACTORIZACION = {
        "allinclusive" : 0,
        "breakfast" : 1,
        "halfboard" : 2,
        "fullboard" : 3,
        "roomonly"  : 4
    }

    return meal_plan_data.map(MEAL_PLAN_FACTORIZACION)

def __factorize_entity_type(entity_type_data) -> pandas.Series:

    ENTITY_TYPE_FACTORIZACION = {
        "hotel_room" : 0,
        "house_room" : 1,
        "house" : 2
    }

    return entity_type_data.map(ENTITY_TYPE_FACTORIZACION)

def __factorize_season(season_data) -> pandas.Series:

    SEASON_FACTORIZACION = {
        "spring" : 0,
        "summer" : 1,
        "autumn" : 2,
        "winter" : 3
    }

    return season_data.map(SEASON_FACTORIZACION)

def __get_season(check_in_month_data) -> str:

    if 3 <= check_in_month_data <= 5:
        return "spring"
    elif 6 <= check_in_month_data <= 8:
        return  "summer"
    elif 9 <= check_in_month_data <= 11:
        return  "autumn"
    else:
        return "winter"

def feature_downloaded_data(path : str, to_dir : str = "featured_data") -> str:
    """
    Add features to the dataset.
    Args:
        path : direction of the file that contains the dataset.
        to_dir : name of the dir, which is used to save the featured data.
    Return:
        The path of the file that contains the featured dataset.
    """

    # Creamos las carpetas donde guardaremos los ficheros que vamos a modificar.
    os.makedirs(to_dir, exist_ok=True)

    # Path relativo del fichero limpio.
    file          = os.path.basename(path)
    featured_file = os.path.join(to_dir, file)

    # Cargamos el csv.
    data = pandas.read_csv(path, header=0)

    # Normalizamos los datos relativos al precio, ya que no es lo mismo 100 euros a 100 yenes
    data["rate_price_normalized"] = minmax_scale(data["rate_price"])

    # Factoriazmos los valores categoricos.
    data["meal_plan_factorizated"]   = __factorize_meal_plan(data["meal_plan"])
    data["entity_type_factorizated"] = __factorize_entity_type(data["entity_type"])

    # Descomponemos las columnas de tipo fecha en valores enteros.
    data["check_in_year"]   = pandas.to_datetime(data["check_in"]).dt.year
    data["check_in_month"]  = pandas.to_datetime(data["check_in"]).dt.month
    data["check_in_day"]    = pandas.to_datetime(data["check_in"]).dt.day
    data["check_out_year"]  = pandas.to_datetime(data["check_out"]).dt.year
    data["check_out_month"] = pandas.to_datetime(data["check_out"]).dt.month
    data["check_out_day"]   = pandas.to_datetime(data["check_out"]).dt.day

    # Inferimos la columna season que nos viene bien para mas adelante.
    data["season"] = data["check_in_month"].apply(__get_season)

    # Factoriazamos el campo season.
    data["season_factorized"] = __factorize_season(data["season"])

    # Como PySpark es un entorno de procesamiento distribuido, tenemos que pasarlo a pandas
    # para generar un solo csv en el disco de la maquina local.
    #data.to_pandas().to_csv(cleaned_file, index=False)
    data.to_csv(featured_file, index=False)

    logging.info("Featured file {}".format(featured_file))

    return featured_file
