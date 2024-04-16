from pyspark import pandas
#import pandas
import os
import logging

def __clean_check_in(check_in_data) -> pandas.Series:
    # El check_in ha de esatr en el formato de fecha YYYY-MM-DD
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    cleaned_data = check_in_data.astype(str).str.match(date_pattern)
    return cleaned_data

def __clean_check_out(check_out_data) -> pandas.Series:
    # El check_out ha de esatr en el formato de fecha YYYY-MM-DD
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    cleaned_data = check_out_data.astype(str).str.match(date_pattern)
    return cleaned_data

def __clean_los(los_data) -> pandas.Series:
    # El numero de noches no puede ser negativo.
    cleaned_data = los_data.astype(int) >= 0
    return cleaned_data

def __clean_entity_id(entity_id_data) -> pandas.Series:
    # El id del hotel no puede ser negativo
    cleaned_data = entity_id_data.astype(int) >= 0
    return cleaned_data

def __clean_currency(currency_data) -> pandas.Series:
    # La moneda de pago no puede estar vacia y ha de tener 3 caracteres.
    cleaned_data = (currency_data.astype(str).str.strip() != "") & (currency_data.astype(str).str.len() == 3)
    return cleaned_data

def __clean_rate_price(rate_price_data) -> pandas.Series:
    # El precio del plan no puede ser negativo.
    cleaned_data = rate_price_data.astype(float) >= 0
    return cleaned_data

def __clean_rate_id(rate_id_data) -> pandas.Series:
    # El id del plan no puede ser negativo.
    cleaned_data = rate_id_data.astype(int) >= 0
    return cleaned_data

def __clean_rate_name(rate_name_data) -> pandas.Series:
    # El nombre del plan ha de empezar por la frase "Rate Plan" seguido de un numero entero
    regular_expression = r"^Rate Plan \d+$"
    cleaned_data = rate_name_data.astype(str).str.strip().str.match(regular_expression)
    return cleaned_data

def __clean_meal_plan(meal_plan_data) -> pandas.Series:
    # El meal_plan solo puede tener los valores almacenados en la lista valid_values
    valid_values = ["allinclusive", "breakfast", "halfboard", "fullboard", "roomonly"]
    cleaned_data = meal_plan_data.astype(str).str.strip().isin(valid_values)
    return cleaned_data

def __clean_count(count_data) -> pandas.Series:
    # El numero de bookings no puede ser negativo.
    cleaned_data = count_data.astype(int) >= 0
    return cleaned_data

def __clean_adults(adults_data) -> pandas.Series:
    # El numero de adultos no puede ser negativo.
    cleaned_data = adults_data.astype(int) >= 0
    return cleaned_data

def __clean_children(children_data) -> pandas.Series:
    # El número de niños no puede ser negativo.
    cleaned_data = children_data.astype(int) >= 0
    return cleaned_data

def __clean_entity_latitude(entity_latitude_data) -> pandas.Series:
    # La latitud ha de ser un valor decimal.
    cleaned_data = pandas.to_numeric(entity_latitude_data, errors='coerce').notnull()
    return cleaned_data

def __clean_entity_longitude(entity_longitude_data) -> pandas.Series:
    # La longitud ha de ser un valor float.
    cleaned_data = pandas.to_numeric(entity_longitude_data, errors='coerce').notnull()
    return cleaned_data

def __clean_entity_city(entity_city_data) -> pandas.Series:
    # La ciudad no puede ser un campo vacio.
    cleaned_data = entity_city_data.astype(str).str.strip() != ""
    return cleaned_data

def __clean_entity_type(entity_type_data) -> pandas.Series:
    # El entity_type solo puede tener los valores almacenados en la lista valid_values
    valid_values = ["hotel_room", "house_room", "house"]
    cleaned_data = entity_type_data.astype(str).str.strip().isin(valid_values)
    return cleaned_data

def __clean_entity_name(entity_name_data) -> pandas.Series:
    # El entity name ha de seguir el siguiente patron pattern = r'^Hotel_room|House_room|House from [A-Za-z]+ \d+$'
    regular_expression = r'^Hotel_room|House_room|House from [A-Za-z]+ \d+$'
    cleaned_data = entity_name_data.astype(str).str.match(regular_expression)
    return cleaned_data

def __clean_entity_stars(entity_stars_data) -> pandas.Series:
    # El numero de estrellas solo puede estar entre 0 y 5
    cleaned_data = (entity_stars_data.astype(float) >= 5) & (0 <= entity_stars_data.astype(float))
    return cleaned_data

def clean_downloaded_data(path : str, to_dir : str = "cleaned_data") -> str:
    """
    Clean the downloaded file located at path.
    Args:
        path : direction of the file that contains the dataset.
        to_dir : name of the dir, which is used to save the cleaned data.
    Return:
        The path of the file that contains the cleaned dataset.
    """

    # Creamos las carpetas donde guardaremos los ficheros que vamos a limpiar.
    os.makedirs(to_dir, exist_ok=True)

    # Path relativo del fichero limpio.
    file = os.path.basename(path)
    cleaned_file = os.path.join(to_dir, file)

    # Cargamos el csv.
    data = pandas.read_csv(path, header=0, nrows=500)

    # Comprobamos valores extraños o nulos.

    data = data.loc[__clean_check_in(data['check_in'])]
    data = data.loc[__clean_check_out(data['check_out'])]
    data = data.loc[__clean_los(data['los'])]
    data = data.loc[__clean_entity_id(data['entity_id'])]
    data = data.loc[__clean_currency(data['currency'])]
    data = data.loc[__clean_rate_price(data['rate_price'])]
    data = data.loc[__clean_rate_id(data['rate_id'])]
    data = data.loc[__clean_rate_name(data['rate_name'])]
    data = data.loc[__clean_meal_plan(data['meal_plan'])]
    data = data.loc[__clean_count(data['count'])]
    data = data.loc[__clean_adults(data['adults'])]
    data = data.loc[__clean_children(data['children'])]
    data = data.loc[__clean_entity_latitude(data['entity_latitude'])]
    data = data.loc[__clean_entity_longitude(data['entity_longitude'])]
    data = data.loc[__clean_entity_city(data['entity_city'])]
    data = data.loc[__clean_entity_type(data['entity_type'])]
    data = data.loc[__clean_entity_name(data['entity_name'])]
    data = data.loc[__clean_entity_stars(data['entity_stars'])]

    # Como PySpark es un entorno de procesamiento distribuido, tenemos que pasarlo a pandas
    # para generar un solo csv en el disco de la maquina local.
    #data.to_pandas().to_csv(cleaned_file, index=False)
    data.to_csv(cleaned_file, index=False)

    logging.info("Cleaned file {}".format(cleaned_file))

    return cleaned_file

