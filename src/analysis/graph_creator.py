import pandas
import os
import matplotlib.pyplot as plt
import logging

def create_graph(path : str, to_dir = "graph_dir") -> str:
    """
    Create a graph based onrates and grouped by city/season.
    """

    # Creamos las carpetas donde guardaremos los ficheros que vamos a limpiar.
    os.makedirs(to_dir, exist_ok=True)

    # Path relativo del fichero limpio.
    file = os.path.basename(os.path.splitext(path)[0])
    graph_file = os.path.join(to_dir, file)

    # Cargamos el csv.
    data = pandas.read_csv(path, header=0, nrows=500)

    # Agrupamos los datos por ciudad y estacion.
    # El campo season ya lo calculamos antes.
    group_data = data.groupby(["entity_city", "season"])

    mean_rate_prices = group_data["rate_price"].mean()

    # Creamos un gráfico de barras.
    mean_rate_prices.plot(kind='bar')

    # Añadiamos mas datos de utilidad.
    plt.title("BEONx Assigment")
    plt.xlabel("City-Season")
    plt.ylabel("Mean rate_price")

    plt.savefig(graph_file)

    logging.info("Graph created: {}".format(graph_file))

    return graph_file

