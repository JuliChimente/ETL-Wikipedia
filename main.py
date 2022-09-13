import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
from constants import *


def obtener_dominio():
    """ Obtiene el dominio solicitado devolviendolo en string """
    soup = BeautifulSoup(requests.get(URL).content, 'html.parser')
    dominio = etree.HTML(str(soup).replace('<b>', '').replace('</b>', ''))

    return dominio


def obtener_data_jugador(dominio, num_jugador):
    """ Obtencion de datos a partir del dominio y número de jugador """
    jugador = dominio.xpath(RUTA_XPATH_BASE + '[2]/a')[num_jugador].text
    url_bandera = 'https:' + dominio.xpath(RUTA_XPATH_BASE + '[2]/span/a/img')[num_jugador].get('src')
    posicion = dominio.xpath(RUTA_XPATH_BASE + '[1]')[num_jugador].text.strip()

    return [jugador, url_bandera, posicion]


def transformar_posicion_jugador(data_jugador, posicion_anterior):
    """ Función para reemplazar (pos == "=") por la pos real """
    if data_jugador[2] == '=':
        data_jugador[2] = posicion_anterior

    return data_jugador


def generar_dataframe():
    """ Genera y retorna un DataFrame de acuerdo a lo solicitado """
    df = pd.DataFrame(columns=COLUMNAS_DATAFRAME)
    dominio_tabla = obtener_dominio()
    posicion_anterior = ''

    for i in range(CANTIDAD_JUGADORES):
        data_jugador = obtener_data_jugador(dominio_tabla, i)
        data_jugador = transformar_posicion_jugador(data_jugador, posicion_anterior)

        df.loc[len(df)] = data_jugador

        posicion_anterior = data_jugador[2]

    return df


def ejecutar_etl():
    """ Ejecuta un etl con el dataframe previamente creado """
    df = generar_dataframe()
    df.to_csv('resultado.tsv', sep="\t", index=False)


ejecutar_etl()
