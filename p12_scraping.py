import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import psycopg2
import sys
import requests
from bs4 import BeautifulSoup

#fijamos los datos  de la base de datos (postgres)
DATABASE_LOCATION = "postgresql://postgres:gestion07@localhost/postgres"

#creamos una funcion para que me avise en caso que no se encuentren datos al realizar la extraccion
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No webs downloaded. Finishing execution")
        return False


    # Chequeamos que la variable definida como primary key, no tenga valores repetidos, de lo contrario imprime mensaje
    if pd.Series(df['url_nota']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

#buscamos valores nulos
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

#todas estas funciones vendrian a ser para la parte de "Transform", Luego de la "Extraccion"
if __name__ == "__main__":

    # asignamos la web a scrapear a una variable, que llamamos url
    url = 'https://www.pagina12.com.ar/'

    # hacemos un request de esa url y vemos estado
    p12 = requests.get(url)


    # parseamos el texto de la pagina principal
    s = BeautifulSoup(p12.text, 'lxml')

    secciones = s.find('ul', attrs={'class': 'horizontal-list main-sections hide-on-dropdown'}).find_all('li')

    # función para obtener las secciones en la pagina ppal del diario
    def obtener_secciones():

        links_secciones = []
        secciones = s.find('ul', attrs={'class': 'horizontal-list main-sections hide-on-dropdown'}).find_all('li')
        for i in range(len(secciones)):
            links_secciones.append(secciones[i].a.get('href'))
        return links_secciones

    links_secciones = obtener_secciones()

    # ahora repetimos el proceso, haciendo un request a cada elemento de la lista 'links_secciones':

    request_de_seccion = requests.get(links_secciones[0])

    soup_de_seccion = BeautifulSoup(request_de_seccion.text, 'lxml')

    # creamos una funcion para que traiga todos los links de la seccion que le pongamos

    def obtener_notas(soup_de_seccion):

        lista_notas = []

        # obtengo articulo principal
        noticia_h2 = soup_de_seccion.find('h2', attrs={'class': 'title-list'})
        if noticia_h2:
            lista_notas.append(noticia_h2.a.get('href'))
        # obtengo articulos restantes
        noticias_h3 = soup_de_seccion.find_all('h3', attrs={'class': 'title-list'})
        if noticias_h3:
            for i in range(len(noticias_h3)):
                lista_notas.append(noticias_h3[i].a.get('href'))

        noticias_h4 = soup_de_seccion.find_all('h4', attrs={'class': 'is-display-inline title-list'})
        if noticias_h4:
            for i in range(len(noticias_h4)):
                lista_notas.append(noticias_h4[i].a.get('href'))

        return (lista_notas)

    lista_notas = obtener_notas(soup_de_seccion)

    # hacemos el request para la nota
    r_nota = requests.get(lista_notas[0])

    # parseamos el contenido de esa pagina
    s_nota = BeautifulSoup(r_nota.text, 'lxml')
    print(s_nota.prettify())

    # extraemos los distintos contenidos de la nota :

    def obtener_info(s_nota):
        try:

            # creamos un diccionario vacio para ir metiendo la informacion
            ret_dict = {}



                # Extraemos la fecha
            fecha = s_nota.find('span', attrs={'pubdate': 'pubdate'})
            if fecha:
                ret_dict['fecha'] = fecha.get('datetime')
            else:
                ret_dict['fecha'] = None

            # Extraemos el titulo
            titulo = s_nota.find('h1', attrs={'class': 'article-title'})
            if titulo:
                ret_dict['titulo'] = titulo.get_text()
            else:
                ret_dict['titulo'] = None

                # Extraemos la volanta
            volanta = s_nota.find('h2', attrs={'class': 'article-prefix'})
            if volanta:
                ret_dict['volanta'] = volanta.get_text()
            else:
                ret_dict['volanta'] = None

                # Extraemos el copete

            copete = s_nota.find('div', attrs={'class': 'article-summary'})
            if copete:
                ret_dict['copete'] = copete.get_text()
            else:
                ret_dict['copete'] = None

            # Extraemos el cuerpo
            cuerpo = s_nota.find('div', attrs={'class': 'article-text'})
            if cuerpo:
                ret_dict['texto'] = cuerpo.get_text()
            else:
                ret_dict['texto'] = None

                # Extraemos el autor
            autor = s_nota.find('div', attrs={'class': 'article-author'})
            if autor:
                ret_dict['autor'] = autor.a.get_text()
            else:
                ret_dict['autor'] = None

                # extraemos imagen principal de la nota
            media = s_nota.find('div', attrs={'class': 'article-main-media-image'})
            if media:
                imagenes = media.find_all('img')
                if len(imagenes) == 0:
                    print('no se encontraron imagenes')
                else:
                    imagen = imagenes[-1]
                    img_src = imagen.get('data-src')
                    try:
                        img_req = requests.get(img_src)
                        if img_req.status_code == 200:
                            ret_dict['imagen'] = img_req.content
                        else:
                            ret_dict['imagen'] = None
                    except:
                        print('no se pudo obtener la imagen')
            else:
                print('no se encontro media')
            # Extraemos la seccion
            seccion_nombre = s_nota.find('div', attrs={'class': 'suplement'})
            if seccion_nombre:
                ret_dict['seccion'] = seccion_nombre.a.get_text()
            else:
                ret_dict['seccion'] = None

            return ret_dict

        except Exception as e:
            print('Error')
            print(e)
            print('\n')

    obtener_info(s_nota)

    # definimos una funcion cuyo parametro va a ser la url de la nota que queremos scrapear
    def scrape_nota(url_nota):
        try:
            r_nota = requests.get(url_nota)
        except Exception as e:
            print('Error scrapeando url', url_nota)
            print(e)
            return None
        if r_nota.status_code != 200:
            print(f'Error obteniendo nota {url_nota}')
            print(f'Status Code={r_nota.status_code}')
            return None
        s_nota = BeautifulSoup(r_nota.text, 'lxml')
        ret_dict = obtener_info(s_nota)
        ret_dict['url_nota'] = url_nota
        return ret_dict

    # armamos una lista con todas las notas que estan en cada una de las secciones (links_secciones)
    notas = []
    for i in range(len(links_secciones)):
        r = requests.get(links_secciones[i])
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'lxml')
            notas.extend(obtener_notas(soup))
        else:
            print('no se pudo obtener la seccion', link)

    data = []

    for i, nota in enumerate(notas):
        print(f'procesando nota: {i}/{len(notas)}')
        data.append(scrape_nota(nota))

    # pasamos lo obtenido  a un df de pandas y exportamos a un .csv
    df = pd.DataFrame(data)
    df.to_csv(f'{datetime.datetime.today().strftime("%d-%m-%Y-%H:%M:%S")}.csv')

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn_string = "host='localhost' dbname='postgres' user='postgres' password='gestion07'"
    conn = psycopg2.connect(conn_string)
    print("Opened database successfully")

    try:
        df.to_sql("notas_p12", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")

# asigno el codigo a  una funcion que va a ser la que el archivo del dag va a importar como operador
def run_p12_scraping():
    database_location = "postgresql://postgres:gestion07@localhost/postgres"
    # asignamos la web a scrapear a una variable, que llamamos url
    url = 'https://www.pagina12.com.ar/'

    # hacemos un request de esa url y vemos estado
    p12 = requests.get(url)

    # parseamos el texto de la pagina principal
    s = BeautifulSoup(p12.text, 'lxml')

    secciones = s.find('ul', attrs={'class': 'horizontal-list main-sections hide-on-dropdown'}).find_all('li')

    # función para obtener las secciones en la pagina ppal del diario
    def obtener_secciones():

        links_secciones = []
        secciones = s.find('ul', attrs={'class': 'horizontal-list main-sections hide-on-dropdown'}).find_all('li')
        for i in range(len(secciones)):
            links_secciones.append(secciones[i].a.get('href'))
        return links_secciones

    links_secciones = obtener_secciones()

    # ahora repetimos el proceso, haciendo un request a cada elemento de la lista 'links_secciones':

    request_de_seccion = requests.get(links_secciones[0])

    soup_de_seccion = BeautifulSoup(request_de_seccion.text, 'lxml')

    # creamos una funcion para que traiga todos los links de la seccion que le pongamos

    def obtener_notas(soup_de_seccion):

        lista_notas = []

        # obtengo articulo principal
        noticia_h2 = soup_de_seccion.find('h2', attrs={'class': 'title-list'})
        if noticia_h2:
            lista_notas.append(noticia_h2.a.get('href'))
        # obtengo articulos restantes
        noticias_h3 = soup_de_seccion.find_all('h3', attrs={'class': 'title-list'})
        if noticias_h3:
            for i in range(len(noticias_h3)):
                lista_notas.append(noticias_h3[i].a.get('href'))

        noticias_h4 = soup_de_seccion.find_all('h4', attrs={'class': 'is-display-inline title-list'})
        if noticias_h4:
            for i in range(len(noticias_h4)):
                lista_notas.append(noticias_h4[i].a.get('href'))

        return (lista_notas)

    lista_notas = obtener_notas(soup_de_seccion)

    # hacemos el request para la nota
    r_nota = requests.get(lista_notas[0])

    # parseamos el contenido de esa pagina
    s_nota = BeautifulSoup(r_nota.text, 'lxml')
    print(s_nota.prettify())

    # extraemos los distintos contenidos de la nota :

    def obtener_info(s_nota):
        try:

            # creamos un diccionario vacio para ir metiendo la informacion
            ret_dict = {}



                # Extraemos la fecha
            fecha = s_nota.find('span', attrs={'pubdate': 'pubdate'})
            if fecha:
                ret_dict['fecha'] = fecha.get('datetime')
            else:
                ret_dict['fecha'] = None

            # Extraemos el titulo
            titulo = s_nota.find('h1', attrs={'class': 'article-title'})
            if titulo:
                ret_dict['titulo'] = titulo.get_text()
            else:
                ret_dict['titulo'] = None

                # Extraemos la volanta
            volanta = s_nota.find('h2', attrs={'class': 'article-prefix'})
            if volanta:
                ret_dict['volanta'] = volanta.get_text()
            else:
                ret_dict['volanta'] = None

                # Extraemos el copete

            copete = s_nota.find('div', attrs={'class': 'article-summary'})
            if copete:
                ret_dict['copete'] = copete.get_text()
            else:
                ret_dict['copete'] = None

            # Extraemos el cuerpo
            cuerpo = s_nota.find('div', attrs={'class': 'article-text'})
            if cuerpo:
                ret_dict['texto'] = cuerpo.get_text()
            else:
                ret_dict['texto'] = None

                # Extraemos el autor
            autor = s_nota.find('div', attrs={'class': 'article-author'})
            if autor:
                ret_dict['autor'] = autor.a.get_text()
            else:
                ret_dict['autor'] = None

                # extraemos imagen principal de la nota
            media = s_nota.find('div', attrs={'class': 'article-main-media-image'})
            if media:
                imagenes = media.find_all('img')
                if len(imagenes) == 0:
                    print('no se encontraron imagenes')
                else:
                    imagen = imagenes[-1]
                    img_src = imagen.get('data-src')
                    try:
                        img_req = requests.get(img_src)
                        if img_req.status_code == 200:
                            ret_dict['imagen'] = img_req.content
                        else:
                            ret_dict['imagen'] = None
                    except:
                        print('no se pudo obtener la imagen')
            else:
                print('no se encontro media')

                # Extraemos la seccion
            seccion_nombre = s_nota.find('div', attrs={'class': 'suplement'})
            if seccion_nombre:
                ret_dict['seccion'] = seccion_nombre.a.get_text()
            else:
                ret_dict['seccion'] = None

            return ret_dict

        except Exception as e:
            print('Error')
            print(e)
            print('\n')

    obtener_info(s_nota)

    # definimos una funcion cuyo parametro va a ser la url de la nota que queremos scrapear
    def scrape_nota(url_nota):
        try:
            r_nota = requests.get(url_nota)
        except Exception as e:
            print('Error scrapeando url', url_nota)
            print(e)
            return None
        if r_nota.status_code != 200:
            print(f'Error obteniendo nota {url_nota}')
            print(f'Status Code={r_nota.status_code}')
            return None
        s_nota = BeautifulSoup(r_nota.text, 'lxml')
        ret_dict = obtener_info(s_nota)
        ret_dict['url_nota'] = url_nota
        return ret_dict

    # armamos una lista con todas las notas que estan en cada una de las secciones (links_secciones)
    notas = []
    for i in range(len(links_secciones)):
        r = requests.get(links_secciones[i])
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'lxml')
            notas.extend(obtener_notas(soup))
        else:
            print('no se pudo obtener la seccion', link)

    data = []

    for i, nota in enumerate(notas):
        print(f'procesando nota: {i}/{len(notas)}')
        data.append(scrape_nota(nota))

    # pasamos lo obtenido  a un df de pandas
    df = pd.DataFrame(data)
    df.to_csv(f'{datetime.datetime.today().strftime("%d-%m-%Y-%H:%M:%S")}.csv')
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn_string = "host='localhost' dbname='postgres' user='postgres' password='gestion07'"
    conn = psycopg2.connect(conn_string)
    print("Opened database successfully")

    try:
        df.to_sql("notas_p12", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")





