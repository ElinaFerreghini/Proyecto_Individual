

#ESTA FUNCION ME GUSTA MAS, TIENE SOLO DOS PARAMETROS DE ENTRADA, PORQUE EL DURATION_TYPE SI SIEMPRE ES MIN ME PARECE AL PEDO PONERLO
#PERO DESDE HENRY DIJERON QUE RESPETEMOS CONSIGNAS ASI QUE ABAJO HAY OTRO CODIGO CON LAS TRES ENTRADAS

# from fastapi import FastAPI
# import pandas as pd
# import numpy as np
# from collections import Counter

# # cargar el archivo CSV en un DataFrame
# df = pd.read_csv('dataset_plataformas.csv')

# app = FastAPI()

# #http://127.0.0.1:8000

# # @app.get ('/')
# # def prueba():
# #     return 'anda bien'

# ##################################################################################################################################
# @app.get('/get_max_duration/{year}/{platform}')
# def get_max_duration(year: int, platform: str):
#     """
#     devuelve sólo el string del nombre de la película con mayor duración según año, plataforma y tipo de duración 'min'
#     """
#     platform = platform.lower()[0]#Aclaro indice en 0 para que tome la primer letra
#     duration_type = 'min'
#     if (type(year) == int) & (type(platform) == str):
#         resultado = df[(df['release_year']==year) & (df['id'].str.startswith(platform)) & (df['duration_type']== duration_type)]
#         if len(resultado) > 0:
#             idx = resultado['duration_int'].idxmax()
#             return {'pelicula': resultado.loc[idx, 'title']}
#         else:
#             return {'pelicula': f'No se encontró ninguna película en la plataforma {platform} para el año {year}.'}
#     else:
#         return {'pelicula': 'Intenta poner valores correctos.'}
    

#### py -m uvicorn main:app --reload --app-dir C:\Users\LENOVO\Desktop\DATAFT-09\1er_Proyecto_Individual



from fastapi import FastAPI
import pandas as pd
import numpy as np
from collections import Counter

# cargar el archivo CSV en un DataFrame
df = pd.read_csv('df.csv')

promedios= pd.read_csv('promedios.csv')


app = FastAPI()


@app.get('/get_dataframe')
async def get_dataframe():
    """
    Devuelve el DataFrame utilizado en la aplicación Flask en formato JSON.
    """
    df_json = df.to_json(orient='records')  # Convertir el DataFrame a JSON
    return df_json



@app.get('/get_max_duration/{year}/{platform}/{duration_type}')
async def get_max_duration(year: int, platform: str, duration_type: str):
    """
    devuelve sólo el string del nombre de la película con mayor duración según año, plataforma y tipo de duración 'min'
    """
    platform = platform.lower()[0] #Aclaro indice en 0 para que tome la primer letra
    duration_type = duration_type.lower()
    if duration_type != 'min':
        raise ValueError('El tipo de duración debe ser "min"')
    if (type(year) == int) & (type(platform) == str):
        resultado = df[(df['release_year']==year) & (df['id'].str.startswith(platform)) & (df['duration_type']== duration_type)]
        if len(resultado) > 0:
            idx = resultado['duration_int'].idxmax()
            return {'pelicula': resultado.loc[idx, 'title']}
        else:
            return {'pelicula': f'No se encontró ninguna película en la plataforma {platform} para el año {year}.'}
    else:
        return {'pelicula': 'Intenta poner valores correctos.'}
    
    




# @app.get('/get_score_count/{platform}/{scored}/{year}')
# async def get_score_count(platform: str, scored: float, year: int):
#     """
#     Devuelve la cantidad de películas (sólo películas, por ende el campo type debe ser = movie)
#     según plataforma (campo platform), con un puntaje mayor a scored (campo rating)
#     en determinado año.
#     """
#     filtered_df = df[(df['platform'] == platform) & (df['scored'] > scored) & (df['release_year'] == year) & (df['type'] == 'movie')]
#     count = len(filtered_df)
#     return {'cantidad_peliculas': count}


@app.get('/get_score_count/{platform}/{scored}/{year}')
async def get_score_count(platform: str, scored: float, year: int):
    """
    Devuelve la cantidad de películas (sólo películas, por ende el campo type debe ser = movie)
    según plataforma (campo platform), con un puntaje mayor a scored (campo rating)
    en determinado año.
    """
    global df  # Declarar el DataFrame 'df' como global dentro de la función
    filtered_df = df[(df['platform'] == platform) & (df['scored'] > scored) & (df['release_year'] == year) & (df['type'] == 'movie')]
    count = len(filtered_df)
    return {'cantidad_peliculas': count}


@app.get('/get_count_platform/{platform}')
async def get_count_platform(platform: str):
    """
    Devuelve la cantidad de películas (sólo películas, por ende el campo type debe ser = movie)
    según plataforma (campo platform).
    """
    global df  # Declarar el DataFrame 'df' como global dentro de la función
    if platform.lower() not in ['amazon', 'netflix', 'hulu', 'disney']:
        raise ValueError('La plataforma ingresada no es válida')
    filtered_df = df[(df['platform'] == platform.lower()) & (df['type'] == 'movie')]
    count = len(filtered_df)
    return {'cantidad_peliculas': count}




from typing import List

@app.get('/get_actor/{platform}/{year}')
async def get_actor(platform: str, year: int) -> str:
    """
    Actor que más se repite según plataforma (platform en mi df) y año (release_year en mi df).
    """
    global df  # Declarar el DataFrame 'df' como global dentro de la función
    platforms = ['amazon', 'netflix', 'hulu', 'disney']
    if platform.lower() not in platforms:
        return f'La plataforma {platform} no es válida. Las plataformas válidas son: {", ".join(platforms)}'
    elif year not in df['release_year'].unique():
        return f'El año {year} no es válido. Los años válidos son: {", ".join(map(str, df["release_year"].unique()))}'
    else:
        filtered_df = df[(df['platform'] == platform) & (df['release_year'] == year) & (df['type'] == 'movie')]
        if filtered_df.empty:
            return f'No se encontraron películas en la plataforma {platform} para el año {year}'
        else:
            # Obtener una lista de todos los actores presentes en las películas filtradas
            actors = filtered_df['cast'].str.split(',').sum()
            # Realizar el conteo de los actores
            count = Counter(actors)
            # Devolver el actor más común
            most_common = count.most_common(1)[0][0]
            return most_common


@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
async def prod_per_county(tipo: str, pais: str, anio: int):
    """
    Devuelve la cantidad de contenidos/productos que se publicó por país (campo country en mi df) y año (campo release_year en mi df).
    """
    global df  # Declarar el DataFrame 'df' como global dentro de la función
    
    filtered_df = df[(df['type'] == tipo) & (df['country'] == pais) & (df['release_year'] == anio)]
    count = len(filtered_df)
    
    return {'pais': pais, 'anio': anio, 'pelicula': count}


@app.get('/get_contents/{rating}')
async def get_contents(rating: str):
    """
    Devuelve la cantidad total de contenidos/productos según el rating de audiencia dado.
    """
    count = len(df[df['rating'] == rating])
    return {'cantidad': count}


# Crear la función que devuelve las recomendaciones de películas
@app.get('/get_recommendation/{titulo}')
def get_recommendation(titulo: str):
    # Seleccionar el mean_rating de la película ingresada
    mean_rating = promedios.loc[promedios['title'] == titulo, 'mean_ratings'].iloc[0]
    
    # Filtrar las películas con ratings mayores a 200
    filtered_promedios = promedios[(promedios['ratings'] > 200) & (promedios['title'] != titulo)]
    
    # Ordenar el dataframe por la cercanía de los valores de mean_rating
    promedios_sorted = filtered_promedios.iloc[(filtered_promedios['mean_ratings']-mean_rating).abs().argsort()]
    
    # Seleccionar los 5 títulos más cercanos
    top_5_titles = promedios_sorted['title'].head(5).tolist()
    
    # Devolver la lista de títulos
    return top_5_titles