from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import timedelta

import pandas as pd
import feather
import os

home_dir = os.environ["HOME"]
url_base = "https://datasets.imdbws.com"


def transform_movies_data():
    """
    Obtener y transformar los datos del archivo title.basic.tsv.gz
    y guardar el resultado en el directorio stating
    """
    file_name = "title.basics.tsv.gz"
    url_file = f"{url_base}/{file_name}"

    # columnas a utilizar del dataset
    req_cols = [
        "tconst", "titleType", "primaryTitle", "originalTitle",
        "startYear", "runtimeMinutes", "genres"]

    titles_df = pd.read_csv(
                    url_file,
                    sep="\t", encoding="utf-8",
                    usecols=req_cols,
                    dtype="object", na_values=r"\N")

    # obtener solos los titulo de tipo "pelicula"
    movies_mask = titles_df["titleType"] == "movie"
    movies_df = titles_df.loc[movies_mask]
    movies_df.drop(columns="titleType")

    # eliminar valores nulos de algunas columnas
    movies_df = movies_df.dropna(
                            subset=["startYear", "runtimeMinutes", "genres"])

    # convertir algunas columnas a tipo entero
    movies_df["startYear"] = movies_df["startYear"].astype("int")
    movies_df["runtimeMinutes"] = movies_df["runtimeMinutes"].astype("int")

    # filtrar las peliculas estrenadas durante el 2015 y el 2020 inclusive
    year_mask = (movies_df["startYear"] >= 2015) & \
                (movies_df["startYear"] <= 2020)
    movies_df = movies_df[year_mask]

    # Ampliar el dataframe, creando una fila por cada genero de cada pelicula
    movies_df["genres"] = movies_df["genres"].str.split(",")
    movies_df = movies_df.explode("genres")

    #Persistir el dataframe en un archivo feather en el directorio staging
    feather.write_dataframe(
                        movies_df,
                        f"{home_dir}/staging/movies_basics.feather")

def transform_crew_data():
    """
    Leer y transformar los datos sobre directores y escritores
    y guardar el resultado en el directorio staging
    """
    file_name = "title.crew.tsv.gz"
    url_file = f"{url_base}/{file_name}"
    crew_df = pd.read_csv(
                        url_file,
                        sep="\t", encoding="utf-8",
                        na_values=r"\N")

    directors_df = crew_df[["tconst", "directors"]]
    writers_df = crew_df[["tconst", "writers"]]

    directors_df = directors_df.dropna(subset=["directors"])
    writers_df = writers_df.dropna(subset=["writers"])

    # Transformar columnas de tipo string a una lista
    directors_df["directors"] = directors_df["directors"].str.split(",")
    writers_df["writers"] = writers_df["writers"].str.split(",")

    # Ampliar dataframes por cada director y pelicula de cada titulo
    directors_df = directors_df.explode("directors")
    writers_df = writers_df.explode("writers")

    # Serializar ambos dataframes en un archivos pkl en el directorio staging
    feather.write_dataframe(
                    directors_df,
                    f"{home_dir}/staging/directors.feather")
    feather.write_dataframe(
                    writers_df,
                    f"{home_dir}/staging/writers.feather")


def join_data():
    """
    Leer y cruzar datos del directorio staging para obtener de cada pelicula
    - anio de estreno y genero
    - los directores y escritores
    - rating promedio y cantidad de votos recibidos
    """
    directors_df = feather.read_dataframe(
                        f"{home_dir}/staging/writers.feather")

    writers_df = feather.read_dataframe(
                        f"{home_dir}/staging/directors.feather")

    basics_df = feather.read_dataframe(
                        f"{home_dir}/staging/movies_basics.feather")

    ratings_df = pd.read_csv(
                        f"{url_base}/title.ratings.tsv.gz",
                        sep="\t", encoding="utf-8")

    joined_df = basics_df.join(
                        ratings_df.set_index("tconst"),
                        on="tconst", how="left"
                        ).join(
                        directors_df.set_index("tconst"),
                        on="tconst", how="left"
                        ).join(
                        writers_df.set_index("tconst"),
                        on="tconst", how="left")

    return joined_df


def aggregate_data(joined_df):
    """
    Calcular, por cada anio de estreno y genero, para todas las peliculas
    - la cantidad total de votos,
    - la media de la duracion y la media del rating
    - cantidad de directores distintos,
    - cantidad de escritores distintos
    """
    target_df = joined_df.groupby(
                            ["startYear", "genres"]
                        ).agg({
                            "numVotes": "sum",
                            "averageRating": "mean",
                            "runtimeMinutes": "mean",
                            "directors": "nunique",
                            "writers": "nunique"
                            }
                        ).rename(
                            columns={
                                "directors": "numDirectors",
                                "writers": "numWriters"}
                        ).round(2)

    target_df.to_csv(f"{home_dir}/results.csv", sep=";")


def join_and_agg_data():
    joined_data = join_data()
    aggregate_data(joined_data)


default_args = {
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
        'datathon',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
        ) as dag:

    t1 = PythonOperator(
        task_id="transform_basics_data",
        python_callable=transform_movies_data)

    t2 = PythonOperator(
        task_id="transform_crew_data",
        python_callable=transform_crew_data)

    t3 = PythonOperator(
        task_id="join_and_aggregate_data",
        python_callable=join_and_agg_data)

t1 >> t3
t2 >> t3
