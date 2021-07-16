from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import timedelta

import pandas as pd
import os
import requests


home_dir = os.environ["HOME"]
url_base = "https://datasets.imdbws.com"


def transform_movies_data():
    """
    Obtener y transformar los datos de peliculas
    y guardar el resultado en el directorio stating
    """
    file_name = "title.basics.tsv.gz"
    url_base = "https://datasets.imdbws.com"
    url_file = f"{url_base}/{file_name}"
    titles_df = pd.read_csv(
                    url_file,
                    sep="\t", encoding="utf-8",
                    dtype="object", na_values=r"\N")

    # obtener solos los titulo de tipo "pelicula"
    movies_mask = titles_df["titleType"] == "movie"
    movies_df = titles_df.loc[movies_mask]

    # eliminar valores nulos de algunas columnas
    movies_df = movies_df.dropna(
                            subset=["startYear", "runtimeMinutes", "genres"])

    # obtener solo algunas columnas del dataframe original
    final_cols = [
        "tconst", "primaryTitle", "originalTitle",
        "startYear", "runtimeMinutes", "genres"]
    movies_df = movies_df.loc[:, final_cols]

    # filtrar las peliculas estrenadas durante el 2015 y el 2020 inclusive
    movies_df["startYear"] = movies_df["startYear"].astype("int")
    year_mask = (movies_df["startYear"] >= 2015) & \
                (movies_df["startYear"] <= 2020)
    movies_df = movies_df[year_mask]

    # Ampliar el dataframe, creando una fila por cada genero de cada pelicula
    movies_df["genres"] = movies_df["genres"].str.split(",")
    movies_df = movies_df.explode("genres")

    # Guardar el resultado en un archivo csv en el directorio staging
    movies_df.to_csv(
        f"{home_dir}/staging/movies_basics.csv",
        index=None, sep=";")


def transform_crew_data():
    """
    Leer y transformar los datos sobre directores y escritores
    y guardar el resultado en el directorio staging
    """
    crew_df = pd.read_csv(
                        f"{home_dir}/data/title.crew.tsv.gz",
                        sep="\t", encoding="utf-8",
                        usecols=["tconst", "directors", "writers"],
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

    # Guardar ambos dataframes en un archivo csv en el directorio staging
    directors_df.to_csv(
                    f"{home_dir}/staging/directors.csv",
                    sep=";", index=None)
    writers_df.to_csv(
                    f"{home_dir}/staging/writers.csv",
                    sep=";", index=None)


def join_data():
    """
    Leer y cruzar datos del directorio staging para obtener de cada pelicula
    - anio de estreno y genero
    - los directores y escritores
    - rating promedio y cantidad de votos recibidos
    """
    directors_df = pd.read_csv(
                        f"{home_dir}/staging/writers.csv", sep=";")

    writers_df = pd.read_csv(
                        f"{home_dir}/staging/directors.csv", sep=";")

    basics_df = pd.read_csv(
                        f"{home_dir}/staging/movies_basics.csv", sep=";")

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
    target_df = joined_df \
                    .groupby(["startYear", "genres"]) \
                    .agg({
                        "numVotes": "sum",
                        "averageRating": "mean",
                        "runtimeMinutes": "mean",
                        "directors": "nunique",
                        "writers": "nunique"
                        }) \
                    .rename(
                        columns={
                            "directors": "numDirectors",
                            "writers": "numWriters"
                            }) \
                    .round(2)

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
