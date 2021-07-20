## Como utilizar esta imagen de Docker

### Primero
Compilar la imagen de la siguente forma:

```console
docker build . -t my_airflow
```

### Segundo
Ejecutar un container a partir de dicha imagen
```console
docker run -it --rm --name airflow -p 8080:8080 my_aiflow
```

### Por ultimo
En un browser, abrir `<hostname>:8080` y ejecutar el dag
