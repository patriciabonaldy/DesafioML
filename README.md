# `MercadoLibre` - Readme


Provee una interfaz al usuario que le permite subir lotes de id's 
Posteriormente a traves de esos Id_site, busca las categorias, vendedores, etc


## How to install

#PRE-REQUISITOS:
```
Debes tener la ultima version de python 3.8
        sudo apt install python3.8
Asegurate de que tengas instalado pipenv, en caso contrario:
        pip3 install --user pipenv
Configura python 3.8 en pipenv        
        pipenv --python 3.8

Descarga la imagen oracle del proyecto https://github.com/patriciabonaldy/docker_oracle.git
seguir las instrucciones plasmada en su README.md
```


## CLONAR LA IMAGEN 
git clone https://github.com/patriciabonaldy/MercadoLibre.git


##  Development

    Para configurar el servicio:

    ~~~bash
    ./scripts/service.sh  env
    ~~~


## Para levantar la aplicacion

    ~~~bash
    ./scripts/service.sh  rundev
    ~~~


## Para ejecutar los test:

    ```
    ./scripts/service.sh  runtest
    ```
