#!/bin/bash

APP_NAME=mercadolibre-api
VERSION=$(git describe --tags)

if [ -z $VERSION ]; then
    VERSION=0.0.1 #Por si no hay version en git aun
fi


create_env(){
pipenv --python 3.8
pipenv install
pipenv install --dev
pipenv install -r requirements.txt 
exit
echo "Para activar el nuevo env basta con hacer pipenv shell"
}

run_dev(){
    export FLASK_ENV=development
    export FLASK_APP=main.py
    flask run --host=127.0.0.1 --port 5000
}




test(){
    #TODO: Crear tests para correr XD
    echo "Ejecutando los tests"
    python3 -m unittest discover -s tests
}

case "$1" in
    env)
        create_env
        ;;
    rundev)
        run_dev
        ;;
    runtest)    
        test
        ;;
    *)
        echo "Usage: $0 {env|rundev|runtest}"
esac