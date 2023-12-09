#!/bin/bash

DB_PATH='/DB/1b61473e9a419ee33668d72653b8d9630b16fef4'
CRONS_PATH='/home/atomic/crons'
DB_NAME='seed1_MM2.db'

/usr/bin/python3 ${CRONS_PATH}/backup_db.py --src ${DB_PATH}/MM2.db --dest ${DB_PATH}/${DB_NAME}
/usr/bin/rsync -avzP ${DB_PATH}/${DB_NAME} atomic@stats-api.atomicdex.io:/home/atomic/DB/${DB_NAME}
