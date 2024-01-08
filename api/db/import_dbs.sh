#!/bin/bash

# This should be run on a separate server so processing is decoupled from serving.

if [ $(hostname) = 'hound' ]; then
    username='hound'
else
    username='admin'
fi

project_folder="/home/${username}/defi_stats"
source "${project_folder}/api/.env"

echo "Node type is: $NODE_TYPE"
if [ $NODE_TYPE = 'serve' ]; then
    echo "Sourcing pre-processed master databases"
    rsync -avzP admin@test.defi-stats.komodo.earth:/home/admin/defi_stats/api/db/master/MM2_7777.db                     "${project_folder}/api/db/source/MM2_7777.db"
    /usr/bin/python3 "${project_folder}/api/db/backup_db.py" --src "${project_folder}/api/db/source/MM2_7777.db" --dest "${project_folder}/api/db/master/MM2_7777.db"
    rsync -avzP admin@test.defi-stats.komodo.earth:/home/admin/defi_stats/api/db/master/MM2_8762.db                     "${project_folder}/api/db/source/MM2_8762.db"
    /usr/bin/python3 "${project_folder}/api/db/backup_db.py" --src "${project_folder}/api/db/source/MM2_8762.db" --dest "${project_folder}/api/db/master/MM2_8762.db"
    rsync -avzP admin@test.defi-stats.komodo.earth:/home/admin/defi_stats/api/db/master/MM2_all.db                      "${project_folder}/api/db/source/MM2_all.db"
    /usr/bin/python3 "${project_folder}/api/db/backup_db.py" --src "${project_folder}/api/db/source/MM2_all.db" --dest  "${project_folder}/api/db/master/MM2_all.db"
else
    # MM2.db from each seednode is mustered on the stats-api.atomicdex.io server
    echo "Sourcing seed node source databases"
    rsync -avzP atomic@stats-api.atomicdex.io:/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db     /home/${username}/defi_stats/api/db/source/seed7_MM2.db
    rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed2_MM2.db                            /home/${username}/defi_stats/api/db/source/seed2_MM2.db
    rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed3_MM2.db                            /home/${username}/defi_stats/api/db/source/seed3_MM2.db
    rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/viserion_MM2.db                         /home/${username}/defi_stats/api/db/source/viserion_MM2.db
    rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/rhaegal_MM2.db                          /home/${username}/defi_stats/api/db/source/rhaegal_MM2.db
    rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/drogon_MM2.db                           /home/${username}/defi_stats/api/db/source/drogon_MM2.db
fi
