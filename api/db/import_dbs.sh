#!/bin/bash

# This should be run on a separate server so processing is decoupled from serving.

if [ $(hostname) = 'hound' ]; then
    username='hound'
else
    username='admin'
fi

# MM2.db from each seednode is mustered on the stats-api.atomicdex.io server
rsync -avzP atomic@stats-api.atomicdex.io:/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db /home/${username}/defi_stats/api/db/source/seed7_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed1_MM2.db                        /home/${username}/defi_stats/api/db/source/seed1_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed2_MM2.db                        /home/${username}/defi_stats/api/db/source/seed2_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed3_MM2.db                        /home/${username}/defi_stats/api/db/source/seed3_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/viserion_MM2.db                     /home/${username}/defi_stats/api/db/source/viserion_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/rhaegal_MM2.db                      /home/${username}/defi_stats/api/db/source/rhaegal_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/drogon_MM2.db                       /home/${username}/defi_stats/api/db/source/drogon_MM2.db
