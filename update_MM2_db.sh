#!/bin/bash

username='hound'

rsync -avzP atomic@stats-api.atomicdex.io:/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db /home/${username}/defi_stats/DB/seed7_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed1_MM2.db /home/${username}/defi_stats/DB/seed1_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed2_MM2.db /home/${username}/defi_stats/DB/seed2_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed3_MM2.db /home/${username}/defi_stats/DB/seed3_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/viserion_MM2.db /home/${username}/defi_stats/DB/viserion_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/rhaegal_MM2.db /home/${username}/defi_stats/DB/rhaegal_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/drogon_MM2.db /home/${username}/defi_stats/DB/drogon_MM2.db
