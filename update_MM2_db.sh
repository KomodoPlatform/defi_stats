#!/bin/bash

username='hound'

rsync -avzP atomic@stats-api.atomicdex.io:/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db /home/${username}/defi_stats/DB/seed7_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed1_MM2.db /home/${username}/defi_stats/DB/seed1_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed2_MM2.db /home/${username}/defi_stats/DB/seed2_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/seed3_MM2.db /home/${username}/defi_stats/DB/seed3_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/streamseed1_MM2.db /home/${username}/defi_stats/DB/streamseed1_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/streamseed2_MM2.db /home/${username}/defi_stats/DB/streamseed2_MM2.db
rsync -avzP atomic@stats-api.atomicdex.io:/home/atomic/DB/streamseed3_MM2.db /home/${username}/defi_stats/DB/streamseed3_MM2.db
