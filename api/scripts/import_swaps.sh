#!/bin/bash
cd /home/atomic/defi_stats/api/scripts
/home/atomic/.local/bin/poetry run /home/atomic/defi_stats/api/scripts/import_swaps.py --start $(date -d "yesterday" +%Y-%m-%d) >> /home/atomic/logs/swaps-imports.log 2>&1


