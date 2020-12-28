PATH_TO_PYTHON="/home/database/anaconda3/envs/goldenswan/bin/python"
PATH_TO_GOLDENSWAN="/home/database/projects/goldenswan"

LAUNCH_SCRIPT="${PATH_TO_GOLDENSWAN}/utilities/script_launcher.py"
UPDATE_TABLES_SCRIPT="${PATH_TO_GOLDENSWAN}/db_creation/db_update/fill_tables_alphavantage.py"


$PATH_TO_PYTHON $LAUNCH_SCRIPT $UPDATE_TABLES_SCRIPT "intraday_60"
