#!/bin/bash
#SBATCH -p k2-hipri
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --time=01:30:00
#SBATCH --mem=100G
#SBATCH -J utilisation_report
#SBATCH -o slurm-%j.out


# Before running this script, ensure that all required sacct logs are in JOBS_DIR and update REPORT_START and REPORT_END
# The command to generate a sacct log in the correct format is:
# sacct -a -P -X --starttime=2025-11-01 --endtime=2025-12-01 -o jobid,user,partition,submit,start,end,state,elapsedraw,nodelist,reqtres,alloctres > JobList_2025-11.txt

# --- Variables ---
JOBS_DIR="/mnt/scratch2/service-reporting/input_data/joblists/"
CAPACITIES_DIR="/mnt/scratch2/service-reporting/input_data/capacities"
REPORT_START="2025-05-01"
REPORT_END="2025-11-01"
VENV_PATH=".venv"

# --- Environment setup ---
module load apps/python3/3.12.4/gcc-14.1.0
source "$VENV_PATH/bin/activate"

# --- Run command ---
python3 main.py \
    --jobs-dir "$JOBS_DIR" \
    --capacities-dir "$CAPACITIES_DIR" \
    --report-start "$REPORT_START" \
    --report-end "$REPORT_END"
