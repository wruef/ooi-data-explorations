#!/usr/bin/env bash
#
# qartod_rca_phsen.sh
#
# Collect the phsen data from all of the OOI RCA platforms to
# calculate QARTOD test ranges and generate the different lookup values and
# tables.  Code modeled after C. Wingard workflow for Coastal Endurance
# Moorings.
#
# C. Wingard, 2021-06-17 -- Initial code *qartod_ce_phsen.sh*
# W. Ruef, 2021-09-14 -- Adapting original code for RCA platforms

# set the base directory python command for all subsequent processing
. $(dirname $CONDA_EXE)/../etc/profile.d/conda.sh
conda activate ooi
PYTHON="python -m ooi_data_explorations.qartod.rca.qartod_rca_phsen"

### CE02SHBP ###
$PYTHON -s CE02SHBP -n LJ01D -sn 10-PHSEND103 -co 2021-01-01T00:00:00 -p fixed

### CE04OSBP ###
$PYTHON -s CE04OSBP -n LJ01C -sn 10-PHSEND107 -co 2021-01-01T00:00:00 -p fixed

### CE04OSPS ###
$PYTHON -s CE04OSPS -n PC01B -sn 4B-PHSENA106 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s CE04OSPS -n SF01B -sn 2B-PHSENA108 -co 2021-01-01T00:00:00 -p profiler

### RS01SBPS ###
$PYTHON -s RS01SBPS -n PC01A -sn 4B-PHSENA102 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s RS01SBPS -n SF01A -sn 2D-PHSENA101 -co 2021-01-01T00:00:00 -p profiler

### RS03AXPS ###
$PYTHON -s RS03AXPS -n PC03A -sn 4B-PHSENA302 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s RS03AXPS -n SF03A -sn 2D-PHSENA301 -co 2021-01-01T00:00:00 -p profiler
