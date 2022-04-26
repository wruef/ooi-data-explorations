#!/usr/bin/env bash
#
# qartod_rca_pco2w.sh
#
# Collect the PCO2W data from all of the OOI RCA platforms to
# calculate QARTOD test ranges and generate the different lookup values and
# tables.  Code modeled after C. Wingard workflow for Coastal Endurance
# Moorings.
#
# C. Wingard, 2021-06-17 -- Initial code *qartod_ce_pco2w.sh*
# W. Ruef, 2021-09-15 -- Adapting original code for RCA platforms

# set the base directory python command for all subsequent processing
. $(dirname $CONDA_EXE)/../etc/profile.d/conda.sh
conda activate ooi
PYTHON="python -m ooi_data_explorations.qartod.rca.qartod_rca_pco2w"

### CE04OSPS ###
$PYTHON -s CE04OSPS -n PC01B -sn 4D-PCO2WA105 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s CE04OSPS -n SF01B -sn 4F-PCO2WA102 -co 2021-01-01T00:00:00 -p profiler

### CE02SHBP ###
$PYTHON -s CE02SHBP -n LJ01D -sn 09-PCO2WB103 -co 2021-01-01T00:00:00 -p fixed

### CE04OSBP ###
$PYTHON -s CE04OSBP -n LJ01C -sn 09-PCO2WB104 -co 2021-01-01T00:00:00 -p fixed

### RS01SBPS ###
$PYTHON -s RS01SBPS -n SF01A -sn 4F-PCO2WA101 -co 2021-01-01T00:00:00 -p profiler

### RS03AXPS ###
$PYTHON -s RS03AXPS -n SF03A -sn 4F-PCO2WA301 -co 2021-01-01T00:00:00 -p profiler

