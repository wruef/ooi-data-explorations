#!/usr/bin/env bash
#
# qartod_rca_phsen.sh
#
# Collect the flort data from all of the OOI RCA platforms to
# calculate QARTOD test ranges and generate the different lookup values and
# tables.  Code modeled after C. Wingard workflow for Coastal Endurance
# Moorings.
#
# W. Ruef, 2022-04 -- Adapting original code for RCA platforms

# set the base directory python command for all subsequent processing
. $(dirname $CONDA_EXE)/../etc/profile.d/conda.sh
conda activate ooi
PYTHON="python -m ooi_data_explorations.qartod.rca.qartod_rca_flort"

### CE04OSPS ###
$PYTHON -s CE04OSPS -n SF01B -sn 3A-FLORTD104 -co 2021-01-01T00:00:00 -p profiler

### RS01SBPS ###
$PYTHON -s RS01SBPS -n PC01A -sn 4C-FLORDD103 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s RS01SBPS -n SF01A -sn 3A-FLORTD101 -co 2021-01-01T00:00:00 -p profiler

### RS03AXPS ###
$PYTHON -s RS03AXPS -n PC03A -sn 4C-FLORDD303 -co 2021-01-01T00:00:00 -p fixed
$PYTHON -s RS03AXPS -n SF03A -sn 3A-FLORTD301 -co 2021-01-01T00:00:00 -p profiler
