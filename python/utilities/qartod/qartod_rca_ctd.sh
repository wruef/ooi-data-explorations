#!/usr/bin/env bash
#
# qartod_rca_ctd.sh
#
# Collect the CTD data from all of the OOI RCA platforms to
# calculate QARTOD test ranges and generate the different lookup values and
# tables.  Code modeled after C. Wingard workflow for Coastal Endurance
# Moorings.
#
# W. Ruef, 2021-09-15 -- Adapting original code for other sensors for RCA CTDs

# set the base directory python command for all subsequent processing
. $(dirname $CONDA_EXE)/../etc/profile.d/conda.sh
conda activate ooi
PYTHON="python -m ooi_data_explorations.qartod.rca.qartod_rca_ctd"

### CE04OSPS ###
#$PYTHON -s CE04OSPS -n PC01B -sn 4A-CTDPFA109 -co 2021-01-01T00:00:00 -p fixed
#$PYTHON -s CE04OSPS -n SF01B -sn 2A-CTDPFA107 -co 2021-01-01T00:00:00 -p profiler

### CE02SHBP ###
#$PYTHON -s CE02SHBP -n LJ01D -sn 06-CTDBPN106 -co 2021-01-01T00:00:00 -p fixed

### CE04OSBP ###
#$PYTHON -s CE04OSBP -n LJ01C -sn 06-CTDBPO108 -co 2021-01-01T00:00:00 -p fixed

### RS01SBPS ###
#$PYTHON -s RS01SBPS -n PC01A -sn 4A-CTDPFA103 -co 2021-01-01T00:00:00 -p fixed
#$PYTHON -s RS01SBPS -n SF01A -sn 2A-CTDPFA102 -co 2021-01-01T00:00:00 -p profiler

### RS03AXPS ###
#$PYTHON -s RS03AXPS -n PC03A -sn 4A-CTDPFA303 -co 2021-01-01T00:00:00 -p fixed
#$PYTHON -s RS03AXPS -n SF03A -sn 2A-CTDPFA302 -co 2021-01-01T00:00:00 -p profiler

### RS01SLBS ###
#$PYTHON -s RS01SLBS -n LJ01A -sn 12-CTDPFB101 -co 2021-01-01T00:00:00 -p fixed

### RS03ASHS ###
#$PYTHON -s RS03ASHS -n MJ03B -sn 10-CTDPFB304 -co 2021-01-01T00:00:00 -p fixed

### RS03AXBS ###
#$PYTHON -s RS03AXBS -n LJ03A -sn 12-CTDPFB301 -co 2021-01-01T00:00:00 -p fixed

### CE04OSPD ###
$PYTHON -s CE04OSPD -n DP01B -sn 01-CTDPFL105 -co 2021-01-01T00:00:00 -p profiler

### RS01SBPD ###
$PYTHON -s RS01SBPD -n DP01A -sn 01-CTDPFL104 -co 2021-01-01T00:00:00 -p profiler

### RS03AXPD ###
$PYTHON -s RS03AXPD -n DP03A -sn 01-CTDPFL304 -co 2021-01-01T00:00:00 -p profiler
