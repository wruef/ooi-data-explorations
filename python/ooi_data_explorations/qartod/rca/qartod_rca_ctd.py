#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author Christopher Wingard
@brief Load the CTD data from the RCA platforms and processes
    the data to generate QARTOD Gross Range and Climatology test limits.
    Code revised from *qartod_ce_phsen.py* to work with RCA CTD
    datasets.  W. Ruef, 2021-09-14.
"""
import argparse
import dateutil.parser as parser
import os
import pandas as pd
import pytz
import re
import sys
import xarray as xr
import numpy as np

##from ooi_data_explorations.common import get_annotations, load_gc_thredds, add_annotation_qc_flags
##from ooi_data_explorations.combine_data import combine_datasets
##from ooi_data_explorations.cabled.process_ctd import ctdbp_streamed, ctdpf_ab_streamed, ctdpfl_recovered_inst
from ooi_data_explorations.qartod.qc_processing import identify_blocks, create_annotations, process_gross_range, \
    process_climatology

def load_downloaded(site,node):
    filePath = os.path.join('/Users/rsn/ctd_data',site.lower() + '-' + node.lower())
    fileList = []
    for myfile in os.listdir(filePath):
        if myfile.endswith('_AA.nc'):
            fileList.append(os.path.join(filePath,myfile))
    
    fileList.sort()

    data = xr.open_dataset(fileList[0])
    for i in range(1,len(fileList)):
        print(fileList[i])
        ds_sub = xr.open_dataset(fileList[i])
        data = xr.concat([data, ds_sub], dim='time')
    
    data = data.sortby('time')

    return data

def inputs(argv=None):
    """
    Sets the main input arguments that will be used in the QC processing
    """
    if argv is None:
        argv = sys.argv[1:]

    # initialize argument parser
    parser = argparse.ArgumentParser(
        description="""Download and process instrument data to generate QARTOD lookup tables""")

    # assign input arguments.
    parser.add_argument("-s", "--site", dest="site", type=str, required=True)
    parser.add_argument("-n", "--node", dest="node", type=str, required=True)
    parser.add_argument("-sn", "--sensor", dest="sensor", type=str, required=True)
    parser.add_argument("-co", "--cut_off", dest="cut_off", type=str, required=False)
    parser.add_argument("-p", "--platform", dest="platform", type=str, required=True)

    # parse the input arguments and create a parser object
    args = parser.parse_args(argv)

    return args




def resampleCTD(ds, resample_time):
    
    itime = '{:d}Min'.format(resample_time)
    btime = int(resample_time / 2)
    loff = '{:d}Min'.format(btime)
    gtime = '{:d}Min'.format(resample_time * 3)    
    
    data = ds.resample(time=itime, base=btime, loffset=loff, skipna=True).median(keep_attrs=True)

    return data


def generate_qartod(site, node, sensor, cut_off, platform):
    """
    Load all of the pH data for a defined reference designator (using the site,
    node and sensor names to construct the reference designator) collected via
    the three data delivery methods of telemetered, recovered host and
    recovered instrument and combine them into a single data set from which
    QARTOD test limits for the gross range and climatology tests can be
    calculated.

    :param site: Site designator, extracted from the first part of the
        reference designator
    :param node: Node designator, extracted from the second part of the
        reference designator
    :param sensor: Sensor designator, extracted from the third and fourth part
        of the reference designator
    :param cut_off: string formatted date to use as cut-off for data to add
        to QARTOD test sets
    :return annotations: Initial list of auto-generated HITL annotations as
        a pandas dataframe
    :return gr_lookup: CSV formatted strings to save to a csv file for the
        QARTOD gross range lookup tables.
    :return clm_lookup: CSV formatted strings to save to a csv file for the
        QARTOD climatology lookup tables.
    :return clm_table: CSV formatted strings to save to a csv file for the
        QARTOD climatology range tables.
    """
    # parse instrument type
    instType = sensor.split('-')[1][0:6]

    # load and combine all of the data sources for the CTD sensor
    if 'CTDBP' in instType:
        dataRecord = 'ctdbp_no_sample'
        data = load_downloaded(site, node)
        origParam_temp = 'seawater_temperature'
    elif ('CTDPFA' in instType) & ('SF0' in node):
        dataRecord = 'ctdpf_sbe43_sample'
        data = load_downloaded(site, node)
        origParam_temp = 'seawater_temperature'
    elif (('CTDPFA' in instType) & ('PC0' in node)) | ('CTDPFB' in instType):
        dataRecord = 'ctdpf_optode_sample'
        data = load_downloaded(site, node)
        origParam_temp = 'seawater_temperature'
    elif 'CTDPFL' in instType:
       dataRecord = 'dpc_ctd_instrument_recovered'
       data = load_downloaded(site, node)
       origParam_temp = 'temp'
    
    if 'fixed' in platform:
        # resample the data into a 3 hour, median averaged time series
        # *** This step is skipped for profilers as we need to preserve the depth
        # bins for computing the climatology ***
        data = resampleCTD(data, 180)
    
    # if a cut_off date was used, limit data to all data collected up to the cut_off date.
    # otherwise, set the limit to the range of the downloaded data.
    if cut_off:
        cut = parser.parse(cut_off)
        cut = cut.astimezone(pytz.utc)
        end_date = cut.strftime('%Y-%m-%dT%H:%M:%S')
        src_date = cut.strftime('%Y-%m-%d')
    else:
        cut = parser.parse(data.time_coverage_end)
        cut = cut.astimezone(pytz.utc)
        end_date = cut.strftime('%Y-%m-%dT%H:%M:%S')
        src_date = cut.strftime('%Y-%m-%d')

    data = data.sortby('time')
    data = data.sel(time=slice("2014-01-01T00:00:00", end_date))

    # create the initial gross range entry
    gr_source = ('Sensor min/max based on the vendor standard calibration range. '
                           'The user min/max is the historical mean of all data collected '
                           'up to {} +/- 3 standard deviations.'.format(src_date))
    gr_lookup_temp = process_gross_range(data, ['seawater_temperature'], [-5.0, 35.0], site=site, node=node, sensor=sensor, stream=dataRecord, source=gr_source)
    gr_lookup_sal = process_gross_range(data, ['practical_salinity'], [0.0, 42.0], site=site, node=node, sensor=sensor, stream=dataRecord, source=gr_source)
    print(gr_lookup_temp)
    print(gr_lookup_sal)    

    # create and format the climatology entries and tables for temperature and salinity
    if 'fixed' in platform:
        clm_lookup_temp, clm_table_temp = process_climatology(data, ['seawater_temperature'], [-5.0, 35.0], site=site, node=node, sensor=sensor, stream=dataRecord)
        clm_lookup_temp['parameters'][0] = {'inp': origParam_temp, 'tinp': 'time', 'zinp': 'None'}
        clm_lookup_sal, clm_table_sal = process_climatology(data, ['practical_salinity'], [0.0, 42.0], site=site, node=node, sensor=sensor, stream=dataRecord)
        clm_lookup_sal['parameters'][0] = {'inp': 'practical_salinity', 'tinp': 'time', 'zinp': 'None'}

    elif 'profiler' in platform:
        clm_lookup_temp_bins = []
        clm_lookup_sal_bins = []
        clm_table_temp_bins = []
        clm_table_sal_bins = []
        clmHeader = ''
        for i in range(1,13):
            clmHeader += ',[{}, {}]'.format(i, i)
        clm_table_temp_bins.append(clmHeader + '\n')
        clm_table_sal_bins.append(clmHeader + '\n')
        ## climatology bins are 1 meter from 6 - 106, then every 5 meters below that 
        if 'SF0' in node:
            shallow_upper = np.arange(6,105,1) 
            shallow_lower = np.arange(105,200,5)
            binList = np.concatenate((shallow_upper,shallow_lower), axis=0).tolist()
        elif 'DP0' in node:
            maxDepth = {'DP01A': 2900, 'DP01B': 600, 'DP03A': 2600}
            binList = np.arange(200,maxDepth[node], 5).tolist()
        bins = []
        for i in range(0,len(binList)-1):
            bins.append((binList[i], binList[i+1]))
        for pressBin in bins:
            print('pressBin: ', pressBin)
            data_bin = data.where( (pressBin[0] < data['seawater_pressure']) & (data['seawater_pressure'] < pressBin[1]) )

            if (data_bin['seawater_temperature'].isnull()).all():
                print('no temperature data available for bin: ', pressBin)
            else:
                data_bin_filtered = data_bin.where( (-5.0 < data_bin['seawater_temperature']) & (data_bin['seawater_temperature'] < 35.0) )
                if (data_bin_filtered['seawater_temperature'].isnull()).all():
                    print('all tempterature values outside of sensor range for bin: ', pressBin)
                else: 
                    clm_lookup_temp, clm_table_temp = process_climatology(data_bin_filtered, ['seawater_temperature'], [-5.0, 35.0], site=site, node=node, sensor=sensor, stream=dataRecord)
                    clm_lookup_temp['parameters'][0] = {'inp': 'seawater_temperature', 'tinp': 'time', 'zinp': 'seawater_pressure'}
                    clm_lookup_temp_bins.append((pressBin, clm_lookup_temp))
                    clm_table_temp_line = clm_table_temp[0].splitlines()[1]
                    clm_table_temp_line = re.sub('^\[0, 0\]' , '[' + str(pressBin[0]) + ', ' + str(pressBin[1]) + ']', clm_table_temp_line)
                    clm_table_temp_bins.append(clm_table_temp_line + '\n')
                    print('data min: ', data_bin_filtered['seawater_temperature'].min())
                    print('data max: ', data_bin_filtered['seawater_temperature'].max())
                    print(clm_table_temp_line)
 
            if (data_bin['practical_salinity'].isnull()).all():
                print('no salinity data available for bin: ', pressBin)
            else:
                data_bin_filtered = data_bin.where( (0.0 < data_bin['practical_salinity']) & (data_bin['practical_salinity'] < 42.0) )
                if (data_bin_filtered['practical_salinity'].isnull()).all():
                    print('all salinity values outside of sensor range for bin: ', pressBin)
                else:
                    clm_lookup_sal, clm_table_sal = process_climatology(data_bin_filtered, ['practical_salinity'], [0.0, 42.0], site=site, node=node, sensor=sensor, stream=dataRecord)
                    clm_lookup_sal['parameters'][0] = {'inp': 'practical_salinity', 'tinp': 'time', 'zinp': 'seawater_pressure'}
                    clm_lookup_sal_bins.append((pressBin, clm_lookup_sal))
                    clm_table_sal_line = clm_table_sal[0].splitlines()[1]
                    clm_table_sal_line = re.sub('^\[0, 0\]' , '[' + str(pressBin[0]) + ', ' + str(pressBin[1]) + ']', clm_table_sal_line)
                    clm_table_sal_bins.append(clm_table_sal_line + '\n')
                    print('data min: ', data_bin_filtered['practical_salinity'].min())
                    print('data max: ', data_bin_filtered['practical_salinity'].max())
                    print(clm_table_sal_line)

        # convert climatology table format to string
        #clm_lookup_temp = []
        #clm_lookup_temp.append(''.join(map(str,clm_lookup_temp_bins)))
        clm_table_temp = []
        clm_table_temp.append(''.join(map(str,clm_table_temp_bins)))

        #clm_lookup_sal = []
        #clm_lookup_sal.append(''.join(map(str,clm_lookup_sal_bins)))
        clm_table_sal = []
        clm_table_sal.append(''.join(map(str,clm_table_sal_bins)))
        
    return gr_lookup_temp, gr_lookup_sal, clm_lookup_temp, clm_table_temp, clm_lookup_sal, clm_table_sal


def main(argv=None):
    """
    Download the CTD data from the Gold Copy THREDDS server and create the
    QARTOD gross range and climatology test lookup tables.
    """
    # setup the input arguments
    args = inputs(argv)
    site = args.site
    node = args.node
    sensor = args.sensor
    cut_off = args.cut_off
    platform = args.platform

    # create the QARTOD gross range and climatology lookup values, and
    # the climatology table for the seawater_temperature and practical_salinity parameters
    gr_lookup_temp, gr_lookup_sal, clm_lookup_temp, clm_table_temp, clm_lookup_sal, clm_table_sal = generate_qartod(site, node, sensor, cut_off, platform)

    # save the resulting annotations and qartod lookups and tables
    out_path = os.path.join(os.path.expanduser('~'), 'ooidata/qartod/ctd')
    out_path = os.path.abspath(out_path)
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # save the gross range values to a csv for further processing
    csv_names = ['subsite', 'node', 'sensor', 'stream', 'parameter', 'qcConfig', 'source']

    gr_csv_temp = '-'.join([site, node, sensor]) + '-seawater_temperature.gross_range.csv'
    gr_lookup_temp.to_csv(os.path.join(out_path, gr_csv_temp), index=False, columns=csv_names)
    gr_csv_sal = '-'.join([site, node, sensor]) + '-practical_salinity.gross_range.csv'
    gr_lookup_sal.to_csv(os.path.join(out_path, gr_csv_sal), index=False, columns=csv_names)

    # save the climatology values and table to a csv for further processing
    csv_names = ['subsite', 'node', 'sensor', 'stream', 'parameters', 'climatologyTable', 'source']

    clm_csv_temp = '-'.join([site, node, sensor]) + '-seawater_temperature.climatology.csv'
    clm_tbl_temp = '-'.join([site, node, sensor]) + '-seawater_temperature.csv'
    clm_lookup_temp.to_csv(os.path.join(out_path, clm_csv_temp), index=False, columns=csv_names)
    with open(os.path.join(out_path, clm_tbl_temp), 'w') as clm:
        clm.write(clm_table_temp[0])

    clm_csv_sal = '-'.join([site, node, sensor]) + '-practical_salinity.climatology.csv'
    clm_tbl_sal = '-'.join([site, node, sensor]) + '-practical_salinity.csv'
    clm_lookup_sal.to_csv(os.path.join(out_path, clm_csv_sal), index=False, columns=csv_names)
    with open(os.path.join(out_path, clm_tbl_sal), 'w') as clm:
        clm.write(clm_table_sal[0])


if __name__ == '__main__':
    main()
