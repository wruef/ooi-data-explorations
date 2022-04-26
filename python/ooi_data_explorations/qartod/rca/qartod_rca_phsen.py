#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author Christopher Wingard
@brief Load the PHSEN data from the RCA fixed platforms and processes
    the data to generate QARTOD Gross Range and Climatology test limits.
    Code revised from *qartod_ce_phsen.py* to work with RCA streamed
    datasets.  W. Ruef, 2021-09-14.
"""
import argparse
import dateutil.parser as parser
import os
import pandas as pd
import pytz
import re
import sys

from ooi_data_explorations.common import get_annotations, load_gc_thredds, add_annotation_qc_flags
from ooi_data_explorations.combine_data import combine_datasets
from ooi_data_explorations.cabled.process_phsen import phsen_streamed, quality_checks
from ooi_data_explorations.qartod.qc_processing import identify_blocks, create_annotations, process_gross_range, \
    process_climatology

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
    # load and combine all of the data sources for the pH sensor
    data = load_gc_thredds(site, node, sensor, 'streamed', 'phsen_data_record', '.*PHSEN.*\\.nc$')
    data = phsen_streamed(data)
    
    if 'fixed' in platform:
        # resample the data into a 3 hour, median averaged time series
        # *** This step is skipped for profilers as we need to preserve the depth
        # bins for computing the climatology ***
        data = combine_datasets(data, None, None, 180)
    
    # recalculate the quality flags as averaging will alter them
    data['seawater_ph_quality_flag'] = quality_checks(data)

    # create a boolean array of the data marked as "fail" by the pH quality checks and generate initial
    # HITL annotations that can be combined with system annotations and pH quality checks to create
    # a cleaned up data set prior to calculating the QARTOD test values
    fail = data.seawater_ph_quality_flag.where(data.seawater_ph_quality_flag == 4).notnull()
    blocks = identify_blocks(fail, [24, 24])
    hitl = create_annotations(site, node, sensor, blocks)

    # get the current system annotations for the sensor
    annotations = get_annotations(site, node, sensor)
    annotations = pd.DataFrame(annotations)
    if not annotations.empty:
        annotations = annotations.drop(columns=['@class'])
        annotations['beginDate'] = pd.to_datetime(annotations.beginDT, unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')
        annotations['endDate'] = pd.to_datetime(annotations.endDT, unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')

    # append the fail annotations to the existing annotations
    annotations = annotations.append(pd.DataFrame(hitl), ignore_index=True, sort=False)

    # create a roll-up annotation flag
    data = add_annotation_qc_flags(data, annotations)

    # clean-up the data, removing values that fail the pH quality checks or were marked as fail in the annotations
    data = data.where((data.seawater_ph_quality_flag != 4) & (data.rollup_annotations_qc_results != 4))

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
    gr_lookup = process_gross_range(data, ['seawater_ph'], [6.9, 9.0], site=site, node=node, sensor=sensor, stream='phsen_data_record', source=gr_source)

    # create and format the climatology entry and table
    if 'fixed' in platform:
        clm_lookup, clm_table = process_climatology(data, ['seawater_ph'], [6.9, 9.0], site=site, node=node, sensor=sensor, stream='phsen_data_record')
        clm_lookup['parameters'][0] = {'inp': 'ph_seawater', 'tinp': 'time', 'zinp': 'None'}
    elif 'profiler' in platform:
        clm_table_bins = []
        clmHeader = ''
        for i in range(1,13):
            clmHeader += ',[{}, {}]'.format(i, i)
        clm_table_bins.append(clmHeader + '\n')
        ## pH is taken only on the stepped downcast twice a day...climatology bins must match the stepped pressure stops for the downcast
        bins = [(15,25),(25,35),(35,45),(45,55),(55,65),(65,75),(75,85),(85,95),(95,105),(105,115),(115,150),(150,195)]
        for pressBin in bins:
            data_bin = data.where( (pressBin[0] < data['int_ctd_pressure']) & (data['int_ctd_pressure'] < pressBin[1]) )
            clm_lookup, clm_table = process_climatology(data_bin, ['seawater_ph'], [6.9, 9.0], site=site, node=node, sensor=sensor, stream='phsen_data_record')
            clm_table_line = clm_table[0].splitlines()[1]
            clm_table_line = re.sub('^\[0, 0\]' , '[' + str(pressBin[0]) + ', ' + str(pressBin[1]) + ']', clm_table_line)
            clm_table_bins.append(clm_table_line + '\n') 
        
        # convert climatology table format to string
        clm_table = []
        clm_table.append(''.join(map(str,clm_table_bins)))
        clm_lookup['parameters'][0] = {'inp': 'ph_seawater', 'tinp': 'time', 'zinp': 'int_ctd_pressure'}
        
    return annotations, gr_lookup, clm_lookup, clm_table


def main(argv=None):
    """
    Download the PHSEN data from the Gold Copy THREDDS server and create the
    QARTOD gross range and climatology test lookup tables.
    """
    # setup the input arguments
    args = inputs(argv)
    site = args.site
    node = args.node
    sensor = args.sensor
    cut_off = args.cut_off
    platform = args.platform

    # create the initial HITL annotation blocks, the QARTOD gross range and climatology lookup values, and
    # the climatology table for the seawater_ph parameter
    annotations, gr_lookup, clm_lookup, clm_table = generate_qartod(site, node, sensor, cut_off, platform)

    # save the resulting annotations and qartod lookups and tables
    out_path = os.path.join(os.path.expanduser('~'), 'ooidata/qartod/phsen')
    out_path = os.path.abspath(out_path)
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # save the annotations to a csv file for further processing
    csv_names = ['id', 'subsite', 'node', 'sensor', 'method', 'stream', 'parameters',
                 'beginDate', 'endDate', 'exclusionFlag', 'qcFlag', 'source', 'annotation']
    anno_csv = '-'.join([site, node, sensor]) + '.quality_annotations.csv'
    annotations.to_csv(os.path.join(out_path, anno_csv), index=False, columns=csv_names)

    # save the gross range values to a csv for further processing
    csv_names = ['subsite', 'node', 'sensor', 'stream', 'parameter', 'qcConfig', 'source']
    gr_csv = '-'.join([site, node, sensor]) + '.gross_range.csv'
    gr_lookup.to_csv(os.path.join(out_path, gr_csv), index=False, columns=csv_names)

    # save the climatology values and table to a csv for further processing
    csv_names = ['subsite', 'node', 'sensor', 'stream', 'parameters', 'climatologyTable', 'source']
    clm_csv = '-'.join([site, node, sensor]) + '.climatology.csv'
    clm_tbl = '-'.join([site, node, sensor]) + '-seawater_ph.csv'
    clm_lookup.to_csv(os.path.join(out_path, clm_csv), index=False, columns=csv_names)
    with open(os.path.join(out_path, clm_tbl), 'w') as clm:
        clm.write(clm_table[0])


if __name__ == '__main__':
    main()
