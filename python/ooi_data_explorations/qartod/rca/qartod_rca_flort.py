#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author Christopher Wingard
@brief Load the FLORT data from the RCA fixed platforms and profilers and
    process the data to generate QARTOD Gross Range and Climatology test limits.
    Code revised from *qartod_ce_flort.py* to work with RCA streamed datasets.
    W. Ruef, 2022-04
"""
import argparse
import dateutil.parser as parser
import os
import pandas as pd
import pytz
import re
import sys

from ooi_data_explorations.common import get_annotations, get_vocabulary, load_gc_thredds, add_annotation_qc_flags
from ooi_data_explorations.cabled.process_flort import flort_streamed
from ooi_data_explorations.qartod.qc_processing import identify_blocks, create_annotations, process_gross_range, \
    process_climatology, ANNO_HEADER, CLM_HEADER, GR_HEADER

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
    Load all FLORT data for a defined reference designator (using the site,
    node and sensor names to construct the reference designator) and
    collected via the different data delivery methods and combine them into a
    single data set from which QARTOD test limits for the gross range and
    climatology tests can be calculated.

    :param site: Site designator, extracted from the first part of the
        reference designator
    :param node: Node designator, extracted from the second part of the
        reference designator
    :param sensor: Sensor designator, extracted from the third and fourth part
        of the reference designator
    :param cut_off: string formatted date to use as cut-off for data to add
        to QARTOD test sets
    :return gr_lookup: CSV formatted strings to save to a csv file for the
        QARTOD gross range lookup tables.
    :return clm_lookup: CSV formatted strings to save to a csv file for the
        QARTOD climatology lookup tables.
    :return clm_table: CSV formatted strings to save to a csv file for the
        QARTOD climatology range tables.
    """
    # load and combine all of the data sources for the flort sensor
    data = load_gc_thredds(site, node, sensor, 'streamed', 'flort_d_data_record', '.*FLOR.*\\.nc$')
    data = flort_streamed(data)

    # create boolean arrays of the data marked as "fail" by the quality checks and generate initial
    # HITL annotations that can be combined with system annotations to create a cleaned up data set
    # prior to calculating the QARTOD test values

    chl_fail = data.estimated_chlorophyll_qc_summary_flag.where(data.estimated_chlorophyll_qc_summary_flag > 3).notnull()
    blocks = identify_blocks(chl_fail[::index], [18, 72])
    chl_hitl = create_annotations(site, node, sensor, blocks)
    chl_hitl['parameters'] = [[22, 1141] for i in chl_hitl['parameters']]

    cdom_fail = data.fluorometric_cdom_qc_summary_flag.where(data.fluorometric_cdom_qc_summary_flag > 3).notnull()
    blocks = identify_blocks(cdom_fail[::index], [18, 72])
    cdom_hitl = create_annotations(site, node, sensor, blocks)
    cdom_hitl['parameters'] = [[23, 1143] for i in cdom_hitl['parameters']]

    beta_fail = data.beta_700_qc_summary_flag.where(data.beta_700_qc_summary_flag > 3).notnull()
    blocks = identify_blocks(beta_fail[::index], [18, 72], 24)
    beta_hitl = create_annotations(site, node, sensor, blocks)
    beta_hitl['parameters'] = [[24, 25, 1139] for i in beta_hitl['parameters']]

    # combine the different dictionaries into a single HITL annotation dictionary for later use
    hitl = chl_hitl.copy()
    for d in (cdom_hitl, beta_hitl):
        for key, value in d.items():
            hitl[key] = hitl[key] + d[key]

    # get the current system annotations for the sensor
    annotations = get_annotations(site, node, sensor)
    annotations = pd.DataFrame(annotations)
    if not annotations.empty:
        annotations = annotations.drop(columns=['@class'])
        annotations['beginDate'] = pd.to_datetime(annotations.beginDT, unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')
        annotations['endDate'] = pd.to_datetime(annotations.endDT, unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')

    # append the fail annotations to the existing annotations
    annotations = annotations.append(pd.DataFrame(hitl), ignore_index=True, sort=False)

    # create an annotation-based quality flag
    data = add_annotation_qc_flags(data, annotations)

    # clean-up the data, NaN-ing values that were marked as fail in the QC checks and/or identified as a block
    # of failed data, and then removing all records where the rollup annotation (every parameter fails) was
    # set to fail.
    data['estimated_chlorophyll'][chl_fail] = np.nan
    if 'fluorometric_chl_a_annotations_qc_results' in data.variables:
        m = data.fluorometric_chl_a_annotations_qc_results == 4
        data['estimated_chlorophyll'][m] = np.nan

    data['fluorometric_cdom'][cdom_fail] = np.nan
    if 'fluorometric_cdom_annotations_qc_results' in data.variables:
        m = data.fluorometric_cdom_annotations_qc_results == 4
        data['fluorometric_cdom'][m] = np.nan

    data['beta_700'][beta_fail] = np.nan
    if 'total_volume_scattering_coefficient_annotations_qc_results' in data.variables:
        m = data.total_volume_scattering_coefficient_annotations_qc_results == 4
        data['beta_700'][m] = np.nan
        data['bback'][m] = np.nan

    if 'rollup_annotations_qc_results' in data.variables:
        data = data.where(data.rollup_annotations_qc_results < 4)

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

    data = data.sel(time=slice('2014-01-01T00:00:00', end_date))

    # set the parameters and the gross range limits
    parameters = ['bback', 'estimated_chlorophyll', 'fluorometric_cdom']
    limits = [[0, 3], [0, 30], [0, 375]]

    # create the initial gross range entry
    gr_lookup = process_gross_range(data, parameters, limits, site=site,
                                    node=node, sensor=sensor, stream='flort_sample')

    # add the stream name and the source comment
    gr_lookup['notes'] = ('User range based on data collected through {}.'.format(src_date))

    # based on the site and node, determine if we need a depth based climatology
    depth_bins = np.array([])
    # create and format the climatology entry and table
    if 'profiler' in platform:
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


    # create and format the climatology lookups and tables for the data
    clm_lookup, clm_table = process_climatology(data, parameters, limits, depth_bins=depth_bins,
                                                site=site, node=node, sensor=sensor, stream='flort_sample')

    # add the stream name
    clm_lookup['stream'] = 'flort_sample'

    return annotations, gr_lookup, clm_lookup, clm_table


def main(argv=None):
    """
    Download the FLORT data from the Gold Copy THREDDS server and create the
    QARTOD gross range and climatology test lookup tables.
    """
    # setup the input arguments
    args = inputs(argv)
    site = args.site
    node = args.node
    sensor = args.sensor
    cut_off = args.cut_off
    platform = args.platform

    # create the QARTOD gross range and climatology lookup values and tables
    annotations, gr_lookup, clm_lookup, clm_table = generate_qartod(site, node, sensor, cut_off, platform)

    # save the downloaded annotations and qartod lookups and tables
    out_path = os.path.join(os.path.expanduser('~'), 'ooidata/qartod/flort')
    out_path = os.path.abspath(out_path)
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # save the annotations to a csv file for further processing
    anno_csv = '-'.join([site, node, sensor]) + '.quality_annotations.csv'
    annotations.to_csv(os.path.join(out_path, anno_csv), index=False, columns=ANNO_HEADER)

    # save the gross range values to a csv for further processing
    gr_csv = '-'.join([site, node, sensor]) + '.gross_range.csv'
    gr_lookup.to_csv(os.path.join(out_path, gr_csv), index=False, columns=GR_HEADER)

    # save the climatology values and table to a csv for further processing
    clm_csv = '-'.join([site, node, sensor]) + '.climatology.csv'
    clm_lookup.to_csv(os.path.join(out_path, clm_csv), index=False, columns=CLM_HEADER)
    parameters = ['bback', 'estimated_chlorophyll', 'fluorometric_cdom']
    for i in range(len(parameters)):
        tbl = '-'.join([site, node, sensor, parameters[i]]) + '.csv'
        with open(os.path.join(out_path, tbl), 'w') as clm:
            clm.write(clm_table[i])


if __name__ == '__main__':
    main()
