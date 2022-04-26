#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import os
import xarray as xr

from ooi_data_explorations.common import dt64_epoch


def ctdbp_streamed(ds):
    """
    Takes CTD data streamed from instruments deployed by the Regional Cabled
    Array and cleans up the data set to make it more user-friendly. Primary
    task is renaming parameters and dropping some that are of limited use.

    :param ds: initial CTDBP data set recorded by the data logger system and
        downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # drop some of the variables:
    #   driver_timestamp == not used for data analysis
    #   ingestion_timestamp == not used for data analysis
    #   internal_timestamp == not used for data analysis
    #   port_timestamp == time, redundant so can remove
    ds = ds.reset_coords()
    ds = ds.drop(['driver_timestamp', 'ingestion_timestamp', 'internal_timestamp', 'port_timestamp'])

    # rename some of the variables for better clarity, two blocks to keep from stepping on ourselves
    rename = {
        'conductivity': 'raw_seawater_conductivity',
        'ctdbp_no_seawater_conductivity': 'seawater_conductivity',
        'ctdbp_no_seawater_conductivity_qc_executed': 'seawater_conductivity_qc_executed',
        'ctdbp_no_seawater_conductivity_qc_results': 'seawater_conductivity_qc_results',
        'ctdbp_no_seawater_conductivity_qartod_executed': 'seawater_conductivity_qartod_executed',
        'ctdbp_no_seawater_conductivity_qartod_results': 'seawater_conductivity_qartod_results',
        'temperature': 'raw_seawater_temperature',
        'pressure_temp': 'raw_pressure_temperature',
        'pressure': 'raw_seawater_pressure',
        'ctdbp_no_seawater_pressure': 'seawater_pressure',
        'ctdbp_no_seawater_pressure_qc_executed': 'seawater_pressure_qc_executed',
        'ctdbp_no_seawater_pressure_qc_results': 'seawater_pressure_qc_results',
        'ctdbp_no_seawater_pressure_qartod_executed': 'seawater_pressure_qartod_executed',
        'ctdbp_no_seawater_pressure_qartod_results': 'seawater_pressure_qartod_results',
    }
    ds = ds.rename(rename)
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    ds = setAttributes(ds)

    return ds



def ctdpf_ab_streamed(ds):
    """
    Takes CTD data streamed from instruments deployed by the Regional Cabled
    Array and cleans up the data set to make it more user-friendly. Primary
    task is renaming parameters and dropping some that are of limited use.

    :param ds: initial CTDPFA/CTDPFB data set recorded by the data logger system
        and downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # drop some of the variables:
    #   driver_timestamp == not used for data analysis
    #   ingestion_timestamp == not used for data analysis
    #   internal_timestamp == not used for data analysis
    #   port_timestamp == time, redundant so can remove
    ds = ds.reset_coords()
	
    keys = ['driver_timestamp', 'ingestion_timestamp', 'internal_timestamp', 'port_timestamp']
    for key in keys:
        if key in ds.variables:
            ds = ds.drop_vars(key)

     # rename some of the variables for better clarity, two blocks to keep from stepping on ourselves
    rename = {
        'conductivity': 'raw_seawater_conductivity',
        'temperature': 'raw_seawater_temperature',
        'pressure_temp': 'raw_pressure_temperature',
        'pressure': 'raw_seawater_pressure',
    }
    ds = ds.rename(rename)
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    ds = setAttributes(ds)

    return ds



def ctdpfl_recovered_inst(ds):
    """
    Takes CTD data streamed from deep profiler instruments deployed by 
    the Regional Cabled Array and cleans up the data set to make it 
    more user-friendly. Primary task is renaming parameters and dropping 
    some that are of limited use.

    :param ds: initial CTDPFL data set recorded by the data logger system and
        downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # drop some of the variables:
    #   driver_timestamp == not used for data analysis
    #   ingestion_timestamp == not used for data analysis
    #   internal_timestamp == not used for data analysis
    #   port_timestamp == time, redundant so can remove
    #   raw_time_microseconds == not used for data analysis
    #   raw_time_seconds == not used for data analysis
    #   conductivity_millisiemens_qc* == raw measurements, no QC tests should be run
    ds = ds.reset_coords()
    ds = ds.drop(['driver_timestamp', 'ingestion_timestamp', 'internal_timestamp', 'port_timestamp',
                  'raw_time_microseconds', 'raw_time_seconds', 'conductivity_millisiemens_qc_executed',
                  'conductivity_millisiemens_qc_results'])

     # rename some of the variables for better clarity, two blocks to keep from stepping on ourselves
    rename = {
        'conductivity_millisiemens': 'raw_seawater_conductivity',
        'dpc_seawater_conductivity': 'seawater_conductivity',
        'dpc_seawater_conductivity_qc_executed': 'seawater_conductivity_qc_executed',
        'dpc_seawater_conductivity_qc_results': 'seawater_conductivity_qc_results',
        'dpc_seawater_conductivity_qartod_executed': 'seawater_conductivity_qartod_executed',
        'dpc_seawater_conductivity_qartod_results': 'seawater_conductivity_qartod_results',
        'temp': 'seawater_temperature',
        'temp_qc_executed': 'seawater_temperature_qc_executed',
        'temp_qc_results': 'seawater_temperature_qc_results',
        'temp_qartod_executed': 'seawater_temperature_qartod_executed',
        'temp_qartod_results': 'seawater_temperature_qartod_results',
        'pressure': 'seawater_pressure',
        'pressure_qc_executed': 'seawater_pressure_qc_executed',
        'pressure_qc_results': 'seawater_pressure_qc_results',
        'pressure_qartod_executed': 'seawater_pressure_qartod_executed',
        'pressure_qartod_results': 'seawater_pressure_qartod_results',
    }
    ds = ds.rename(rename)
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    ds = setAttributes(ds)

    return ds

def setAttributes(ds):

    # correct incorrect units and attributes
    ds['seawater_temperature'].attrs['units'] = 'degree_Celsius'

    ds['raw_seawater_conductivity'].attrs['long_name'] = 'Raw Seawater Conductivity'
    ds['raw_seawater_conductivity'].attrs['comment'] = ('Raw seawater conductivity measurement recorded internally by '
                                                        'the instrument in counts')

    ds['raw_seawater_temperature'].attrs['long_name'] = 'Raw Seawater Temperature'
    ds['raw_seawater_temperature'].attrs['comment'] = ('Raw seawater temperature measurement recorded internally by '
                                                       'the instrument in counts')

    ds['raw_seawater_pressure'].attrs['long_name'] = 'Raw Seawater Pressure'
    ds['raw_seawater_pressure'].attrs['comment'] = ('Raw seawater pressure measurement recorded internally by the '
                                                    'instrument in counts')

    ds['raw_pressure_temperature'].attrs['long_name'] = 'Raw Seawater Pressure Sensor Temperature'
    ds['raw_pressure_temperature'].attrs['comment'] = ('Raw pressure sensor thermistor temperature, internal to the '
                                                       'sensor and recorded internally by the instrument in counts. '
                                                       'Used to convert the raw pressure measurement, compensating '
                                                       'for the sensor temperature, to pressure reported in dbar.')

    # ancillary_variables attribute set incorrectly (should be a space separated list) for certain variables
    ds['seawater_temperature'].attrs['ancillary_variables'] = ('raw_seawater_temperature '
                                                               'seawater_temperature_qc_executed '
                                                               'seawater_temperature_qc_results '
                                                               'seawater_temperature_qartod_executed '
                                                               'seawater_temperature_qartod_results')
    ds['seawater_conductivity'].attrs['ancillary_variables'] = ('raw_seawater_conductivity '
                                                                'seawater_conductivity_qc_executed '
                                                                'seawater_conductivity_qc_results '
                                                                'seawater_conductivity_qartod_executed '
                                                                'seawater_conductivity_qartod_results')
    ds['seawater_pressure'].attrs['ancillary_variables'] = ('raw_seawater_pressure raw_pressure_temperature '
                                                            'seawater_pressure_qc_executed '
                                                            'seawater_pressure_qc_results'
                                                            'seawater_pressure_qartod_executed '
                                                            'seawater_pressure_qartod_results')
    ds['practical_salinity'].attrs['ancillary_variables'] = ('seawater_conductivity seawater_temperature '
                                                             'seawater_pressure practical_salinity_qc_executed '
                                                             'practical_salinity_qc_results '
                                                             'practical_salinity_qartod_executed '
                                                             'practical_salinity_qartod_results')
    ds['density'].attrs['ancillary_variables'] = ('seawater_conductivity seawater_temperature seawater_pressure '
                                                  'lat lon density_qc_executed density_qc_results')

    return ds
