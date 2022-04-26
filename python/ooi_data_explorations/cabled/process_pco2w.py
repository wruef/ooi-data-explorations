#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import os
import xarray as xr

from ooi_data_explorations.common import dt64_epoch
from ooi_data_explorations.uncabled.process_pco2w import ATTRS, quality_checks


def pco2w_streamed(ds):
    """
    Takes PCO2W data streamed from instruments deployed by the Regional Cabled 
    Array and cleans up the data set to make it more user-friendly. Primary 
    task is renaming parameters and dropping some that are of limited use. 
    Additionally, re-organize some of the variables to permit better assessments 
    of the data.

    :param ds: initial PCO2W data set recorded by the data logger system and
        downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # drop some of the variables:
    #   checksum == not used
    #   record_type == not used
    #   record_time == internal_timestamp == time, redundant so can remove
    #   absorbance_ratio_*_qc_results == incorrectly set tests, ignoring
    #   absorbance_ratio_*_qc_executed == incorrectly set tests, ignoring
    ds = ds.reset_coords()
    ds = ds.drop(['checksum', 'record_type', 'internal_timestamp'])
    # Streamed pco2w-a does not always include 'absorbance_ratio_XXX_qc_results', so we must
    # check if it exists before dropping.  If '...qc_results...' exists for one wavelength,
    # we assume it exists for both, as well as '...qc_executed...'
    if 'absorbance_ratio_434_qc_results' in ds:
        ds = ds.drop(['absorbance_ratio_434_qc_results', 'absorbance_ratio_434_qc_executed',
                     'absorbance_ratio_620_qc_results', 'absorbance_ratio_620_qc_executed'])

    # convert the internal_timestamp values from a datetime64[ns] object to a floating point number with the time in
    # seconds, replacing the internal_timestamp with the record_time (the internal_timestamp is incorrectly set in the
    # NetCDF file).
    ds['internal_timestamp'] = ('time', dt64_epoch(ds.record_time))
    ds['internal_timestamp'].attrs = dict({
        'long_name': 'Internal SAMI-pH Clock Time',
        'standard_name': 'time',
        'units': 'seconds since 1970-01-01 00:00:00 0:00',
        'calendar': 'gregorian',
        'comment': ('Comparing the instrument internal clock versus the GPS referenced sampling time will allow for ' +
                    'calculations of the instrument clock offset and drift. Useful when working with the ' +
                    'recovered instrument data where no external GPS referenced clock is available.')
    })
    ds = ds.drop(['record_time'])

    # rename some of the variables for better clarity
    # in streamed pco2 there is a different name for absorbance blank depending on series
    # define absorbance blank name for renaming variable in dataset for clarity
    if 'pco2w_a_absorbance_blank_434' in ds:
        absBlank_434 = 'pco2w_a_absorbance_blank_434'
        absBlank_620 = 'pco2w_a_absorbance_blank_620'
    elif 'pco2w_b_absorbance_blank_434' in ds:
        absBlank_434 = 'pco2w_b_absorbance_blank_434'
        absBlank_620 = 'pco2w_b_absorbance_blank_620'

    rename = {
        'voltage_battery': 'raw_battery_voltage',
        'thermistor_raw': 'raw_thermistor',
        'pco2w_thermistor_temperature': 'thermistor_temperature',
        'pco2w_thermistor_temperature_qc_executed': 'thermistor_temperature_qc_executed',
        'pco2w_thermistor_temperature_qc_results': 'thermistor_temperature_qc_results',
        absBlank_434: 'absorbance_blank_434',
        absBlank_620: 'absorbance_blank_620',
    }
    ds = ds.rename(rename)

    # now we need to reset the light array to named variables that will be more meaningful and useful in
    # the final data files
    light = ds.light_measurements.astype('int32')
    dark_reference = light[:, [0, 8]].values    # dark reference
    dark_signal = light[:, [1, 9]].values       # dark signal
    reference_434 = light[:, [2, 10]].values    # reference signal, 434 nm
    signal_434 = light[:, [3, 11]].values       # signal intensity, 434 nm
    reference_620 = light[:, [4, 12]].values    # reference signal, 620 nm
    signal_620 = light[:, [5, 13]].values       # signal intensity, 620 nm

    # create a data set with the duplicate measurements for each variable
    data = xr.Dataset({
        'dark_reference': (['time', 'duplicates'], dark_reference),
        'dark_signal': (['time', 'duplicates'], dark_signal),
        'reference_434': (['time', 'duplicates'], reference_434),
        'signal_434': (['time', 'duplicates'], signal_434),
        'reference_620': (['time', 'duplicates'], reference_620),
        'signal_620': (['time', 'duplicates'], signal_620)
    }, coords={'time': ds['time'],  'duplicates': np.arange(0, 2).astype('int32')})
    ds = ds.drop(['spectrum', 'light_measurements'])

    # merge the data sets back together
    ds = ds.merge(data)

    # test the data quality
    ds['pco2_seawater_quality_flag'] = quality_checks(ds)

    # calculate the battery voltage
    ds['battery_voltage'] = ds['raw_battery_voltage'] * 15. / 4096.

    # reset some of the data types
    data_types = ['deployment', 'raw_thermistor', 'raw_battery_voltage',
                  'absorbance_blank_434', 'absorbance_blank_620', 'absorbance_ratio_434',
                  'absorbance_ratio_620']
    for v in data_types:
        ds[v] = ds[v].astype('int32')

    data_types = ['thermistor_temperature', 'pco2_seawater']
    for v in data_types:
        ds[v] = ds[v].astype('float32')

    # reset some attributes
    for key, value in ATTRS.items():
        for atk, atv in value.items():
            ds[key].attrs[atk] = atv

    # add the original variable name as an attribute, if renamed
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    return ds
