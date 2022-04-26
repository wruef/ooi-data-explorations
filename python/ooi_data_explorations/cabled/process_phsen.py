#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import os
import xarray as xr

from ooi_data_explorations.common import dt64_epoch
from ooi_data_explorations.uncabled.process_phsen import ATTRS, quality_checks


def phsen_streamed(ds):
    """
    Takes PHSEN data streamed from instruments deployed by the Regional Cabled
    Array and cleans up the data set to make it more user-friendly. Primary
    task is renaming parameters and dropping some that are of limited use.
    Additionally, re-organize some of the variables to permit better assessments
    of the data.

    :param ds: initial PHSEN data set recorded by the data logger system and
        downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # drop some of the variables:
    #   checksum == not used
    #   record_type == not used
    #   record_length == not used
    #   signal_intensity_434, part of the light measurements array, redundant so can remove
    #   signal_intensity_578, part of the light measurements array, redundant so can remove
    ds = ds.reset_coords()
    ds = ds.drop(['checksum', 'record_type', 'record_length', 'signal_intensity_434',
                  'signal_intensity_578'])

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
    rename = {
        'voltage_battery': 'raw_battery_voltage',
        'thermistor_start': 'raw_thermistor_start',
        'thermistor_end': 'raw_thermistor_end',
        'phsen_thermistor_temperature': 'thermistor_temperature',
        'phsen_battery_volts': 'battery_voltage',
        'ph_seawater': 'seawater_ph',
        'ph_seawater_qc_executed': 'seawater_ph_qc_executed',
        'ph_seawater_qc_results': 'seawater_ph_qc_results'
    }
    ds = ds.rename(rename)

    # now we need to reset the light and reference arrays to named variables that will be more meaningful and useful in
    # the final data files
    nrec = len(ds['time'].values)
    light = np.array(np.vstack(ds['ph_light_measurements'].values), dtype='int32')
    light = np.atleast_3d(light)
    light = np.reshape(light, (nrec, 23, 4))  # 4 sets of 23 seawater measurements
    reference_434 = light[:, :, 0]            # reference signal, 434 nm
    signal_434 = light[:, :, 1]               # signal intensity, 434 nm (PH434SI_L0)
    reference_578 = light[:, :, 2]            # reference signal, 578 nm
    signal_578 = light[:, :, 3]               # signal intensity, 578 nm (PH578SI_L0)

    refnc = np.array(np.vstack(ds['reference_light_measurements'].values), dtype='int32')
    refnc = np.atleast_3d(refnc)
    refnc = np.reshape(refnc, (nrec, 4, 4))   # 4 sets of 4 DI water measurements (blanks)
    blank_refrnc_434 = refnc[:, :, 0]  # DI blank reference, 434 nm
    blank_signal_434 = refnc[:, :, 1]  # DI blank signal, 434 nm
    blank_refrnc_578 = refnc[:, :, 2]  # DI blank reference, 578 nm
    blank_signal_578 = refnc[:, :, 3]  # DI blank signal, 578 nm

    # create a data set with the reference and light measurements
    ph = xr.Dataset({
        'blank_refrnc_434': (['time', 'blanks'], blank_refrnc_434.astype('int32')),
        'blank_signal_434': (['time', 'blanks'], blank_signal_434.astype('int32')),
        'blank_refrnc_578': (['time', 'blanks'], blank_refrnc_578.astype('int32')),
        'blank_signal_578': (['time', 'blanks'], blank_signal_578.astype('int32')),
        'reference_434': (['time', 'measurements'], reference_434.astype('int32')),
        'signal_434': (['time', 'measurements'], signal_434.astype('int32')),
        'reference_578': (['time', 'measurements'], reference_578.astype('int32')),
        'signal_578': (['time', 'measurements'], signal_578.astype('int32'))
    }, coords={'time': ds['time'], 'measurements': np.arange(0, 23).astype('int32'),
               'blanks': np.arange(0, 4).astype('int32')
               })
    ds = ds.drop(['ph_light_measurements', 'reference_light_measurements',
                  'ph_light_measurements_dim_0', 'reference_light_measurements_dim_0'])

    # merge the data sets back together
    ds = ds.merge(ph)

    # test data quality
    ds['seawater_ph_quality_flag'] = quality_checks(ds)

    # reset some attributes
    for key, value in ATTRS.items():
        for atk, atv in value.items():
            ds[key].attrs[atk] = atv

    # add the original variable name as an attribute, if renamed
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    # and reset some of the data types
    data_types = ['deployment', 'raw_thermistor_end', 'raw_thermistor_start', 'unique_id', 'raw_battery_voltage']
    for v in data_types:
        ds[v] = ds[v].astype('int32')

    return ds
