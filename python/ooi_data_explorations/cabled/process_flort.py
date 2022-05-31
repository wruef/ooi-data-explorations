#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import os
import xarray as xr

from ooi_data_explorations.common import dt64_epoch
from ooi_data_explorations.uncabled.process_flort import ATTRS, quality_checks
from ooi_data_explorations.qartod.qc_processing import parse_qc

def flort_dropVars(ds):
    """
    Drop unused variables from dataset
    param ds: initial FLORT dataset
    return: reduced dataset
    """"
    # unused variables in streamed FLORT datastream:
    dropList = ['date_string', 'deployment', 'driver_timestamp', 'id',
        'ingestion_timestamp','internal_timestamp','port_timestamp',
        'prefrred_timestamp','provenance','time_string','measurement_wavelength_beta',
        'measurement_wavelength_cdom','measurement_wavelength_chl']

    ds.reset_coords()
    ds.drop(dropList)
    return ds

def flort_streamed(ds):
    """
    Takes FLORT data streamed from instruments deployed by the Regional Cabled
    Array and cleans up the data set to make it more user-friendly. Primary
    task is renaming parameters and dropping some that are of limited use.
    Additionally, re-organize some of the variables to permit better assessments
    of the data.

    :param ds: initial FLORT data set recorded by the data logger system and
        downloaded from OOI via the M2M system
    :return: cleaned up and reorganized data set
    """
    # lots of renaming here to get a better defined data set with cleaner attributes
    rename = {
        'raw_signal_chl': 'raw_chlorophyll',
        'fluorometric_chlorophyll_a': 'estimated_chlorophyll',
        'fluorometric_chlorophyll_a_qc_executed': 'estimated_chlorophyll_qc_executed',
        'fluorometric_chlorophyll_a_qc_results': 'estimated_chlorophyll_qc_results',
        'raw_signal_cdom': 'raw_cdom',
        'raw_signal_beta': 'raw_backscatter',
        'total_volume_scattering_coefficient': 'beta_700',
        'total_volume_scattering_coefficient_qc_executed': 'beta_700_qc_executed',
        'total_volume_scattering_coefficient_qc_results': 'beta_700_qc_results',
        'optical_backscatter': 'bback',
        'optical_backscatter_qc_executed': 'bback_qc_executed',
        'optical_backscatter_qc_results': 'bback_qc_results',
    }
    ds = ds.rename(rename)

    # reset some attributes
    for key, value in ATTRS.items():
        for atk, atv in value.items():
            if key in ds.variables:
                ds[key].attrs[atk] = atv

    # add the original variable name as an attribute, if renamed
    for key, value in rename.items():
        ds[value].attrs['ooinet_variable_name'] = key

    # check if the raw data for all three channels is 0, if so the FLORT wasn't talking to the CTD and these are
    # all just fill values that can be removed.
    ds = ds.where(ds['raw_backscatter'] + ds['raw_cdom'] + ds['raw_chlorophyll'] > 0, drop=True)
    if len(ds.time) == 0:
        # this was one of those deployments where the FLORT was never able to communicate with the CTD.
        warnings.warn('Communication failure between the FLORT and the CTDBP. No data was recorded.')
        return None

    # parse the OOI QC variables and add QARTOD style QC summary flags to the data, converting the
    # bitmap represented flags into an integer value representing pass == 1, suspect or of high
    # interest == 3, and fail == 4.
    ds = parse_qc(ds)

    # create qc flags for the data and add them to the OOI qc flags
    beta_flag, cdom_flag, chl_flag = quality_checks(ds)
    ds['beta_700_qc_summary_flag'] = ('time', (np.array([ds.beta_700_qc_summary_flag,
                                                         beta_flag])).max(axis=0, initial=1))
    ds['fluorometric_cdom_qc_summary_flag'] = ('time', (np.array([ds.fluorometric_cdom_qc_summary_flag,
                                                                 cdom_flag])).max(axis=0, initial=1))
    ds['estimated_chlorophyll_qc_summary_flag'] = ('time', (np.array([ds.estimated_chlorophyll_qc_summary_flag,
                                                                      chl_flag])).max(axis=0, initial=1))

    return ds

