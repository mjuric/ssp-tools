# ***** GENERATED FILE, DO NOT EDIT BY HAND *****
# ruff: noqa: W505
# generated with .venv/bin/ssp-generate-dtypes tests/data/sdm_schemas/sso_base.yaml SSSource NearbySSO # noqa: E501

import numpy as np

# SSSource: Solar System source measurements: one row per Rubin observation submitted to and accepted by the
# MPC (obs_sbn), with the measurement it was submitted from (a DiaSource or a Source, see measuredOn) and its
# ephemeris from mpc_orbits. Built from the record of submitted measurements, independent of the PPDB
# DiaSource table (RFC-1188).
SSSourceDtype = np.dtype([
    ('obsid', '<U32'),              # MPC's unique identifier of the observation (obs_sbn.obsid): the obs_sbn
                                    # row this SSSource row is. The primary key.
    ('trksub', '<U8'),              # Observer-assigned tracklet identifier, as submitted (obs_sbn.trksub).
    ('trkid', '<U16'),              # MPC-assigned tracklet identifier (obs_sbn.trkid).
    ('submission_id', '<U32'),      # Identifier of the MPC submission this observation arrived in
                                    # (obs_sbn.submission_id).
    ('status', '<U1'),              # MPC status of the observation (obs_sbn.status). 'I' marks observations
                                    # in the Isolated Tracklet File: accepted as valid, but not identified...
    ('primary', '|b1'),             # True on exactly one row per measurement (processing,
                                    # diaSourceId/sourceId): the '-A' row of a trail pair, else the row fr...
    ('matchMethod', '<U16'),        # How the measurement this observation was submitted from was found:
                                    # 'obssubid' (its obsSubID, LSST-<processing>-<id> or a bare id, looke...
    ('ssObjectId', '<i8'),          # Unique LSST identifier of the Solar System object
                                    # (SSObject.ssObjectId). NULL when this observation is not identified...
    ('designation', '<U16'),        # The unpacked primary provisional designation of the object the MPC
                                    # identified this observation with; NULL if none.
    ('measuredOn', '<U16'),         # Kind of image the row was measured on: 'difference' for DiaSource rows,
                                    # 'science' for Source rows.
    ('processing', '<U16'),         # Processing label, e.g. AP-DS, pDP2-DS or NV-S: which (re)processing the
                                    # row belongs to. (processing, id) is the key, and obsSubID is...
    ('processingTable', '<U32'),    # Fully qualified ssp table the row came from, e.g.
                                    # ssp.dia_source_ppdb_rel2; provenance runs from there to the table's...
    ('diaSourceId', '<i8'),         # Identifier of the DiaSource this observation was measured as
                                    # (DiaSource.diaSourceId in its processing); NULL on Source rows...
    ('sourceId', '<i8'),            # Identifier of the Source this observation was measured as
                                    # (Source.sourceId in its processing); NULL on DiaSource rows (measure...
    ('parentDiaSourceId', '<i8'),   # Id of the parent DiaSource this one was deblended from, if any; NULL on
                                    # Source rows.
    ('parentSourceId', '<i8'),      # Id of the parent Source this one was deblended from, if any; NULL on
                                    # DiaSource rows.
    ('visit', '<i8'),               # Visit id. DiaSource visit; Source visit.
    ('detector', '<i2'),            # Detector id. DiaSource detector; Source detector.
    ('midpointMjdTai', '<f8'),      # [d] Exposure midpoint, TAI MJD. DiaSource midpointMjdTai; on Source
                                    # rows the visit's exposure midpoint (the export's mjd column). Named...
    ('exposureTime', '<f4'),        # [s] Measured exposure (shutter-open) time of the visit, s: ConsDB
                                    # exposure.shut_time (= Butler visitInfo.exposureTime), looked up by...
    ('ra', '<f8'),                  # [deg] Right ascension of the centroid, degrees. DiaSource ra; Source ra
                                    # (AP-S: coord_ra).
    ('raErr', '<f4'),               # [deg] Uncertainty of ra (of RA*cos(Dec)), degrees. DiaSource raErr;
                                    # Source raErr (AP-S: coord_raErr).
    ('dec', '<f8'),                 # [deg] Declination of the centroid, degrees. DiaSource dec; Source dec
                                    # (AP-S: coord_dec).
    ('decErr', '<f4'),              # [deg] Uncertainty of dec, degrees. DiaSource decErr; Source decErr
                                    # (AP-S: coord_decErr).
    ('ra_dec_Cov', '<f4'),          # [deg**2] Covariance of ra (RA*cos(Dec)) and dec, degrees^2. DiaSource
                                    # ra_dec_Cov; Source ra_dec_Cov (AP-S: coord_ra_dec_Cov). NULL on 001-DS.
    ('x', '<f4'),                   # [pixel] Centroid x on the detector, pixels. DiaSource x; Source x
                                    # (AP-S: slot_Centroid_x).
    ('xErr', '<f4'),                # [pixel] Uncertainty of x, pixels. DiaSource xErr; Source xErr (AP-S:
                                    # slot_Centroid_xErr).
    ('y', '<f4'),                   # [pixel] Centroid y on the detector, pixels. DiaSource y; Source y
                                    # (AP-S: slot_Centroid_y).
    ('yErr', '<f4'),                # [pixel] Uncertainty of y, pixels. DiaSource yErr; Source yErr (AP-S:
                                    # slot_Centroid_yErr).
    ('centroid_flag', '|b1'),       # General centroid failure flag. DiaSource centroid_flag; Source
                                    # centroid_flag (AP-S: slot_Centroid_flag).
    ('apFlux', '<f4'),              # [nJy] Flux in the 12 pixel (2.4 arcsec) radius circular aperture, nJy,
                                    # not aperture-corrected. DiaSource apFlux (difference image); Source...
    ('apFluxErr', '<f4'),           # [nJy] Uncertainty of apFlux, nJy. DiaSource apFluxErr; Source
                                    # ap12FluxErr (AP-S: calibrated in full quadrature with...
    ('apMag', '<f4'),               # [mag] AB magnitude of apFlux: 31.4 - 2.5*log10(apFlux/nJy), NULL when
                                    # apFlux <= 0; computed on read, never stored. DiaSource (ppdb) apMag;...
    ('apMagErr', '<f4'),            # [mag] Uncertainty of apMag: 1.0857*apFluxErr/apFlux, NULL when apFlux
                                    # <= 0; computed on read. DiaSource (ppdb) apMagErr; Source ap12MagErr.
    ('apFlux_flag', '|b1'),         # Failure flag of the 12 px aperture flux. DiaSource apFlux_flag; Source
                                    # ap12Flux_flag (AP-S: base_CircularApertureFlux_12_0_flag).
    ('apFlux_flag_apertureTruncated', '|b1'), # The 12 px aperture did not fit within the image. DiaSource
                                              # apFlux_flag_apertureTruncated; Source...
    ('ap03Flux', '<f4'),            # [nJy] Flux in the 3 pixel (0.6 arcsec) radius circular aperture, nJy,
                                    # not aperture-corrected. Source ap03Flux (AP-S:...
    ('ap03FluxErr', '<f4'),         # [nJy] Uncertainty of ap03Flux, nJy. Source ap03FluxErr (AP-S:
                                    # calibrated in full quadrature with base_LocalPhotoCalibErr)....
    ('ap03Mag', '<f4'),             # [mag] AB magnitude of ap03Flux: 31.4 - 2.5*log10(ap03Flux/nJy), NULL
                                    # when ap03Flux <= 0; computed on read, never stored. Source ap03Mag....
    ('ap03MagErr', '<f4'),          # [mag] Uncertainty of ap03Mag: 1.0857*ap03FluxErr/ap03Flux, NULL when
                                    # ap03Flux <= 0; computed on read. Source ap03MagErr. Source-only extr...
    ('ap03Flux_flag', '|b1'),       # Failure flag of the 3 px aperture flux. Source ap03Flux_flag (AP-S:
                                    # base_CircularApertureFlux_3_0_flag). Source-only extra: DiaSource...
    ('ap03Flux_flag_apertureTruncated', '|b1'), # The 3 px aperture did not fit within the image. AP-S
                                                # base_CircularApertureFlux_3_0_flag_apertureTruncated; NU...
    ('ap06Flux', '<f4'),            # [nJy] Flux in the 6 pixel (1.2 arcsec) radius circular aperture, nJy,
                                    # not aperture-corrected. Source ap06Flux (AP-S:...
    ('ap06FluxErr', '<f4'),         # [nJy] Uncertainty of ap06Flux, nJy. Source ap06FluxErr (AP-S:
                                    # calibrated in full quadrature with base_LocalPhotoCalibErr)....
    ('ap06Mag', '<f4'),             # [mag] AB magnitude of ap06Flux: 31.4 - 2.5*log10(ap06Flux/nJy), NULL
                                    # when ap06Flux <= 0; computed on read, never stored. Source ap06Mag....
    ('ap06MagErr', '<f4'),          # [mag] Uncertainty of ap06Mag: 1.0857*ap06FluxErr/ap06Flux, NULL when
                                    # ap06Flux <= 0; computed on read. Source ap06MagErr. Source-only extr...
    ('ap06Flux_flag', '|b1'),       # Failure flag of the 6 px aperture flux. Source ap06Flux_flag (AP-S:
                                    # base_CircularApertureFlux_6_0_flag). Source-only extra: DiaSource...
    ('ap06Flux_flag_apertureTruncated', '|b1'), # The 6 px aperture did not fit within the image. AP-S
                                                # base_CircularApertureFlux_6_0_flag_apertureTruncated; NU...
    ('ap25Flux', '<f4'),            # [nJy] Flux in the 25 pixel (5.0 arcsec) radius circular aperture, nJy,
                                    # not aperture-corrected. Source ap25Flux (AP-S:...
    ('ap25FluxErr', '<f4'),         # [nJy] Uncertainty of ap25Flux, nJy. Source ap25FluxErr (AP-S:
                                    # calibrated in full quadrature with base_LocalPhotoCalibErr)....
    ('ap25Mag', '<f4'),             # [mag] AB magnitude of ap25Flux: 31.4 - 2.5*log10(ap25Flux/nJy), NULL
                                    # when ap25Flux <= 0; computed on read, never stored. Source ap25Mag....
    ('ap25MagErr', '<f4'),          # [mag] Uncertainty of ap25Mag: 1.0857*ap25FluxErr/ap25Flux, NULL when
                                    # ap25Flux <= 0; computed on read. Source ap25MagErr. Source-only extr...
    ('ap25Flux_flag', '|b1'),       # Failure flag of the 25 px aperture flux. Source ap25Flux_flag (AP-S:
                                    # base_CircularApertureFlux_25_0_flag). Source-only extra: DiaSource...
    ('ap25Flux_flag_apertureTruncated', '|b1'), # The 25 px aperture did not fit within the image. AP-S
                                                # base_CircularApertureFlux_25_0_flag_apertureTruncated; N...
    ('isNegative', '|b1'),          # Detected as significantly negative on the difference image. DiaSource
                                    # isNegative; no Source equivalent (NULL on Source rows).
    ('snr', '<f4'),                 # Signal-to-noise ratio of the detection on the difference image.
                                    # DiaSource snr; no Source equivalent.
    ('psfFlux', '<f4'),             # [nJy] PSF-model flux, nJy. DiaSource psfFlux (on the difference image,
                                    # the visit-minus-template flux); Source psfFlux (AP-S:...
    ('psfFluxErr', '<f4'),          # [nJy] Uncertainty of psfFlux, nJy. DiaSource psfFluxErr; Source
                                    # psfFluxErr (AP-S: calibrated in full quadrature with...
    ('psfMag', '<f4'),              # [mag] AB magnitude of psfFlux: 31.4 - 2.5*log10(psfFlux/nJy), NULL when
                                    # psfFlux <= 0; computed on read, never stored. DiaSource (ppdb) psfMa...
    ('psfMagErr', '<f4'),           # [mag] Uncertainty of psfMag: 1.0857*psfFluxErr/psfFlux, NULL when
                                    # psfFlux <= 0; computed on read. DiaSource (ppdb) psfMagErr; Source...
    ('psfLnL', '<f4'),              # Natural log-likelihood of the PSF-model fit. DiaSource psfLnL (prompt
                                    # and PPDB tables only); no Source equivalent.
    ('psfChi2', '<f4'),             # Chi^2 of the PSF-model fit. DiaSource psfChi2; AP-S base_PsfFlux_chi2;
                                    # NULL on NV-S and DP2-S.
    ('psfNdata', '<i4'),            # Number of pixels used in the PSF-model fit. DiaSource psfNdata; AP-S
                                    # base_PsfFlux_npixels; NULL on NV-S and DP2-S.
    ('psfFlux_flag', '|b1'),        # PSF-flux fit failure flag. DiaSource psfFlux_flag; Source psfFlux_flag
                                    # (AP-S: base_PsfFlux_flag).
    ('psfFlux_flag_edge', '|b1'),   # Too close to the image edge for the full PSF model. DiaSource
                                    # psfFlux_flag_edge; Source psfFlux_flag_edge (AP-S:...
    ('psfFlux_flag_noGoodPixels', '|b1'), # Not enough non-rejected pixels to fit the PSF model. DiaSource
                                          # psfFlux_flag_noGoodPixels; Source psfFlux_flag_noGoodPixels...
    ('trailFlux', '<f4'),           # [nJy] Flux of the trailed-source model fit, nJy (visit-minus-template).
                                    # DiaSource trailFlux; no Source equivalent.
    ('trailFluxErr', '<f4'),        # [nJy] Uncertainty of trailFlux, nJy. DiaSource trailFluxErr (not in the
                                    # RFL and DP1 tables); no Source equivalent.
    ('trailMag', '<f4'),            # [mag] AB magnitude of trailFlux: 31.4 - 2.5*log10(trailFlux/nJy), NULL
                                    # when trailFlux <= 0; computed on read, never stored. DiaSource (ppdb...
    ('trailMagErr', '<f4'),         # [mag] Uncertainty of trailMag: 1.0857*trailFluxErr/trailFlux, NULL when
                                    # trailFlux <= 0; computed on read. DiaSource (ppdb) trailMagErr; no...
    ('trailRa', '<f8'),             # [deg] Right ascension of the trailed-model centroid, degrees. DiaSource
                                    # trailRa; no Source equivalent.
    ('trailRaErr', '<f4'),          # [deg] Uncertainty of trailRa (of RA*cos(Dec)), degrees. DiaSource
                                    # trailRaErr (prompt and PPDB tables only); no Source equivalent.
    ('trailDec', '<f8'),            # [deg] Declination of the trailed-model centroid, degrees. DiaSource
                                    # trailDec; no Source equivalent.
    ('trailDecErr', '<f4'),         # [deg] Uncertainty of trailDec, degrees. DiaSource trailDecErr (prompt
                                    # and PPDB tables only); no Source equivalent.
    ('trailLength', '<f4'),         # [arcsec] Maximum-likelihood trail length, arcsec. DiaSource
                                    # trailLength; no Source equivalent.
    ('trailLengthErr', '<f4'),      # [arcsec] Uncertainty of trailLength, arcsec. DiaSource trailLengthErr
                                    # (prompt and PPDB tables only); no Source equivalent.
    ('trailAngle', '<f4'),          # [deg] Maximum-likelihood angle between the meridian through the
                                    # centroid and the trail (bearing), degrees. DiaSource trailAngle; no...
    ('trailAngleErr', '<f4'),       # [deg] Uncertainty of trailAngle, degrees. DiaSource trailAngleErr
                                    # (prompt and PPDB tables only); no Source equivalent.
    ('trailChi2', '<f4'),           # Chi^2 of the trailed-source model fit. DiaSource trailChi2 (prompt and
                                    # PPDB tables only); no Source equivalent.
    ('trailNdata', '<i4'),          # Number of pixels used in the trailed-source fit. DiaSource trailNdata
                                    # (prompt and PPDB tables only); no Source equivalent.
    ('trailAlgorithm', '<i4'),      # Trail-fit algorithm key, 1=SDSSShape, 2=HSMShape (DiaSource
                                    # trailAlgorithm, new in ApdbSchema 10.0.0). Filled on...
    ('trail_flag_edge', '|b1'),     # The trailed source extends onto or past edge pixels. DiaSource
                                    # trail_flag_edge; no Source equivalent.
    ('trail_flag', '|b1'),          # The trailed-source fit failed (DiaSource trail_flag, new in ApdbSchema
                                    # 10.0.0). Filled on dia_source_prompt_20260708 (Prompt Processing fro...
    ('dipoleMeanFlux', '<f4'),      # [nJy] Maximum-likelihood mean absolute flux of the two lobes of a
                                    # dipole model, nJy. DiaSource dipoleMeanFlux; no Source equivalent.
    ('dipoleMeanFluxErr', '<f4'),   # [nJy] Uncertainty of dipoleMeanFlux, nJy. DiaSource dipoleMeanFluxErr;
                                    # no Source equivalent.
    ('dipoleMeanMag', '<f4'),       # [mag] AB magnitude of dipoleMeanFlux: 31.4 -
                                    # 2.5*log10(dipoleMeanFlux/nJy), NULL when dipoleMeanFlux <= 0; comput...
    ('dipoleMeanMagErr', '<f4'),    # [mag] Uncertainty of dipoleMeanMag:
                                    # 1.0857*dipoleMeanFluxErr/dipoleMeanFlux, NULL when dipoleMeanFlux <=...
    ('dipoleFluxDiff', '<f4'),      # [nJy] Maximum-likelihood difference of the absolute fluxes of the two
                                    # dipole lobes, nJy. DiaSource dipoleFluxDiff; no Source equivalent.
    ('dipoleFluxDiffErr', '<f4'),   # [nJy] Uncertainty of dipoleFluxDiff, nJy. DiaSource dipoleFluxDiffErr;
                                    # no Source equivalent.
    ('dipoleLength', '<f4'),        # [arcsec] Maximum-likelihood lobe separation of the dipole model,
                                    # arcsec. DiaSource dipoleLength; no Source equivalent.
    ('dipoleAngle', '<f4'),         # [deg] Maximum-likelihood bearing of the dipole, negative to positive
                                    # lobe, degrees. DiaSource dipoleAngle; no Source equivalent.
    ('dipoleChi2', '<f4'),          # Chi^2 of the dipole model fit. DiaSource dipoleChi2; no Source
                                    # equivalent.
    ('dipoleNdata', '<i4'),         # Number of pixels used in the dipole fit. DiaSource dipoleNdata; no
                                    # Source equivalent.
    ('scienceFlux', '<f4'),         # [nJy] Forced PSF flux on the science (visit) image at the DiaSource
                                    # position, nJy. DiaSource scienceFlux; NULL on Source rows (never fil...
    ('scienceFluxErr', '<f4'),      # [nJy] Uncertainty of scienceFlux, nJy. DiaSource scienceFluxErr; NULL
                                    # on Source rows.
    ('scienceMag', '<f4'),          # [mag] AB magnitude of scienceFlux: 31.4 - 2.5*log10(scienceFlux/nJy),
                                    # NULL when scienceFlux <= 0; computed on read, never stored. DiaSourc...
    ('scienceMagErr', '<f4'),       # [mag] Uncertainty of scienceMag: 1.0857*scienceFluxErr/scienceFlux,
                                    # NULL when scienceFlux <= 0; computed on read. DiaSource (ppdb)...
    ('forced_PsfFlux_flag', '|b1'), # Forced PSF photometry on the science image (scienceFlux) failed.
                                    # DiaSource forced_PsfFlux_flag; no Source equivalent.
    ('forced_PsfFlux_flag_edge', '|b1'), # scienceFlux was too close to the image edge for the full PSF
                                         # model. DiaSource forced_PsfFlux_flag_edge; no Source equivalent.
    ('forced_PsfFlux_flag_noGoodPixels', '|b1'), # Not enough non-rejected pixels for scienceFlux. DiaSource
                                                 # forced_PsfFlux_flag_noGoodPixels; no Source equivalent.
    ('templateFlux', '<f4'),        # [nJy] Forced PSF flux on the template image at the DiaObject position,
                                    # nJy. DiaSource templateFlux (DP2 and PPDB tables; prompt eras from...
    ('templateFluxErr', '<f4'),     # [nJy] Uncertainty of templateFlux, nJy. DiaSource templateFluxErr; no
                                    # Source equivalent.
    ('templateMag', '<f4'),         # [mag] AB magnitude of templateFlux: 31.4 - 2.5*log10(templateFlux/nJy),
                                    # NULL when templateFlux <= 0; computed on read, never stored. DiaSour...
    ('templateMagErr', '<f4'),      # [mag] Uncertainty of templateMag: 1.0857*templateFluxErr/templateFlux,
                                    # NULL when templateFlux <= 0; computed on read. DiaSource (ppdb)...
    ('ixx', '<f4'),                 # [arcsec**2] Adaptive second moment xx of the source, arcsec^2.
                                    # DiaSource ixx; AP-S ext_shapeHSM_HsmSourceMoments_xx converted from...
    ('iyy', '<f4'),                 # [arcsec**2] Adaptive second moment yy of the source, arcsec^2.
                                    # DiaSource iyy; AP-S ext_shapeHSM_HsmSourceMoments_yy converted from...
    ('ixy', '<f4'),                 # [arcsec**2] Adaptive second moment xy of the source, arcsec^2.
                                    # DiaSource ixy; AP-S ext_shapeHSM_HsmSourceMoments_xy converted from...
    ('ixxPSF', '<f4'),              # [arcsec**2] Adaptive second moment xx of the PSF, arcsec^2. DiaSource
                                    # ixxPSF; AP-S ext_shapeHSM_HsmPsfMoments_xx converted from pixels^2 w...
    ('iyyPSF', '<f4'),              # [arcsec**2] Adaptive second moment yy of the PSF, arcsec^2. DiaSource
                                    # iyyPSF; AP-S ext_shapeHSM_HsmPsfMoments_yy converted from pixels^2 w...
    ('ixyPSF', '<f4'),              # [arcsec**2] Adaptive second moment xy of the PSF, arcsec^2. DiaSource
                                    # ixyPSF; AP-S ext_shapeHSM_HsmPsfMoments_xy converted from pixels^2 w...
    ('shape_flag', '|b1'),          # Source-moment failure flag. DiaSource shape_flag; AP-S
                                    # ext_shapeHSM_HsmSourceMoments_flag; NULL on NV-S and DP2-S.
    ('shape_flag_no_pixels', '|b1'), # No pixels to measure the moments. DiaSource shape_flag_no_pixels; AP-S
                                     # ext_shapeHSM_HsmSourceMoments_flag_no_pixels; NULL on NV-S and DP2-S.
    ('shape_flag_not_contained', '|b1'), # Centroid not contained in the footprint's bounding box. DiaSource
                                         # shape_flag_not_contained; AP-S...
    ('shape_flag_parent_source', '|b1'), # A deblend parent; moments are measured on children only. DiaSource
                                         # shape_flag_parent_source; AP-S...
    ('extendedness', '<f4'),        # Size-based extendedness classifier (moment-based traced radius vs the
                                    # PSF's). DiaSource extendedness; Source sizeExtendedness (AP-S:...
    ('reliability', '<f4'),         # Probability (0-1) that the detection is astrophysical, from a
                                    # machine-learning model. DiaSource reliability; no Source equivalent.
    ('reliabilityVersion', '<U7'),  # Version of the reliability model (DiaSource reliabilityVersion, new in
                                    # ApdbSchema 10.0.0). Filled on dia_source_prompt_20260708 (Prompt...
    ('band', '<U1'),                # Filter band. DiaSource band; Source band (from the data id).
    ('isDipole', '|b1'),            # Well fit by a dipole model. DiaSource isDipole; no Source equivalent.
    ('dipoleFitAttempted', '|b1'),  # A dipole fit was attempted. DiaSource dipoleFitAttempted; no Source
                                    # equivalent.
    ('bboxSize', '<i4'),            # [pixel] Side of the square bounding box containing the footprint,
                                    # pixels. DiaSource bboxSize; no Source equivalent.
    ('pixelFlags', '|b1'),          # Failure flag of the pixel-flag measurement: when set, the other
                                    # pixelFlags_* may be wrongly False. DiaSource pixelFlags; AP-S...
    ('pixelFlags_bad', '|b1'),      # Bad pixel in the footprint. DiaSource pixelFlags_bad; Source
                                    # pixelFlags_bad (AP-S: base_PixelFlags_flag_bad).
    ('pixelFlags_cr', '|b1'),       # Cosmic ray in the footprint. DiaSource pixelFlags_cr; Source
                                    # pixelFlags_cr (AP-S: base_PixelFlags_flag_cr).
    ('pixelFlags_crCenter', '|b1'), # Cosmic ray in the 3x3 region around the centroid. DiaSource
                                    # pixelFlags_crCenter; Source pixelFlags_crCenter (AP-S:...
    ('pixelFlags_edge', '|b1'),     # Some of the footprint is outside the usable exposure region (masked
                                    # EDGE, or centroid off image). DiaSource pixelFlags_edge; Source...
    ('pixelFlags_nodata', '|b1'),   # NO_DATA pixel in the footprint. DiaSource pixelFlags_nodata (not in
                                    # pDP1-DS); Source pixelFlags_nodata (AP-S: base_PixelFlags_flag_nodata).
    ('pixelFlags_nodataCenter', '|b1'), # NO_DATA pixel in the 3x3 region around the centroid. DiaSource
                                        # pixelFlags_nodataCenter (not in pDP1-DS); AP-S...
    ('pixelFlags_interpolated', '|b1'), # Interpolated pixel in the footprint. DiaSource
                                        # pixelFlags_interpolated; Source pixelFlags_interpolated (AP-S:...
    ('pixelFlags_interpolatedCenter', '|b1'), # Interpolated pixel in the 3x3 region around the centroid.
                                              # DiaSource pixelFlags_interpolatedCenter; Source...
    ('pixelFlags_offimage', '|b1'), # Source center is off image. DiaSource pixelFlags_offimage; Source
                                    # pixelFlags_offimage (AP-S: base_PixelFlags_flag_offimage).
    ('pixelFlags_saturated', '|b1'), # Saturated pixel in the footprint. DiaSource pixelFlags_saturated;
                                     # Source pixelFlags_saturated (AP-S: base_PixelFlags_flag_saturated).
    ('pixelFlags_saturatedCenter', '|b1'), # Saturated pixel in the 3x3 region around the centroid. DiaSource
                                           # pixelFlags_saturatedCenter; Source pixelFlags_saturatedCenter...
    ('pixelFlags_suspect', '|b1'),  # Suspect pixel in the footprint. DiaSource pixelFlags_suspect; Source
                                    # pixelFlags_suspect (AP-S: base_PixelFlags_flag_suspect).
    ('pixelFlags_suspectCenter', '|b1'), # Suspect pixel in the 3x3 region around the centroid. DiaSource
                                         # pixelFlags_suspectCenter; Source pixelFlags_suspectCenter (AP-S...
    ('pixelFlags_streak', '|b1'),   # Streak in the footprint. DiaSource pixelFlags_streak; no Source
                                    # equivalent.
    ('pixelFlags_streakCenter', '|b1'), # Streak in the 3x3 region around the centroid. DiaSource
                                        # pixelFlags_streakCenter; no Source equivalent.
    ('pixelFlags_injected', '|b1'), # Injection in the footprint. DiaSource pixelFlags_injected; no Source
                                    # equivalent.
    ('pixelFlags_injectedCenter', '|b1'), # Injection in the 3x3 region around the centroid. DiaSource
                                          # pixelFlags_injectedCenter; no Source equivalent.
    ('pixelFlags_injected_template', '|b1'), # Template injection in the footprint. DiaSource
                                             # pixelFlags_injected_template; no Source equivalent.
    ('pixelFlags_injected_templateCenter', '|b1'), # Template injection in the 3x3 region around the
                                                   # centroid. DiaSource pixelFlags_injected_templateCente...
    ('glint_trail', '|b1'),         # Part of a satellite glint trail. DiaSource glint_trail (DP2 and PPDB
                                    # tables; prompt eras from 20250905); no Source equivalent.
    ('eclLambda', '<f8'),           # [deg] Ecliptic longitude, converted from the observed coordinates.
    ('eclBeta', '<f8'),             # [deg] Ecliptic latitude, converted from the observed coordinates.
    ('galLon', '<f8'),              # [deg] Galactic longitude, converted from the observed coordinates.
    ('galLat', '<f8'),              # [deg] Galactic latitude, converted from the observed coordinates.
    ('elongation', '<f4'),          # [deg] Solar elongation of the object at the time of observation.
    ('phaseAngle', '<f4'),          # [deg] Phase angle between the Sun, object, and observer.
    ('topoRange', '<f4'),           # [AU] Topocentric distance (delta) at light-emission time.
    ('topoRangeRate', '<f4'),       # [km/s] Topocentric radial (line-of-sight) velocity (deldot); positive
                                    # values indicate motion away from the observer.
    ('helioRange', '<f4'),          # [AU] Heliocentric distance (r) at light-emission time.
    ('helioRangeRate', '<f4'),      # [km/s] Heliocentric radial velocity (rdot); positive values indicate
                                    # motion away from the Sun.
    ('ephRa', '<f8'),               # [deg] Predicted ICRS right ascension from the orbit in mpc_orbits.
    ('ephRaErr', '<f4'),            # [deg] 1-sigma uncertainty of ephRa (on the sky, i.e. including the
                                    # cos(dec) factor), propagated from the mpc_orbits covariance.
    ('ephDec', '<f8'),              # [deg] Predicted ICRS declination from the orbit in mpc_orbits.
    ('ephDecErr', '<f4'),           # [deg] 1-sigma uncertainty of ephDec, propagated from the mpc_orbits
                                    # covariance.
    ('ephRa_ephDec_Cov', '<f4'),    # [deg**2] Covariance between ephRa (on the sky) and ephDec; with
                                    # ephRaErr and ephDecErr, the predicted position's error ellipse, in t...
    ('ephVmag', '<f4'),             # [mag] Predicted magnitude in V band, computed from mpc_orbits data
                                    # including the mpc_orbits-provided (H, G) estimates
    ('ephRate', '<f4'),             # [deg/d] Total predicted on-sky angular rate of motion.
    ('ephRateRa', '<f4'),           # [deg/d] Predicted on-sky angular rate in the R.A. direction (includes
                                    # the cos(dec) factor).
    ('ephRateDec', '<f4'),          # [deg/d] Predicted on-sky angular rate in the declination direction.
    ('ephAntiSunPA', '<f4'),        # [deg] Predicted position angle of the extended Sun-to-object radius
                                    # vector (the anti-Sun direction, along which an ion tail points),...
    ('ephAntiMotionPA', '<f4'),     # [deg] Predicted position angle of the negative of the object's
                                    # heliocentric velocity vector (the direction a dust trail lags toward...
    ('ephOffset', '<f4'),           # [arcsec] Total observed versus predicted angular separation on the sky.
    ('ephOffsetRa', '<f8'),         # [arcsec] Offset between observed and predicted position in the R.A.
                                    # direction (includes cos(dec) term).
    ('ephOffsetDec', '<f8'),        # [arcsec] Offset between observed and predicted position in declination.
    ('ephOffsetAlongTrack', '<f4'), # [arcsec] Offset between observed and predicted position in the
                                    # along-track direction on the sky.
    ('ephOffsetCrossTrack', '<f4'), # [arcsec] Offset between observed and predicted position in the
                                    # cross-track direction on the sky.
    ('helio_x', '<f4'),             # [AU] Cartesian heliocentric X coordinate at light-emission time (ICRS).
    ('helio_y', '<f4'),             # [AU] Cartesian heliocentric Y coordinate at light-emission time (ICRS).
    ('helio_z', '<f4'),             # [AU] Cartesian heliocentric Z coordinate at light-emission time (ICRS).
    ('helio_vx', '<f4'),            # [km/s] Cartesian heliocentric X velocity at light-emission time (ICRS).
    ('helio_vy', '<f4'),            # [km/s] Cartesian heliocentric Y velocity at light-emission time (ICRS).
    ('helio_vz', '<f4'),            # [km/s] Cartesian heliocentric Z velocity at light-emission time (ICRS).
    ('helio_vtot', '<f4'),          # [km/s] The magnitude of the heliocentric velocity vector, sqrt(vx*vx +
                                    # vy*vy + vz*vz).
    ('topo_x', '<f4'),              # [AU] Cartesian topocentric X coordinate at light-emission time (ICRS).
    ('topo_y', '<f4'),              # [AU] Cartesian topocentric Y coordinate at light-emission time (ICRS).
    ('topo_z', '<f4'),              # [AU] Cartesian topocentric Z coordinate at light-emission time (ICRS).
    ('topo_vx', '<f4'),             # [km/s] Cartesian topocentric X velocity at light-emission time (ICRS).
    ('topo_vy', '<f4'),             # [km/s] Cartesian topocentric Y velocity at light-emission time (ICRS).
    ('topo_vz', '<f4'),             # [km/s] Cartesian topocentric Z velocity at light-emission time (ICRS).
    ('topo_vtot', '<f4'),           # [km/s] The magnitude of the topocentric velocity vector, sqrt(vx*vx +
                                    # vy*vy + vz*vz).
])

# NearbySSO: For each DiaSource, the nearest known Solar System object whose predicted position (from
# mpc_orbits) is within the object's matching radius (5 arcsec; 15 arcsec for comets and interstellar
# objects, i.e. designations starting C/, P/, D/ or I/), and whose predicted 1-sigma uncertainty is small
# enough for the association to be meaningful. Regenerated daily (RFC-1188).
NearbySSODtype = np.dtype([
    ('diaSourceId', '<i8'),         # Unique identifier of the DiaSource.
    ('ssObjectId', '<i8'),          # Id of the SSObject found within a matching radius of this source, if
                                    # any; NULL if the object has no SSObject row.
    ('designation', '<U16'),        # The primary provisional designation in unpacked form (e.g. 2008 AB).
    ('ephRa', '<f8'),               # [deg] Predicted ICRS right ascension from the orbit in mpc_orbits.
    ('ephRaErr', '<f4'),            # [deg] 1-sigma uncertainty of ephRa (on the sky, i.e. including the
                                    # cos(dec) factor), propagated from the mpc_orbits covariance.
    ('ephDec', '<f8'),              # [deg] Predicted ICRS declination from the orbit in mpc_orbits.
    ('ephDecErr', '<f4'),           # [deg] 1-sigma uncertainty of ephDec, propagated from the mpc_orbits
                                    # covariance.
    ('ephRa_ephDec_Cov', '<f4'),    # [deg**2] Covariance between ephRa (on the sky) and ephDec; with
                                    # ephRaErr and ephDecErr, the predicted position's error ellipse, in t...
    ('ephOffset', '<f4'),           # [arcsec] Total observed versus predicted angular separation on the sky.
    ('diaDistanceRank', '<i2'),     # Rank of this DiaSource by its separation from the object's predicted
                                    # position, among all DiaSources of the same visit within the object's...
    ('ephVmag', '<f4'),             # [mag] Predicted magnitude in V band, computed from mpc_orbits data
                                    # including the mpc_orbits-provided (H, G) estimates.
    ('ephRateRa', '<f4'),           # [deg/d] Predicted on-sky angular rate in the R.A. direction (includes
                                    # the cos(dec) factor).
    ('ephRateDec', '<f4'),          # [deg/d] Predicted on-sky angular rate in the declination direction.
    ('ephAntiSunPA', '<f4'),        # [deg] Predicted position angle of the extended Sun-to-object radius
                                    # vector (the anti-Sun direction, along which an ion tail points),...
    ('ephAntiMotionPA', '<f4'),     # [deg] Predicted position angle of the negative of the object's
                                    # heliocentric velocity vector (the direction a dust trail lags toward...
])
