"""The contract between the SSObservation work packages.

See docs/design/sssource-widened.md. The column list, order and types are
``ssp.schema_ppdb.SSObservationDtype``, generated from sdm_schemas'
sso_base.yaml (branch tickets/DM-55375; a copy is in tests/data/sdm_schemas/).
This module adds the rules that dtype can't express. Work packages must not
change this file or schema_ppdb.py; a needed change goes to the integrating
session.
"""

from ssp.schema_ppdb import NearbySSODtype, SSObservationDtype  # noqa: F401

# --------------------------------------------------------------------------
# The SSObservation table
# --------------------------------------------------------------------------

#: Columns that may never be NULL (sso_base.yaml's ``nullable: false``;
#: tests/test_ssobservation_contract.py checks they agree). Every other column
#: is nullable. The writer must fail if any of these has a NULL.
SSOBSERVATION_NONNULL = frozenset({
    "obsid", "status", "primary",
    "measuredOn", "processing", "processingTable",
    "visit", "detector", "midpointMjdTai", "midpointMjdTai_flag",
    "ra", "dec", "band", "psfFlux", "psfFluxErr",
    "eclLambda", "eclBeta", "galLon", "galLat",
})

#: Low-cardinality string columns, written dictionary-encoded.
#: (Applies to the internal columns in the sidecar too.)
SSOBSERVATION_DICTIONARY = ("status", "matchMethod", "measuredOn", "processing", "processingTable",
                            "band", "reliabilityVersion")

#: Row order of ssobservation.parquet (ascending; NULL ssObjectId rows last).
SSOBSERVATION_SORT = ("ssObjectId", "midpointMjdTai", "obsid")

#: The values of ``matchMethod``: how extract-submitted-sources found the
#: measurement each obs_sbn row was submitted from.
#:   obssubid        the obsSubID (LSST-<processing>-<id> or a bare id) looked
#:                   up, and verified by position and time
#:   obssubid_trail  an '-A'/'-B' pair of obs_sbn rows (the two ends of one
#:                   trailed source) verified at their midpoint; both rows
#:                   point at the one measurement, and the -A row is primary
#:   position        the position + time search (the obsSubID didn't parse or
#:                   verify, or an -A/-B row lacks its partner)
MATCH_METHODS = ("obssubid", "obssubid_trail", "position")

#: The view's ``id``/``parentId`` go to one of these pairs, by ``measuredOn``;
#: the other pair is NULL. (extract-submitted-sources renames the view's
#: ``id`` to ``diaSourceId`` in dia_sources.parquet, whatever measuredOn is,
#: and keeps ``parentId``.)
ID_SPLIT = {
    "difference": ("diaSourceId", "parentDiaSourceId"),
    "science": ("sourceId", "parentSourceId"),
}

#: SubmittableSources columns not carried into SSObservation: the view's own
#: spatial query helpers.
VIEW_DROPPED = ("hpix29", "cx", "cy", "cz")

# --------------------------------------------------------------------------
# Internal columns and the sidecar (docs/design/ssobservation-delivery.md)
# --------------------------------------------------------------------------
#
# Internal columns are computed by the build like any other, but are not
# in the Felis schema (SSObservationDtype) and not in the delivered table:
# they go to the sidecar, SIDECAR_FILE, with the key column obsid.
#
# The internal set is configurable (ssp-build-ssobservation
# --internal-columns, and ssp-build-sso / ssp-sso-daily, which pass it
# on); the default is SSOBSERVATION_INTERNAL_DEFAULT. Every internal column
# must be in SSOBSERVATION_INTERNAL_DTYPE (the columns the build computes
# that may be internal) and not in SSObservationDtype: a column returning
# to the delivered table goes back into the Felis schema and leaves the
# internal set. The build refuses a column in neither or in both.

#: The default internal columns.
SSOBSERVATION_INTERNAL_DEFAULT = ("matchMethod", "midpointMjdTai_flag_degraded")

#: The types of the columns that may be internal (NumPy dtype strings, as
#: in SSObservationDtype; they were in sso_base.yaml until the RFC-1188
#: schema review, 2026-10-08). Both are non-null.
SSOBSERVATION_INTERNAL_DTYPE = {
    "matchMethod": "<U16",                    # MATCH_METHODS
    "midpointMjdTai_flag_degraded": "|b1",    # see 'Shutter-motion' below
}
SSOBSERVATION_INTERNAL_NONNULL = frozenset(SSOBSERVATION_INTERNAL_DTYPE)

#: The sidecar: obsid (as SSObservationDtype's, non-null) followed by the
#: internal columns in the configured order; one row per SSObservation
#: row, in the same order as the parts concatenated in part order; zstd;
#: a single file. Not uploaded to the PPDB.
SIDECAR_FILE = "SSObservation_internal.parquet"
SIDECAR_KEY = "obsid"

# --------------------------------------------------------------------------
# The partitioned delivery (docs/design/ssobservation-delivery.md)
# --------------------------------------------------------------------------
#
# SSObservation is delivered as parts, PART_FILE_FORMAT.format(k) for
# k = 0, 1, ... (contiguous), plus SSOBSERVATION_MANIFEST_FILE, all in the
# delivery directory. Rows are in SSOBSERVATION_SORT order across the parts
# taken in part order. A part is cut from that order:
#   - a part never splits an ssObjectId: each part's [min, max] range is
#     disjoint from every other's and the ranges ascend with k;
#   - a ranged part closes at the first object boundary at or after
#     part_rows rows (default PART_ROWS_DEFAULT; configurable), so a ranged
#     part has at least part_rows rows unless it is the last ranged part,
#     and may have more (one object's rows are never split);
#   - rows with a NULL ssObjectId come after every ranged part, in their own
#     part(s), cut every part_rows rows (these may split anywhere): every
#     NULL part has exactly part_rows rows except the last, which has
#     1..part_rows;
#   - an empty table is one part with no rows (null_ssObjectId false,
#     range null).
# Each part has exactly the delivered schema (the columns of
# SSObservationDtype, as ssp.ssobservation.ssobservation_schema()).
PART_ROWS_DEFAULT = 2_000_000
PART_FILE_FORMAT = "SSObservation.part{:04d}.parquet"
PART_GLOB = "SSObservation.part*.parquet"
SSOBSERVATION_MANIFEST_FILE = "SSObservation.manifest.json"
MANIFEST_FORMAT_VERSION = 1

#: The manifest's fields (JSON; written last, after every part and the
#: sidecar).
SSOBSERVATION_MANIFEST_FIELDS = {
    "table": '"SSObservation"',
    "format_version": "MANIFEST_FORMAT_VERSION",
    "schema": '{"source": "lsst/sdm_schemas tickets/DM-55375", "file": "sso_base.yaml", '
              '"md5": <md5 of the sso_base.yaml the build used>}',
    "ssp_tools_commit": "git commit of the code that built it (null if unknown)",
    "created_utc": "ISO 8601 UTC time the manifest was written",
    "partition_key": '"ssObjectId"',
    "sort": "list(SSOBSERVATION_SORT)",
    "part_rows": "the part_rows used",
    "rows": "total rows over all parts",
    "parts": "[PART_FIELDS, ...] in part order",
    "sidecar": "SIDECAR_FIELDS",
}
#: Per part. ssObjectId_min/max are null for a NULL-ssObjectId part (and
#: for the empty table's single part).
PART_FIELDS = ("file", "rows", "ssObjectId_min", "ssObjectId_max", "null_ssObjectId", "bytes", "md5")
#: The sidecar entry: columns lists the internal columns (not the key).
SIDECAR_FIELDS = ("file", "key", "columns", "rows", "bytes", "md5")
#: The manifest's schema.md5 is the md5 of tests/data/sdm_schemas/sso_base.yaml
#: (the copy ssp/schema_ppdb.py is generated from), null where the source
#: tree isn't available; ssp_tools_commit is ``git rev-parse HEAD`` of the
#: source tree, null where it isn't available.

# --------------------------------------------------------------------------
# Interfaces between the work packages (W1, W2, W3)
# --------------------------------------------------------------------------
#
# The builder (W1): ssp-build-ssobservation / python -m ssp.ssobservation
#   writes into --output-dir the parts, SSOBSERVATION_MANIFEST_FILE and
#   SIDECAR_FILE, under exactly the delivered names, and nothing else named
#   SSObservation* or ssobservation*. New options:
#     --part-rows N              (default PART_ROWS_DEFAULT)
#     --internal-columns A,B,... (default SSOBSERVATION_INTERNAL_DEFAULT;
#                                 an empty string means none: no column of
#                                 SSOBSERVATION_INTERNAL_DTYPE is then
#                                 produced, and the sidecar has only obsid)
#   build_ssobservation(input_dir, output_dir, ...,
#                       part_rows=PART_ROWS_DEFAULT,
#                       internal_columns=SSOBSERVATION_INTERNAL_DEFAULT)
#   A column in --internal-columns must be in SSOBSERVATION_INTERNAL_DTYPE
#   (else the build fails before reading the inputs).
#
# Reading (everyone but W2's black-box checks): ssp.ssobservation_parts
#   (read_ssobservation(path, columns=None, filters=None, internal=False),
#   part_paths, sidecar_path, read_manifest, num_rows), with ``path`` the
#   directory holding the manifest, or the manifest file.
#
# The build (W3): ssp-build-sso / ssp-sso-daily pass --part-rows and
#   --internal-columns through to the builder (same names and meaning) and
#   record them in report.json's steps.ssobservation; the step's outputs
#   (every part, the manifest, the sidecar) go to RUN_DIR/delivery/; SSObject
#   and NearbySSO read SSObservation through ssp.ssobservation_parts.
#
# The checks (W2): bench.ssobservation_validate's subcommands take the
#   SSObservation directory (or its manifest) wherever they took the
#   ssobservation.parquet path, and read internal columns from its sidecar;
#   ssp.delivery_check.check_ssobservation_parts as in delivery_contract.

# --------------------------------------------------------------------------
# Shutter-motion-corrected times (docs/design/shutter-timing.md)
# --------------------------------------------------------------------------
#
# dia_sources.parquet (the extract, WP S1) carries, per row:
#   midpointMjdTai                the source's shutter-corrected exposure
#                                 midpoint (shutter_timing.corrections.
#                                 corrected_midpoints(visit, detector, x, y)),
#                                 or the visit's midpoint where uncorrected
#   midpointMjdTaiVisit           the visit's midpoint, as the measurement
#                                 had it
#   midpointMjdTai_flag           True: midpointMjdTai is the visit's midpoint
#                                 (the table omits this visit: status 2)
#   midpointMjdTai_flag_degraded  True: corrected with reduced accuracy
#                                 (status 1)
#   dt_corrected_ms               diagnostic: obstime - corrected time [ms]
#   obstime_basis                 how the obs_sbn row's time matched: 'visit',
#                                 'corrected' or 'both' (each within DT_MS)
# A NOT_BUILT visit (status 3) gets the visit's time and midpointMjdTai_flag
# True, with a warning naming it; more than MAX_NOT_BUILT_VISITS distinct
# NOT_BUILT visits fail the extract (a stale or skipped stage 0).
# The flags are non-null for every row (False/False for a status-0 correction).
#
# SSObservation copies midpointMjdTai and the two flags (block 4) and computes
# every ephemeris column at that midpointMjdTai. midpointMjdTaiVisit and
# obstime_basis are internal: not published, dropped without a warning.
SHUTTER_INTERNAL = ("midpointMjdTaiVisit", "obstime_basis", "dt_corrected_ms")

#: The default limit on NOT_BUILT visits in one extract (configurable).
MAX_NOT_BUILT_VISITS = 20

#: Visits on nights before the correction table's first night are outside
#: its coverage (e.g. ComCam): the visit time, midpointMjdTai_flag True, not
#: counted toward MAX_NOT_BUILT_VISITS; counted separately in the manifest.
#: The correction is applied (however large) only where the pipeline's
#: visit time equals the exposure log's header midpoint header_mid_mjd_tai
#: ((MJD-BEG + MJD-END)/2, from the table's exposures_<day_obs>.parquet) to
#: within this [s]; otherwise the visit time, midpointMjdTai_flag True, a
#: warning, counted in the manifest as time_mismatch (docs/design/
#: shutter-timing.md, "Visit-time guard").
MAX_HEADER_MISMATCH_S = 0.001
#: The guard also passes a pipeline time that equals the corrected time to
#: within MAX_HEADER_MISMATCH_S (an input whose times are already corrected,
#: e.g. once AP writes shutter-corrected DiaSource times): the time stands.
#:
#: The guard checks the header midpoint first, then the corrected time.
#:
#: A correction that moves a time by more than this [s] from the pipeline's
#: visit time is still applied, but midpointMjdTai_flag_degraded is set and
#: a warning names the visits; counted in the manifest as large_shift. (Such
#: shifts are hung end-of-integration readouts, where the table's time is
#: right and MJD-END is minutes late, or rare exposures whose shutter
#: profile contradicts the header; the largest late-readout shift in normal
#: operations is ~1.94 s.)
MAX_CORRECTION_S = 3.0

#: The predicted position's error ellipse (deg, deg, deg^2): the
#: NearbySSO convention, i.e. the DiaSource raErr/decErr/ra_dec_Cov one.
ELLIPSE_COLUMNS = ("ephRaErr", "ephDecErr", "ephRa_ephDec_Cov")

# --------------------------------------------------------------------------
# Rules for specific columns
# --------------------------------------------------------------------------
#
# NULL and NaN:
#   - Copied columns (blocks 1, 3 and 4, from dia_sources.parquet and
#     obs_sbn) keep their value as is: a NULL stays NULL and a NaN stays NaN.
#   - Computed columns (block 6) are NULL, not NaN, where there is no value.
#   - String columns are NULL where there is no value, never ''.
# Arrow field nullability: declared non-nullable exactly for
#   SSOBSERVATION_NONNULL, nullable otherwise.
# "No orbit" means ephRa/ephDec are NULL; the columns computed from the
#   observed position alone (ecl*, gal*, elongation) are still filled.
#
# Block 1 (obs_sbn): obsid, trksub, trkid, submission_id and primary from
#   dia_sources.parquet (the extractor's own); status from obs_sbn, joined
#   on obsid. matchMethod from dia_sources.parquet (WP1).
# Block 2: ssObjectId is NULL unless the row's object has an SSObject row,
#   i.e. NULL for status-'I' rows (no designation) and for designated objects
#   missing from mpc_orbits (issue #7). designation is the MPC's primary
#   provisional designation whenever there is one (also for #7 rows), and
#   NULL otherwise (status-'I' rows).
# Block 3 and 4: copied from dia_sources.parquet, cast to SSObservationDtype; a
#   narrowing cast must not overflow (fail if it would), float64 -> float32
#   rounding is expected.
# Block 6: as today's SSObservation
#   (ssp.ssobservation.compute_ssobservation_entry), plus ELLIPSE_COLUMNS
#   (WP3), and the along/cross-track offsets, as pipe_tasks' ssoAssociation
#   computes them [arcsec]:
#     along = (ephOffsetRa * ephRateRa + ephOffsetDec * ephRateDec) / ephRate
#     cross = (-ephOffsetRa * ephRateDec + ephOffsetDec * ephRateRa) / ephRate
#   (ephOffsetRa includes cos(dec); ephRate = hypot(ephRateRa, ephRateDec)),
#   NULL where there is no orbit or ephRate is 0. (SSObservation no longer has
#   diaDistanceRank; it is on NearbySSO.) Every block-6 column is NULL for
#   rows without an orbit; the ellipse is also NULL where the orbit has no
#   usable covariance.

# --------------------------------------------------------------------------
# Tail position angles (docs/design/tail-angles.md): ephAntiSunPA,
# ephAntiMotionPA in SSObservation and NearbySSO
# --------------------------------------------------------------------------
#
# ssp.ephem_assist.tail_position_angles(helio_pos, helio_vel, topo_pos)
#     -> (anti_sun_pa, anti_motion_pa)
#   (3, N) arrays from one EphResult (helio_pos [AU], helio_vel [km/s],
#   topo_pos [AU]: the light-emission-time vectors, as published in the
#   helio_* and topo_* columns). Returns two (N,) float64 arrays [deg] in
#   [0, 360). With u = topo_pos / |topo_pos| at (alpha, delta) and the ICRS
#   tangent basis
#     north = (-sin d cos a, -sin d sin a, cos d),  east = (-sin a, cos a, 0),
#   a vector w has position angle atan2(w . east, w . north) mod 360, i.e.
#   measured from ICRS north through east:
#     anti_sun_pa    for w = helio_pos   (the extended Sun -> object vector;
#                                          JPL Horizons PsAng)
#     anti_motion_pa for w = -helio_vel  (JPL Horizons PsAMV)
#   NaN where w's projection on the sky is exactly zero, or any input is NaN.
#   (A vector along the line of sight rarely projects to exactly zero in
#   floating point; there the angle is arbitrary, as Horizons' is. At
#   dec = +-90 deg, alpha = atan2(0, 0) = 0 fixes "east".) The pole is ICRS,
#   as for ephRa/ephDec, and Horizons' with REF_SYSTEM=ICRF (WP T2).
# The stored float32 columns are ssp.ephem_assist.tail_position_angles_f32 of
# these: a value that rounds up to 360.0f is stored as 0, so the stored
# columns are in [0, 360) too.
# Both columns are block-6 columns: float32, NULL without an orbit, filled
# for every object with one (comet or not), at every phase angle.

# --------------------------------------------------------------------------
# WP1: extract-submitted-sources
# --------------------------------------------------------------------------
#
# dia_sources.parquet gains a non-null string column ``matchMethod`` (one of
# MATCH_METHODS), next to the existing ``match``; the -B row of a trail pair
# repeats its -A row's value, obssubid_trail. Everything else is unchanged.

# --------------------------------------------------------------------------
# WP3: the ephemeris ellipse (module ssp/ssobservation_ellipse.py)
# --------------------------------------------------------------------------
#
# load_orbit_covariances(mpc_orbits_path, designations, ephem) -> dict
#   {designation: one ssp.nearbysso._contract.ORBIT_DTYPE row} for the given
#   designations present in mpc_orbits, via ssp.nearbysso.orbits.load_orbits
#   (with_filter=False: SSObservation keeps comets and short arcs).
#
# ephemeris_ellipse(orbit, t_assist, obs_pos, topo_pos, ephem)
#     -> (ra_err, dec_err, ra_dec_cov)
#   For one object at its K observation times: ``orbit`` an ORBIT_DTYPE row,
#   ``t_assist`` (K,) ASSIST times (TDB days since J2000, i.e. the
#   observations' midpointMjdTai converted), ``obs_pos`` (K, 3) X05
#   barycentric ICRF positions [AU] at those times, ``topo_pos`` (K, 3) the
#   precise pass's object - observer vectors [AU] (EphResult.topo_pos.T).
#   Returns float64 arrays in deg, deg and deg^2: ssp.nearbysso.propagate.
#   coarse sampled so that every time is bracketed, then ellipse_at(...,
#   topo_pos=topo_pos). NaN where the orbit has no usable covariance or the
#   propagation failed. Never raises for one bad orbit; raises ValueError for
#   caller errors (``orbit`` not an ORBIT_DTYPE row, ``ephem`` None, shapes
#   not (K,), (K, 3), (K, 3)).
#   ssp.nearbysso.propagate.STEP_CAP_STOPS counts the simulations the step cap
#   stopped, per process: in a forked pool, reset it per chunk and return it
#   to the parent for the run report (as ssp/nearbysso/build.py does).

# --------------------------------------------------------------------------
# WP2: the writer (ssp.ssobservation; console script ssp-build-ssobservation)
# --------------------------------------------------------------------------
#
# Writes ssobservation.parquet with exactly the SSObservationDtype columns, in
# order: Arrow types from the dtype (U<n> -> string, dictionary-encoded for
# SSOBSERVATION_DICTIONARY), nullable except SSOBSERVATION_NONNULL, sorted by
# SSOBSERVATION_SORT, zstd. One row per dia_sources.parquet row (obsid unique).
