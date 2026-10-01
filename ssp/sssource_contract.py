"""The contract between the widened-SSSource work packages.

See docs/design/sssource-widened.md. The column list, order and types are
``ssp.schema_ppdb.SSSourceDtype``, generated from sdm_schemas' sso_base.yaml
(branch tickets/DM-55375; a copy is in tests/data/sdm_schemas/). This module
adds the rules that dtype can't express. Work packages must not change
this file or schema_ppdb.py; a needed change goes to the integrating session.
"""

from ssp.schema_ppdb import NearbySSODtype, SSSourceDtype  # noqa: F401

# --------------------------------------------------------------------------
# The widened SSSource table
# --------------------------------------------------------------------------

#: Columns that may never be NULL (sso_base.yaml's ``nullable: false``;
#: tests/test_sssource_contract.py checks they agree). Every other column
#: is nullable. The writer must fail if any of these has a NULL.
SSSOURCE_NONNULL = frozenset({
    "obsid", "status", "primary", "matchMethod",
    "measuredOn", "processing", "processingTable",
    "visit", "detector", "midpointMjdTai", "ra", "dec", "band", "psfFlux", "psfFluxErr",
    "eclLambda", "eclBeta", "galLon", "galLat",
})

#: Low-cardinality string columns, written dictionary-encoded.
SSSOURCE_DICTIONARY = ("status", "matchMethod", "measuredOn", "processing", "processingTable",
                       "band", "trailAlgorithm", "reliabilityVersion")

#: Row order of sssource.parquet (ascending; NULL ssObjectId rows last).
SSSOURCE_SORT = ("ssObjectId", "midpointMjdTai", "obsid")

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

#: SubmittableSources columns not carried into SSSource: the view's own
#: spatial query helpers.
VIEW_DROPPED = ("hpix29", "cx", "cy", "cz")

#: The predicted position's error ellipse (deg, deg, deg^2): the
#: NearbySSO convention, i.e. the DiaSource raErr/decErr/ra_dec_Cov one.
ELLIPSE_COLUMNS = ("ephRaErr", "ephDecErr", "ephRa_ephDec_Cov")

# --------------------------------------------------------------------------
# Rules for specific columns
# --------------------------------------------------------------------------
#
# Block 1 (obs_sbn): obsid, trksub, trkid, submission_id and primary from
#   dia_sources.parquet (the extractor's own); status from obs_sbn, joined
#   on obsid. matchMethod from dia_sources.parquet (WP1).
# Block 2: ssObjectId is NULL unless the row's object has an SSObject row,
#   i.e. NULL for status-'I' rows (no designation) and for designated objects
#   missing from mpc_orbits (issue #7). designation is the MPC's primary
#   provisional designation whenever there is one (also for #7 rows).
# Block 3 and 4: copied from dia_sources.parquet, cast to SSSourceDtype; a
#   narrowing cast must not overflow (fail if it would), float64 -> float32
#   rounding is expected.
# Block 6: as today's SSSource (ssp.sssource.compute_sssource_entry), plus
#   ELLIPSE_COLUMNS (WP3). NULL (NaN) for rows without an orbit; the ellipse
#   is also NULL where the orbit has no usable covariance.

# --------------------------------------------------------------------------
# WP1: extract-submitted-sources
# --------------------------------------------------------------------------
#
# dia_sources.parquet gains a non-null string column ``matchMethod`` (one of
# MATCH_METHODS), next to the existing ``match``; the -B row of a trail pair
# repeats its -A row's value, obssubid_trail. Everything else is unchanged.

# --------------------------------------------------------------------------
# WP3: the ephemeris ellipse (module ssp/sssource_ellipse.py)
# --------------------------------------------------------------------------
#
# load_orbit_covariances(mpc_orbits_path, designations, ephem) -> dict
#   {designation: one ssp.nearbysso._contract.ORBIT_DTYPE row} for the given
#   designations present in mpc_orbits, via ssp.nearbysso.orbits.load_orbits
#   (with_filter=False: SSSource keeps comets and short arcs).
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
#   propagation failed. Never raises for one bad orbit.

# --------------------------------------------------------------------------
# WP2: the writer (ssp.sssource; console script ssp-build-sssource)
# --------------------------------------------------------------------------
#
# Writes sssource.parquet with exactly the SSSourceDtype columns, in order:
# Arrow types from the dtype (U<n> -> string, dictionary-encoded for
# SSSOURCE_DICTIONARY), nullable except SSSOURCE_NONNULL, sorted by
# SSSOURCE_SORT, zstd. One row per dia_sources.parquet row (obsid unique).
