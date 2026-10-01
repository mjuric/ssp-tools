"""WP3 of the widened SSSource: the predicted position's error ellipse
(``ephRaErr``, ``ephDecErr``, ``ephRa_ephDec_Cov``). See
docs/design/sssource-widened.md ("How it is built", item 2) and the WP3
section of ``ssp.sssource_contract``.

This is NearbySSO's machinery (``ssp.nearbysso.propagate``), used the same
way ``ssp.nearbysso.build.process_orbit`` uses it: ``coarse`` for the
covariance C(t) = Phi C0 Phi^T, then ``ellipse_at(track, t,
topo_pos=...)`` to project its position block on the tangent plane of the
precise pass's line of sight. Here ``coarse`` is sampled at the object's
(distinct) observation times themselves, so every time is bracketed, and
``ellipse_at`` hits a sample exactly: no free-motion interpolation is
involved, and the ellipse is C_pp(t) projected along ``topo_pos``.

The convention is NearbySSO's (and DiaSource's raErr/decErr/ra_dec_Cov):
with e_ra, e_dec the local east and north unit vectors, the sky covariance
of (RA cos Dec, Dec) is Sigma = J C_pp J^T, J = [e_ra; e_dec] / |rho|, and
ra_err = sqrt(Sigma_00) [deg] (so it *includes* cos Dec: it is an angle on
the sky, not an RA difference), dec_err = sqrt(Sigma_11) [deg], ra_dec_cov =
Sigma_01 [deg^2].
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from .nearbysso import propagate

#: mpc_orbits' designation column (the SSSource ``designation``).
_KEY = "unpacked_primary_provisional_designation"

#: Rows per batch when streaming mpc_orbits.
_BATCH = 20_000


def _subset_parquet(mpc_orbits_path, designations):
    """The rows of ``mpc_orbits_path`` whose designation is in
    ``designations`` (a pyarrow string array), with the columns
    ``load_orbits`` reads, as an in-memory Parquet file (a pyarrow Buffer),
    or None if no row matches.

    It streams the file in batches, one thread per row group, so neither the
    whole catalog nor the subset's JSON is ever decoded in memory at once
    (the subset is kept lz4-compressed: ~0.7 GB for 300k orbits). Row order
    is not kept; load_orbits sorts by designation."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from .nearbysso.orbits import _COLUMNS

    columns = list(_COLUMNS) + ["mpc_orb_jsonb"]
    pf = pq.ParquetFile(mpc_orbits_path)
    schema = pa.schema([pf.schema_arrow.field(c) for c in columns])
    nrg = pf.num_row_groups
    sink = pa.BufferOutputStream()
    lock = threading.Lock()
    nrows = [0]
    with pq.ParquetWriter(sink, schema, compression="lz4", use_dictionary=False) as writer:
        def scan(rg):
            f = pq.ParquetFile(mpc_orbits_path)
            for b in f.iter_batches(batch_size=_BATCH, row_groups=[rg], columns=columns,
                                    use_threads=False):
                b = b.filter(pc.is_in(b.column(_KEY), value_set=designations))
                if b.num_rows:
                    with lock:
                        writer.write_batch(b, row_group_size=_BATCH)
                        nrows[0] += b.num_rows

        with ThreadPoolExecutor(max_workers=max(1, min(nrg, 16))) as ex:
            list(ex.map(scan, range(nrg)))
    return sink.getvalue() if nrows[0] else None


def load_orbit_covariances(mpc_orbits_path, designations, ephem, *, verbose=True):
    """{designation: one ``ssp.nearbysso._contract.ORBIT_DTYPE`` row} for the
    ``designations`` present in ``mpc_orbits_path``.

    The rows are ``ssp.nearbysso.orbits.load_orbits(..., with_filter=False)``'s
    (SSSource keeps comets and short arcs), computed on only the requested
    rows: mpc_orbits is first streamed and filtered by designation, so the
    cost is that of the subset (~12 s and ~5 GB peak for 300k of 1.57M
    orbits, against ~15 s and ~7.3 GB for the whole catalog). ``ephem`` is
    an open ASSIST ephemeris (for the Sun at epoch). Empty and None
    designations are ignored. ``verbose`` passes to load_orbits (its
    one-line summary).

    The values are views into one compact structured array (~0.5 kB per
    orbit), so the dict is cheap to inherit by forked workers.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    from .nearbysso.orbits import load_orbits

    d = pa.array([str(x) for x in designations if x is not None and str(x) != ""],
                 type=pa.string())
    d = pc.unique(d)
    if len(d) == 0:
        return {}
    buf = _subset_parquet(mpc_orbits_path, d)
    if buf is None:
        return {}
    # (load_orbits reads it with pq.read_table and pq.ParquetFile, which take
    # a Buffer as well as a path)
    orbits = load_orbits(buf, with_filter=False, ephem=ephem, verbose=verbose)
    del buf
    return {str(des): orbits[k] for k, des in enumerate(orbits["designation"].tolist())}


def ephemeris_ellipse(orbit, t_assist, obs_pos, topo_pos, ephem):
    """The error ellipse of one object at its K observations: (ra_err,
    dec_err, ra_dec_cov), float64 arrays (K,) in deg, deg and deg^2 (the
    NearbySSO convention; see the module docstring).

    ``orbit`` is an ORBIT_DTYPE row; ``t_assist`` (K,) the ASSIST times (TDB
    days since J2000) of the observations; ``obs_pos`` (K, 3) the observer's
    barycentric ICRF positions [AU] at those times; ``topo_pos`` (K, 3) the
    precise pass's object - observer vectors [AU] (``EphResult.topo_pos.T``).
    The times need not be sorted, and may repeat.

    Pass the precise pass's float64 ``topo_pos``, not SSSource's float32
    ``topo_x/y/z``: a short arc's position covariance is a cigar along the
    line of sight (sigma ~0.1 AU radially against ~1e-6 AU across), so the
    projection is sensitive to the direction. The float32 rounding (~3e-8
    relative) changes such ellipses by up to ~1e-3, and the geometric
    instead of the light-time-corrected direction (~13" apart) by up to 10x.

    ``coarse`` is run once, at the distinct finite times that have a finite
    observer position, then ``ellipse_at(track, t, topo_pos=topo_pos)``. NaN
    where the orbit has no usable covariance (``has_cov`` False, or a
    non-PSD C(t)), where the propagation failed, and where t, obs_pos or
    topo_pos is not finite. Never raises for one bad orbit; inconsistent
    input shapes (a caller's bug) do raise ValueError.
    """
    t = np.asarray(t_assist, dtype=np.float64).reshape(-1)
    K = len(t)
    obs_pos = np.asarray(obs_pos, dtype=np.float64)
    topo_pos = np.asarray(topo_pos, dtype=np.float64)
    if obs_pos.shape != (K, 3) or topo_pos.shape != (K, 3):
        raise ValueError(f"ephemeris_ellipse: {K} times, obs_pos {obs_pos.shape}, "
                         f"topo_pos {topo_pos.shape}; expected ({K}, 3)")
    ra_err, dec_err, cov = np.full(K, np.nan), np.full(K, np.nan), np.full(K, np.nan)
    try:
        if K == 0 or not bool(orbit["has_cov"]):
            return ra_err, dec_err, cov
        good = np.isfinite(t) & np.all(np.isfinite(obs_pos), axis=1)
        if not good.any():
            return ra_err, dec_err, cov
        # one sample per distinct time (C(t) doesn't depend on the observer;
        # obs_pos only gates the sample), in ascending order
        tu, first = np.unique(t[good], return_index=True)
        track = propagate.coarse(orbit, tu, obs_pos[good][first], ephem)
        e_ra, e_dec, e_cov, _ = propagate.ellipse_at(track, t[good], topo_pos=topo_pos[good])
        ra_err[good], dec_err[good], cov[good] = e_ra, e_dec, e_cov
    except Exception:       # one bad orbit must not stop a run
        ra_err[:] = dec_err[:] = cov[:] = np.nan
    return ra_err, dec_err, cov
