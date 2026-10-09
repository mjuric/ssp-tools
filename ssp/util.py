import numpy as np
from astropy.time import Time
import astropy.units as u
from astropy.coordinates import get_sun, angular_separation
import numpy.ma as ma
from astropy.constants import R_earth
from astropy.coordinates import (
    EarthLocation,
    solar_system_ephemeris,
    get_body_barycentric_posvel,
)
from astroquery.mpc import MPC
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pyarrow.compute as pc
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool


def assoc_validate_recorded(dia, assoc):
    """Check the offsets extract-submitted-sources recorded per match
    (``sep_mas``, ``dt_ms``) of the ``assoc`` rows of ``dia``: at most 5 mas,
    and under 0.51 s (the USDF replica's obstime is rounded to 1 s)."""
    rec = dia[["sep_mas", "dt_ms"]].iloc[assoc["dia_index"].values]
    sep = rec["sep_mas"].to_numpy(dtype=float, na_value=np.nan) / 1000
    dt = rec["dt_ms"].to_numpy(dtype=float, na_value=np.nan) / 1000

    print("Separation diffeerence range (arcsec): ", sep.min(), sep.max())
    assert sep.max() <= 0.005
    print("Time diffeerence range (sec):          ", dt.min(), dt.max())
    assert abs(dt).max() < 0.51

    print(f"All OK, {len(assoc):,} observations.")


def packed_ascii_to_uint64_le(mpc_packed):
    """
    Convert a pandas string[pyarrow] column of ASCII strings (<= 8 bytes)
    to little-endian uint64 by left-padding with ASCII spaces to 8 chars.
    """

    # Step 1: Convert pandas Series → real pyarrow.StringArray
    arr = pa.array(mpc_packed, type=pa.string())

    # Step 2: Left-pad to length 8 with ASCII spaces (works on older PyArrow!)
    # ascii_lpad takes (string_array, target_length, pad_char)
    padded = pc.ascii_lpad(arr, 8, " ")  # returns string array padded to 8 chars

    # Step 3: Convert padded string → binary
    bin_arr = pc.cast(padded, pa.binary())

    # Step 4: Slice each to exactly 8 bytes
    fixed = pc.binary_slice(bin_arr, 0, 8)

    # Step 5: Flatten chunks into a single contiguous array
    if isinstance(fixed, pa.ChunkedArray):
        fixed = fixed.combine_chunks()

    # Step 6: Extract contiguous values buffer
    buf = fixed.buffers()[2]

    # Step 7: Interpret every 8 bytes as a little-endian uint64
    return np.frombuffer(buf, dtype="<u8")


#: solar_elongation_ndarray's finite-difference step for the Sun's motion
#: [s], with ``dt_s``.
SUN_SHIFT_STEP_S = 1.0


def solar_elongation_ndarray(ra_deg, dec_deg, t, dt_s=None):
    """
    Very fast computation of solar elongation (ICRS great-circle separation)
    using astropy.coordinates.angular_separation.

    Parameters
    ----------
    ra_deg : ndarray
        Target RA in degrees (ICRS).
    dec_deg : ndarray
        Target Dec in degrees (ICRS).
    t : astropy.time.Time
        Observation times; with ``dt_s``, reference times (e.g. the visit
        midpoints) that the observation times differ from by ``dt_s``.
    dt_s : ndarray, optional
        The observation times minus ``t`` [s] (at most a second or so, e.g.
        the shutter-motion correction). The Sun is then evaluated once per
        unique ``t`` (and at ``t`` + SUN_SHIFT_STEP_S, for its motion) and
        shifted linearly by ``dt_s``: the Sun moves ~0.04"/s, so the error
        of the linear shift is far below a micro-arcsecond. Where ``dt_s``
        is 0 the result is bitwise as without it.

    Returns
    -------
    elong_deg : ndarray
        Solar elongation in degrees.
    """

    # Get Sun coordinates. get_sun is slow (~90 us per time), but sources
    # from the same visit share one time, so evaluate it only once per
    # unique time and broadcast (e.g. 730k sources -> 1,380 visit times).
    t = Time(t)
    _, first, inv = np.unique(
        np.stack([np.atleast_1d(t.jd1), np.atleast_1d(t.jd2)], axis=-1),
        axis=0, return_index=True, return_inverse=True,
    )
    sun = get_sun(np.atleast_1d(t)[first]).icrs

    # Extract Sun RA/Dec arrays (radian floats)
    sun_ra = sun.ra.radian[inv.ravel()].reshape(t.shape)
    sun_dec = sun.dec.radian[inv.ravel()].reshape(t.shape)

    if dt_s is not None:
        # the Sun's motion per second, at each unique reference time
        t1 = np.atleast_1d(t)[first] + SUN_SHIFT_STEP_S * u.s
        sun1 = get_sun(t1).icrs
        dra = np.angle(np.exp(1j * (sun1.ra.radian - sun.ra.radian)))   # (wrapped to [-pi, pi])
        rate_ra = (dra / SUN_SHIFT_STEP_S)[inv.ravel()].reshape(t.shape)
        rate_dec = ((sun1.dec.radian - sun.dec.radian) / SUN_SHIFT_STEP_S)[inv.ravel()].reshape(t.shape)
        dt_s = np.broadcast_to(np.asarray(dt_s, dtype=np.float64), t.shape)
        sun_ra = sun_ra + rate_ra * dt_s
        sun_dec = sun_dec + rate_dec * dt_s

    # Convert input to radians
    ra = np.radians(ra_deg)
    dec = np.radians(dec_deg)

    # Fast great-circle angular separation
    sep = angular_separation(ra, dec, sun_ra, sun_dec)

    # Convert to degrees for return
    return np.degrees(sep)


def wrap_ra_deg(ra):
    """RA [deg] wrapped to [0, 360), bitwise as astropy's ``Longitude`` (and
    so ``SkyCoord(...).ra.deg``) does it: the same operations, applied only
    when some value is out of range (so e.g. -0.0 stays -0.0 otherwise)."""
    ra = np.array(ra, dtype=np.float64)
    if not ((ra < 0.0) | (ra >= 360.0)).any():
        return ra
    ra -= (ra - 0.0) // 360.0 * 360.0
    ra[ra >= 360.0] -= 360.0
    ra[ra < 0.0] += 360.0
    return ra


_DEG2RAD = u.deg.to(u.rad)
_RAD2DEG = u.rad.to(u.deg)
_DEG2ARCSEC = u.deg.to(u.arcsec)


def sky_separation_arcsec(ra1, dec1, ra2, dec2):
    """Great-circle separation [arcsec] of ICRS points given in degrees,
    bitwise as ``SkyCoord(ra1, dec1).separation(SkyCoord(ra2, dec2)).arcsec``
    but without its per-call overhead: the same Vincenty formula
    (``astropy.coordinates.angular_separation``) with the same operations
    and unit-conversion factors as astropy's Quantity arithmetic applies
    (the RA difference taken in degrees, then converted)."""
    ra1, ra2 = wrap_ra_deg(ra1), wrap_ra_deg(ra2)
    dlon = (ra2 - ra1) * _DEG2RAD
    lat1, lat2 = np.asarray(dec1) * _DEG2RAD, np.asarray(dec2) * _DEG2RAD
    sdlon, cdlon = np.sin(dlon), np.cos(dlon)
    slat1, slat2, clat1, clat2 = np.sin(lat1), np.sin(lat2), np.cos(lat1), np.cos(lat2)
    num1 = clat2 * sdlon
    num2 = clat1 * slat2 - slat1 * clat2 * cdlon
    denominator = slat1 * slat2 + clat1 * clat2 * cdlon
    return np.arctan2(np.hypot(num1, num2), denominator) * _RAD2DEG * _DEG2ARCSEC


def group_by(arrs, key, func, out=None, check_grouped=True):
    """
    Group multiple NumPy arrays by arrs[0][key], assuming the key column
    is already grouped (equal keys are contiguous), but group blocks may
    appear in any order.

    If out is provided:
        func(row_view, *subarrs)
    If out is None:
        func(*subarrs)

    Parameters
    ----------
    arrs : list/tuple of ndarray
        Equal-length arrays. arrs[0] contains the grouping key.
    key : str
        Column name in arrs[0] to group by.
    func : callable
        Called either as func(row, *subarrs) or func(*subarrs).
    out : ndarray or None
        Optional preallocated structured output array.
    check_grouped : bool
        If True, verify that key values are grouped contiguously.

    Returns
    -------
    ndarray or dict
    """
    arr0 = arrs[0]
    keys = arr0[key]

    # ---------- Grouped-contiguous check ----------
    if check_grouped:
        seen = set()
        current = keys[0]
        seen.add(current)

        for i in range(1, len(keys)):
            k = keys[i]
            if k != current:
                # Key changed
                if k in seen:
                    raise ValueError(
                        f"Key '{key}' is not properly grouped. "
                        f"Value {k} reappears at index {i} "
                        f"after a different key was encountered."
                    )
                seen.add(k)
                current = k

    # ---------- Find true group boundaries ----------
    unique_keys, idx_start, counts = np.unique(keys, return_index=True, return_counts=True)
    idx_end = idx_start + counts
    n_groups = len(unique_keys)

    # ---------- Preallocated output path ----------
    if out is not None:
        if len(out) < n_groups:
            raise ValueError(f"Out array too small: need {n_groups}, have {len(out)}")

        for out_idx, (start, end) in enumerate(zip(idx_start, idx_end)):
            subarrs = tuple(a[start:end] for a in arrs)
            row = out[out_idx]  # writable structured scalar
            func(row, *subarrs)
            if out_idx % 100 == 0:
                print(f"[{datetime.now().isoformat()}] count={out_idx}")

        return out

    # ---------- Dict output path ----------
    results = {}
    for keyval, start, end in zip(unique_keys, idx_start, idx_end):
        subarrs = tuple(a[start:end] for a in arrs)
        results[keyval] = func(*subarrs)

    return results


def values_grouped(a: np.ndarray) -> bool:
    """
    Return True if each distinct value in 1D array `a`
    appears in a single contiguous block (all duplicates grouped).
    """
    a = np.asarray(a)
    if a.ndim != 1:
        raise ValueError("a must be 1D")
    if a.size == 0:
        return True

    # 1) True where a new group starts: first element, or value != previous
    group_starts = np.concatenate(([True], a[1:] != a[:-1]))

    # 2) Values for each group (one per contiguous block)
    group_vals = a[group_starts]

    # 3) Check that no value appears in more than one group
    #    i.e., all group_vals are unique
    return np.unique(group_vals).size == group_vals.size


def earthlocation_from_obscode(obscode: str) -> EarthLocation:
    """
    Convert an MPC observatory code (e.g. 'X05') to an EarthLocation,
    using MPC.get_observatory_codes() columns:
      Code, Longitude, cos, sin, Name.
    """
    tbl = MPC.get_observatory_codes()
    row = tbl[tbl["Code"] == obscode]
    if len(row) != 1:
        raise ValueError(f"Unknown or ambiguous obscode {obscode!r}")
    row = row[0]

    # Handle missing ground positions (spacecraft, etc.)
    if ma.is_masked(row["Longitude"]) or ma.is_masked(row["cos"]) or ma.is_masked(row["sin"]):
        raise ValueError(f"Obscode {obscode!r} has no ground position (spacecraft?)")

    lon = (row["Longitude"] * u.deg).to(u.rad).value  # radians
    rho_cosphi = float(row["cos"])
    rho_sinphi = float(row["sin"])

    # Geocentric Cartesian coordinates in Earth radii
    x_er = rho_cosphi * np.cos(lon)
    y_er = rho_cosphi * np.sin(lon)
    z_er = rho_sinphi

    # Convert Earth radii -> meters
    x = (x_er * R_earth).to(u.m)
    y = (y_er * R_earth).to(u.m)
    z = (z_er * R_earth).to(u.m)

    return EarthLocation.from_geocentric(x, y, z)


def observatory_barycentric_posvel(obscode: str, obstime: Time):
    """
    Barycentric (ICRS) position and velocity of an observatory given an
    MPC obscode, using JPL DE440 for the Earth ephemeris.

    Returns
    -------
    r_bary : Quantity, shape (3, ...)
        Barycentric position in AU.
    v_bary : Quantity, shape (3, ...)
        Barycentric velocity in AU/day.
    """
    loc = earthlocation_from_obscode(obscode)

    # Geocentric position & velocity of the site in GCRS (Earth center)
    obsgeoloc, obsgeovel = loc.get_gcrs_posvel(obstime)

    # Earth barycentric pos/vel in ICRS using DE440
    with solar_system_ephemeris.set("de440"):
        earth_pos, earth_vel = get_body_barycentric_posvel("earth", obstime)

    # ---- convert everything to SI ----
    # Earth (ICRS, barycentric)
    r_earth_si = earth_pos.xyz.to(u.m)
    v_earth_si = earth_vel.xyz.to(u.m / u.s)

    # Site (GCRS, geocentric)
    r_site_geo_si = getattr(obsgeoloc, "xyz", obsgeoloc).to(u.m)
    v_site_geo_si = getattr(obsgeovel, "xyz", obsgeovel).to(u.m / u.s)

    # ---- barycentric site vectors in SI ----
    r_site_bary_si = r_earth_si + r_site_geo_si
    v_site_bary_si = v_earth_si + v_site_geo_si

    # ---- convert to AU, AU/day ----
    r_site_bary = r_site_bary_si.to(u.au)
    v_site_bary = v_site_bary_si.to(u.au / u.day)

    return r_site_bary, v_site_bary


#
# Serialization
#


def struct_to_parquet(
    arr: np.ndarray,
    path: str,
    *,
    chunk_size: Optional[int] = None,
    row_group_size: Optional[int] = None,
) -> None:
    """
    Write a large NumPy structured array to a Parquet file using PyArrow.

    Designed for dtypes like dia_dtype / orbit_dtype with up to ~1e8 rows.
    """

    if arr.dtype.names is None:
        raise TypeError("struct_to_parquet expects a structured NumPy array (dtype.names is None).")

    n_rows = len(arr)
    if n_rows == 0:
        return

    # Heuristic default chunk size
    if chunk_size is None:
        if n_rows <= 10_000_000:
            chunk_size = n_rows
        else:
            chunk_size = 1_000_000

    def _numpy_to_arrow_array(col: np.ndarray, name: str) -> pa.Array:
        """
        Convert a 1D NumPy column view to a PyArrow Array.

        - Object / unicode / bytes → Arrow string(), with padding stripped
          for fixed-width 'S'/'U' dtypes.
        - Numeric types → Arrow infers type from NumPy.
        """
        kind = col.dtype.kind

        if kind == "O":
            # Already Python objects (str); Arrow will handle them fine.
            return pa.array(col, type=pa.string())

        if kind == "S":
            # Fixed-width bytes, padded with b"\\x00".
            # Decode + strip trailing NULs.
            # NOTE: adjust encoding if you know it's not ASCII/UTF-8.
            decoded = np.char.decode(col, "utf-8", errors="ignore")
            stripped = np.char.rstrip(decoded, "\x00")
            return pa.array(stripped, type=pa.string())

        if kind == "U":
            # Fixed-width unicode, padded with U+0000.
            stripped = np.char.rstrip(col, "\x00")
            return pa.array(stripped, type=pa.string())

        # Numeric / bool dtypes: let Arrow infer
        return pa.array(col)

    def _chunk_to_table(chunk: np.ndarray) -> pa.Table:
        arrays = []
        fields = []

        for name in chunk.dtype.names:
            col = chunk[name]
            arr_arrow = _numpy_to_arrow_array(col, name)
            arrays.append(arr_arrow)
            fields.append(pa.field(name, arr_arrow.type))

        schema = pa.schema(fields)
        return pa.Table.from_arrays(arrays, schema=schema)

    writer: Optional[pq.ParquetWriter] = None
    try:
        for start in range(0, n_rows, chunk_size):
            end = min(start + chunk_size, n_rows)
            chunk = arr[start:end]
            table = _chunk_to_table(chunk)

            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)
            writer.write_table(table, row_group_size=row_group_size)
    finally:
        if writer is not None:
            writer.close()


# Jupiter's semimajor axis in AU (J2000-ish)
A_JUP = 5.2044


def tisserand_jupiter(a, e, inc_deg, a_j=A_JUP):
    """
    Compute Tisserand parameter with respect to Jupiter.

    Parameters
    ----------
    a : float or ndarray
        Semimajor axis of the small body [AU].
    e : float or ndarray
        Eccentricity.
    inc_deg : float or ndarray
        Inclination [degrees], typically to the ecliptic.
    a_j : float
        Semimajor axis of Jupiter [AU]. Default ~5.2044.

    Returns
    -------
    T_J : float or ndarray
        Tisserand parameter with respect to Jupiter.
    """
    inc_rad = np.deg2rad(inc_deg)
    return (a_j / a) + 2.0 * np.cos(inc_rad) * np.sqrt((a / a_j) * (1.0 - e**2))


def unpack(df, to_numpy=True):
    """
    Return all DataFrame columns as a tuple.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe.
    to_numpy : bool, default True
        If True, return each column as a NumPy array.
        If False, return each column as a pandas Series.

    Returns
    -------
    tuple
        Tuple of columns in the original order.
    """
    if to_numpy:
        return tuple(df[col].to_numpy() for col in df.columns)
    else:
        return tuple(df[col] for col in df.columns)


def argjoin(a, v):
    """
    Perform an efficient inner join between two 1-D NumPy arrays, returning
    the index pairs that match by value.

    Parameters
    ----------
    a : ndarray
        The left-hand array to join on. Must be 1-dimensional.
    v : ndarray
        The right-hand array to join on. Must be 1-dimensional.

    Returns
    -------
    aidx : ndarray (int)
        Indices into `a` selecting the rows that participate in the join.
    vidx : ndarray (int)
        Indices into `v` selecting the corresponding matching rows.

        After the join:
            a[aidx] == v[vidx]
        is guaranteed to be true for all elements.

    Notes
    -----
    This function implements a pure NumPy equivalent of an SQL-style
    INNER JOIN on the key columns `a` and `v`.

    The algorithm:

    1. Sort `a` to produce a permutation `i` so that `a[i]` is sorted.
    2. Use `np.searchsorted(a[i], v)` to find, for each element of `v`,
       the candidate matching location in the sorted array.
    3. Map these positions back to the coordinates of the original array `a`
       using the permutation `i`.
    4. Filter out non-matches (values in `v` not present in `a`).
       The remaining pairs form the inner join.

    Complexity
    ----------
    Sorting:      O(len(a) log len(a))
    Searching:    O(len(v) log len(a))
    Total:        O(n log n)

    This is optimal for join-like operations on unsorted arrays in NumPy.

    Examples
    --------
    >>> a = np.array(["b", "a", "c", "b"])
    >>> v = np.array(["a", "b", "x", "b"])

    >>> aidx, vidx = argjoin(a, v)
    >>> a[aidx]
    array(['a', 'b', 'b'])
    >>> v[vidx]
    array(['a', 'b', 'b'])

    """
    # 1. Sort a, remembering the permutation
    i = np.argsort(a)
    ai = a[i]

    # 2. Locate each element of v within the sorted array
    idx = np.searchsorted(ai, v)

    # Clip to avoid out-of-range indices when v contains values > max(a)
    idx = np.clip(idx, 0, len(ai) - 1)

    # 3. Map positions in sorted array back to original array indices
    aidx_candidate = i[idx]

    # 4. Keep only true matches (this implements an INNER JOIN)
    mask = ai[idx] == v
    vidx = np.flatnonzero(mask)

    # Final matched indices in a
    aidx = aidx_candidate[vidx]

    assert np.all(a[aidx] == v[vidx])
    return aidx, vidx


#
# Forked process pools over contiguous chunks (ssobject and ssobservation
# --workers N > 1)
#
def fork_context():
    """The fork multiprocessing context, or None where fork is unavailable."""
    if "fork" not in multiprocessing.get_all_start_methods():
        return None
    return multiprocessing.get_context("fork")


def balanced_chunks(weights, n_chunks):
    """
    Split ``len(weights)`` items into at most ``n_chunks`` contiguous,
    non-empty ranges of about equal total weight. Returns a list of
    (start, end) index pairs covering all items in order.
    """
    n = len(weights)
    if n == 0:
        return []
    n_chunks = max(1, min(n_chunks, n))
    cum = np.cumsum(weights, dtype=np.float64)
    targets = cum[-1] * np.arange(1, n_chunks) / n_chunks
    cuts = np.searchsorted(cum, targets, side="left") + 1
    edges = np.unique(np.concatenate(([0], cuts, [n])))
    return [(int(a), int(b)) for a, b in zip(edges[:-1], edges[1:]) if b > a]


def run_chunks(func, chunks, workers, label, weights=None, unit="objects"):
    """
    Run ``func(start, end)`` for each (start, end) chunk in a forked
    process pool and return the results in chunk order.

    Prints progress once per finished chunk (with more than 200 chunks,
    about 200 times in all); the time left is estimated
    from the chunks' ``weights`` (default: their sizes), counting
    ``unit``. The first exception in any worker cancels the remaining
    chunks and is re-raised in the parent; a worker process that dies (e.g.
    killed for memory) raises BrokenProcessPool naming ``label`` and the
    chunks that hadn't finished.
    """
    if weights is None:
        weights = [e - s for s, e in chunks]
    total, total_weight = chunks[-1][1] - chunks[0][0], float(sum(weights))
    t0 = time.monotonic()
    results = [None] * len(chunks)
    done, done_weight, n_done = 0, 0.0, 0
    every = max(1, len(chunks) // 200)
    pool = ProcessPoolExecutor(max_workers=min(workers, len(chunks)), mp_context=fork_context())
    try:
        futures = {pool.submit(func, s, e): n for n, (s, e) in enumerate(chunks)}
        for fut in as_completed(futures):
            n = futures[fut]
            results[n] = fut.result()   # re-raises a worker's exception
            s, e = chunks[n]
            done += e - s
            done_weight += weights[n]
            n_done += 1
            if n_done % every and n_done < len(chunks):
                continue
            elapsed = time.monotonic() - t0
            left = elapsed * (total_weight - done_weight) / done_weight if done_weight else float("nan")
            print(f"[{label}] {done:,}/{total:,} {unit}, "
                  f"{elapsed:.1f} s elapsed, ~{left:.0f} s left", flush=True)
    except BrokenProcessPool as ex:
        pool.shutdown(wait=False, cancel_futures=True)
        lost = [chunks[n] for f, n in futures.items() if not f.done() or f.exception() is not None]
        raise BrokenProcessPool(f"[{label}] a worker process died (killed, e.g. for memory?); "
                                f"{len(lost)} chunk(s) unfinished, e.g. {lost[:5]}") from ex
    except BaseException:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown(wait=True)
    return results
