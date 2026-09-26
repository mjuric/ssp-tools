import argparse
import sys

from astropy.coordinates import (
    SkyCoord,
    HeliocentricEclipticIAU76,
)
from astropy.time import Time
import astropy.units as u
from functools import partial
import numpy as np
import pandas as pd

from . import util, schema
from .photfit import hg_V_mag
from .ephem_assist import compute_ephemerides_one, open_ephem


def compute_sssource_entry(sss, assoc, mpcorb, dia, ephem):
    """Fill the ephemeris-derived SSSource columns for one object.

    ``mpcorb`` must be indexed by unpacked_primary_provisional_designation;
    ``assoc`` must carry the observer's barycentric state per observation
    (obs_x/y/z [AU], obs_vx/vy/vz [km/s]).
    """

    # extract only the subset of observations related to this object
    dia = dia.iloc[assoc["dia_index"]]

    # just verify we didn't screw up something
    assert np.all(sss["ssObjectId"] == sss["ssObjectId"][0])
    assert len(dia) == len(sss)

    provID = sss["designation"][0]
    ephTimes = Time(dia["midpointMjdTai"].values, format="mjd", scale="tai")
    e = compute_ephemerides_one(
        provID,
        ephTimes,
        None,
        ephem,
        row=mpcorb.loc[provID],
        obs_pos=assoc[["obs_x", "obs_y", "obs_z"]].to_numpy().T,
        obs_vel=assoc[["obs_vx", "obs_vy", "obs_vz"]].to_numpy().T,
    )

    sss["ephRateRa"] = e.mu_lon
    sss["ephRateDec"] = e.mu_lat
    sss["ephRate"] = e.mu_total

    # Heliocentric and topocentric vectors are at light-emission time, per
    # the SSSource schema, following JPL Horizons conventions (see
    # ssp.ephem_assist.EphResult).
    eph = SkyCoord(ra=e.ra_deg * u.deg, dec=e.dec_deg * u.deg, frame="icrs")

    sss["ephRa"] = eph.ra.deg
    sss["ephDec"] = eph.dec.deg
    obsv = SkyCoord(ra=dia["ra"], dec=dia["dec"], unit="deg", frame="icrs")

    sss["ephOffsetDec"] = (dia["dec"].to_numpy() - sss["ephDec"]) * 3600
    sss["ephOffsetRa"] = (dia["ra"].to_numpy() - sss["ephRa"]) * np.cos(np.deg2rad(sss["ephDec"])) * 3600
    sss["ephOffset"] = eph.separation(obsv).arcsec

    # Compute heliocentric position components
    sss["helio_x"] = e.helio_pos[0]
    sss["helio_y"] = e.helio_pos[1]
    sss["helio_z"] = e.helio_pos[2]
    sss["helioRange"] = np.sqrt(sss["helio_x"] ** 2 + sss["helio_y"] ** 2 + sss["helio_z"] ** 2)

    # Compute heliocentric velocity components
    sss["helio_vx"] = e.helio_vel[0]
    sss["helio_vy"] = e.helio_vel[1]
    sss["helio_vz"] = e.helio_vel[2]
    sss["helio_vtot"] = np.sqrt(sss["helio_vx"] ** 2 + sss["helio_vy"] ** 2 + sss["helio_vz"] ** 2)

    # Compute heliocentric radial velocity: dot product of velocity
    # and unit position vector
    sss["helioRangeRate"] = (
        sss["helio_vx"] * sss["helio_x"] + sss["helio_vy"] * sss["helio_y"] + sss["helio_vz"] * sss["helio_z"]
    ) / sss["helioRange"]

    # Compute topocentric position components
    sss["topo_x"] = e.topo_pos[0]
    sss["topo_y"] = e.topo_pos[1]
    sss["topo_z"] = e.topo_pos[2]
    sss["topoRange"] = np.sqrt(sss["topo_x"] ** 2 + sss["topo_y"] ** 2 + sss["topo_z"] ** 2)

    # Compute topocentric velocity components
    sss["topo_vx"] = e.topo_vel[0]
    sss["topo_vy"] = e.topo_vel[1]
    sss["topo_vz"] = e.topo_vel[2]
    sss["topo_vtot"] = np.sqrt(sss["topo_vx"] ** 2 + sss["topo_vy"] ** 2 + sss["topo_vz"] ** 2)

    # Compute topocentric radial velocity: dot product of velocity
    # and unit position vector
    sss["topoRangeRate"] = (
        sss["topo_vx"] * sss["topo_x"] + sss["topo_vy"] * sss["topo_y"] + sss["topo_vz"] * sss["topo_z"]
    ) / sss["topoRange"]

    sss["phaseAngle"] = e.phase_angle

    sss["ephVmag"] = hg_V_mag(e.H, e.G, sss["helioRange"], sss["topoRange"], e.phase_angle)

    max_sep = np.max(sss["ephOffset"])
    med_sep = np.median(sss["ephOffset"])
    print(f"{provID}: max/median separation: {max_sep:.4f}, {med_sep:.4f} arcsec")


def build_sssource(input_dir, output_dir, max_objects=None, dia_sample_frac=1.0, seed=42):
    """Build ``{output_dir}/sssource.parquet`` from the DiaSource, MPC
    observation (obs_sbn), identification and orbit tables in ``input_dir``.

    ``max_objects`` and ``dia_sample_frac`` subsample the inputs, for
    testing.
    """
    dia = pd.read_parquet(
        f"{input_dir}/dia_sources.parquet", engine="pyarrow", dtype_backend="pyarrow"
    ).reset_index(drop=True)
    if dia_sample_frac < 1.0:
        # Testing aid: drop some DIA sources and shuffle the rest, to
        # exercise the association logic with missing / unsorted indices.
        dia = dia.sample(frac=dia_sample_frac, random_state=seed).reset_index(drop=True)

    det = pd.read_parquet(
        f"{input_dir}/obs_sbn.parquet", engine="pyarrow", dtype_backend="pyarrow"
    ).reset_index()

    if max_objects is not None:
        # Testing aid: keep only a random subset of objects.
        sampled_provids = det["provid"].drop_duplicates().sample(max_objects, random_state=seed)
        det = det[det["provid"].isin(sampled_provids)].reset_index()
    print(f"{len(det):,} MPC observations")

    # DiaSources from extract-submitted-sources carry the obs_sbn obsid
    # they were resolved from; link on it. Otherwise (Butler extraction)
    # obssubid is the bare diaSourceId.
    by_obsid = "obsid" in dia.columns
    if not by_obsid:
        det["obssubid"] = det["obssubid"].astype(int)
    det = det[
        (["obsid"] if by_obsid else []) + (["trkid"] if "trkid" in det.columns else []) + [
            "trksub",
            "obssubid",
            "provid",
            "permid",
            "submission_id",
            "ra",
            "dec",
            "obstime",
            "designation_asterisk",
        ]
    ].copy()

    # verify types didn't get mangled somewhere along the way
    # from the database to here
    expect_dtypes = dict(
        obsid="string[pyarrow]",
        trkid="string[pyarrow]",
        trksub="string[pyarrow]",
        obssubid="string[pyarrow]" if by_obsid else "int64",
        provid="string[pyarrow]",
        permid="string[pyarrow]",
        submission_id="string[pyarrow]",
        ra="double[pyarrow]",
        dec="double[pyarrow]",
        obstime="timestamp[us][pyarrow]",
        designation_asterisk="bool[pyarrow]",
    )

    for col in det.columns:
        assert det[col].dtype == expect_dtypes[col]

    # create the association side table. From extract-submitted-sources,
    # dia has one row per obs_sbn row (obsid is unique), so each obs_sbn row
    # gets one SSSource row; a source claimed by several (both endpoints of
    # a trail, or repeated submissions) has one of them marked primary.
    if by_obsid:
        assoc = (
            dia[["diaSourceId", "obsid"]]
            .reset_index()
            .merge(det.add_prefix("mpc_"), left_on="obsid", right_on="mpc_obsid", how="inner")
        )
    else:
        assoc = (
            dia[["diaSourceId"]]
            .reset_index()
            .merge(det.add_prefix("mpc_"), left_on="diaSourceId", right_on="mpc_obssubid", how="inner")
        )
    assoc.rename(columns={"index": "dia_index"}, inplace=True)

    # verify all went well
    assert np.all(dia["diaSourceId"].iloc[assoc["dia_index"]].to_numpy() == assoc["diaSourceId"].to_numpy())

    # verify contents of the association table
    if by_obsid:
        # extract-submitted-sources already verified each match against
        # the PSF *or trail* centroid and the midpoint of -A/-B endpoint
        # pairs, neither of which assoc_validate (PSF position vs. the
        # submitted row) can reproduce; check its recorded offsets against
        # the same tolerances instead.
        util.assoc_validate_recorded(dia, assoc)
    else:
        util.assoc_validate(dia, assoc)

    # obs_sbn also holds observations of unidentified tracklets (status
    # 'I', no provid nor permid). They are in SSSource too -- they were
    # sent to and accepted by the MPC -- with ssObjectId 0, no designation
    # and no orbit-derived columns. Set them aside while resolving the
    # designations of the rest.
    if by_obsid:
        undesignated = assoc["mpc_provid"].isna() & assoc["mpc_permid"].isna()
        und = assoc[undesignated].reset_index(drop=True)
        assoc = assoc[~undesignated].reset_index(drop=True)

    totalNumObs = len(assoc)

    numid = pd.read_parquet(
        f"{input_dir}/numbered_identifications.parquet",
        engine="pyarrow",
        columns=["permid", "unpacked_primary_provisional_designation"],
        dtype_backend="pyarrow",
    ).reset_index(drop=True)
    curid = pd.read_parquet(
        f"{input_dir}/current_identifications.parquet",
        engine="pyarrow",
        dtype_backend="pyarrow",
        columns=[
            "unpacked_primary_provisional_designation",
            "unpacked_secondary_provisional_designation",
            "packed_primary_provisional_designation",
        ],
    ).reset_index(drop=True)

    # First step: some numbered objects in `obs_sbn` don't have their
    # provID set. Restore it.
    df = assoc[["mpc_provid", "mpc_permid"]].merge(numid, left_on="mpc_permid", right_on="permid", how="left")
    assert len(df) == len(assoc)

    assoc["mpc_provid"] = assoc["mpc_provid"].where(
        assoc["mpc_provid"].notna(), df["unpacked_primary_provisional_designation"]
    )

    assert not assoc["mpc_provid"].isna().any()
    assert len(assoc) == totalNumObs

    # Second step: update provisional designations with the primary ones.

    df = assoc[["mpc_provid"]].merge(
        curid, left_on="mpc_provid", right_on="unpacked_secondary_provisional_designation", how="inner"
    )
    # (the assignments below align on the index: a missing designation
    # would silently shift every later row)
    assert len(df) == len(assoc), (
        f"{assoc['mpc_provid'].nunique() - df['mpc_provid'].nunique():,} designations "
        f"({len(assoc) - len(df):,} observations) missing from current_identifications"
    )
    assoc["mpc_provid"] = df["unpacked_primary_provisional_designation"]
    assoc["mpc_packed"] = df["packed_primary_provisional_designation"]

    assert len(assoc) == totalNumObs

    mpcorb = pd.read_parquet(
        f"{input_dir}/mpc_orbits.parquet",
        engine="pyarrow",
        dtype_backend="pyarrow",
        columns=[
            "unpacked_primary_provisional_designation",
            "packed_primary_provisional_designation",
            "a",
            "q",
            "e",
            "i",
            "node",
            "argperi",
            "peri_time",
            "mean_anomaly",
            "epoch_mjd",
            "h",
            "g",
        ],
    ).set_index("unpacked_primary_provisional_designation", drop=False, verify_integrity=True)

    # Rows without an orbit to compute ephemerides from: the undesignated
    # ones and, on the obsid path, designated objects missing from
    # mpc_orbits (which the Butler path treats as an error).
    assoc["no_orbit"] = False
    if by_obsid:
        assoc["no_orbit"] = ~assoc["mpc_provid"].isin(mpcorb.index)
        missing = assoc.loc[assoc["no_orbit"], "mpc_provid"]
        print(f"{len(und):,} observations of undesignated objects; {len(missing):,} observations of "
              f"{missing.nunique():,} designated objects without an orbit: {sorted(missing.unique())[:10]}")
        und["no_orbit"] = True
        assoc = pd.concat([assoc, und], ignore_index=True)
        totalNumObs = len(assoc)

    # sort the association table by object, those without an orbit last
    assoc.sort_values(["no_orbit", "mpc_provid"], inplace=True)
    n_orbit = int(np.sum(~assoc["no_orbit"].to_numpy(dtype=bool)))

    # create the output array for SSSource, plus the DiaSource collection
    # (from extract-submitted-sources; null for Butler DiaSources), which
    # together with diaSourceId identifies the source, and the submitted
    # tracklet: (submission_id, trksub), and MPC's finer trkid. These group
    # the detections of undesignated objects. On the obsid path also the
    # obsid (the key SSObject joins DiaSource on) and whether this row is
    # the primary one of its source (the one SSObject counts).
    tracklet = ("submission_id", "trksub", "trkid")
    sss = np.zeros(totalNumObs, dtype=np.dtype(
        schema.SSSourceDtype.descr + [("collection", object)] + [(c, object) for c in tracklet]
        + [("obsid", object), ("primary", bool)]))

    #
    # construct SSSource -- start with easily vectorizable columns
    #
    sss["diaSourceId"] = assoc["diaSourceId"].values
    # (ssObjectId 0 and an empty designation for undesignated objects)
    has_id = assoc["mpc_packed"].notna().to_numpy(dtype=bool)
    sss["ssObjectId"][has_id] = util.packed_ascii_to_uint64_le(assoc["mpc_packed"][has_id])
    sss["designation"] = assoc["mpc_provid"].fillna("")
    if "collection" in dia.columns:
        sss["collection"] = dia["collection"].iloc[assoc["dia_index"]].to_numpy(dtype=object, na_value=None)
    else:
        sss["collection"] = None
    if by_obsid:
        sss["obsid"] = dia["obsid"].iloc[assoc["dia_index"]].to_numpy(dtype=object)
        sss["primary"] = dia["primary"].iloc[assoc["dia_index"]].to_numpy(dtype=bool)
    else:
        sss["obsid"] = None
        sss["primary"] = True
    for c in tracklet:
        # (on the obsid path from dia: the obs_sbn row it is linked to)
        src = dia[c].iloc[assoc["dia_index"]] if by_obsid else assoc.get(f"mpc_{c}")
        sss[c] = None if src is None else src.to_numpy(dtype=object, na_value=None)

    df = dia[["ra", "dec", "midpointMjdTai"]].iloc[assoc["dia_index"]]
    ra, dec, t = (
        df["ra"].to_numpy(),
        df["dec"].to_numpy(),
        Time(df["midpointMjdTai"].to_numpy(), format="mjd", scale="tai"),
    )

    sss["elongation"] = util.solar_elongation_ndarray(ra, dec, t)

    # Observer barycentric state for every observation, carried per row of
    # assoc so compute_sssource_entry gets its object's slice. It is
    # computed once per unique time (all sources from a visit share one
    # midpointMjdTai) in one vectorized call: the computation costs ~65 us
    # per time plus a large fixed overhead per call.
    tu, inv = np.unique(t.tai.mjd, return_inverse=True)
    robs, vobs = util.observatory_barycentric_posvel("X05", Time(tu, format="mjd", scale="tai"))
    robs = robs.to_value(u.au)[:, inv]
    vobs = vobs.to_value(u.km / u.s)[:, inv]
    for k, c in enumerate("xyz"):
        assoc[f"obs_{c}"] = robs[k]
        assoc[f"obs_v{c}"] = vobs[k]

    # FIXME: verify these coordinate transforms replicate IAU76 at JPL
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="hcrs")
    ecl = p.transform_to(HeliocentricEclipticIAU76)
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="icrs")
    gal = p.transform_to("galactic")

    sss["eclLambda"] = ecl.lon
    sss["eclBeta"] = ecl.lat
    sss["galLon"] = gal.l
    sss["galLat"] = gal.b

    # JPL planet and ASSIST asteroid ephemeris files, from the
    # SSP_ASSIST_PLANETS and SSP_ASSIST_ASTEROIDS environment variables.
    ephem = open_ephem()

    # compute_sssource_entry slices DiaSource rows per object; give it only
    # the columns it uses, numpy-backed, as taking rows of all ~85
    # pyarrow-backed columns dominated the per-object cost.
    dia_eph = pd.DataFrame({c: dia[c].to_numpy() for c in ("midpointMjdTai", "ra", "dec")})

    # ephemerides for the objects with orbits (the first n_orbit rows);
    # every orbit-derived column of the rest is NaN.
    util.group_by(
        [sss[:n_orbit], assoc.iloc[:n_orbit]], "ssObjectId",
        partial(compute_sssource_entry, mpcorb=mpcorb, dia=dia_eph, ephem=ephem),
    )
    measured = ("elongation", "eclLambda", "eclBeta", "galLon", "galLat")
    for name in sss.dtype.names:
        if sss.dtype[name].kind == "f" and name not in measured:
            sss[name][n_orbit:] = np.nan

    totalNumObjects = np.unique(sss["ssObjectId"][sss["ssObjectId"] != 0]).size
    print(f"{totalNumObjects:,} unique objects with {len(sss):,} total observations "
          f"({np.sum(sss['ssObjectId'] == 0):,} of them undesignated, "
          f"{len(sss) - n_orbit:,} without an orbit).")

    util.struct_to_parquet(sss, f"{output_dir}/sssource.parquet")


def main():
    parser = argparse.ArgumentParser(
        description="Build the SSSource table from DiaSource and MPC Parquet files",
        epilog=(
            "Reads dia_sources, obs_sbn, numbered_identifications, "
            "current_identifications and mpc_orbits .parquet files from the input "
            "directory and writes sssource.parquet to the output directory. The ASSIST "
            "ephemeris files are taken from the SSP_ASSIST_PLANETS and "
            "SSP_ASSIST_ASTEROIDS environment variables."
        ),
    )
    parser.add_argument("--input-dir", default="./analysis/inputs", help="Input directory (default: %(default)s)")
    parser.add_argument("--output-dir", default="./analysis/outputs", help="Output directory (default: %(default)s)")
    parser.add_argument(
        "--max-objects", type=int, default=None,
        help="Process only this many randomly chosen objects (default: all)",
    )
    parser.add_argument(
        "--dia-sample-frac", type=float, default=1.0,
        help="Randomly keep this fraction of DIA sources, shuffled (default: %(default)s)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for subsampling (default: %(default)s)")
    parser.add_argument(
        "--reraise", action="store_true",
        help="Re-raise exceptions instead of exiting gracefully (for debugging)",
    )
    args = parser.parse_args()

    try:
        build_sssource(
            args.input_dir, args.output_dir,
            max_objects=args.max_objects, dia_sample_frac=args.dia_sample_frac, seed=args.seed,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
