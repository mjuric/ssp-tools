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
    assert np.all(dia["ssObjectId"] == dia["ssObjectId"].iloc[0])
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


if __name__ == "__main__":
    input_dir = "./analysis/inputs"
    output_dir = "./analysis/outputs"

    dia = pd.read_parquet(
        f"{input_dir}/dia_sources.parquet", engine="pyarrow", dtype_backend="pyarrow"
    ).reset_index(drop=True)
    # DEBUG: while debugging, remove some indices and resort the array
    dia = dia.sample(frac=0.9, random_state=42).reset_index(drop=True)

    det = pd.read_parquet(
        f"{input_dir}/obs_sbn.parquet", engine="pyarrow", dtype_backend="pyarrow"
    ).reset_index()

    # DEBUG: cut this down to a much smaller table
    sampled_provids = det["provid"].drop_duplicates().sample(10, random_state=42)
    det = det[det["provid"].isin(sampled_provids)].reset_index()
    print(len(det))

    # FIXME: this will have to check if the ID's are IAU-style
    # (with string prefixes)
    det["obssubid"] = det["obssubid"].astype(int)
    det = det[
        [
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
        trksub="string[pyarrow]",
        obssubid="int64",
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

    # create the association side table
    assoc = (
        dia[["diaSourceId"]]
        .reset_index()
        .merge(det.add_prefix("mpc_"), left_on="diaSourceId", right_on="mpc_obssubid", how="inner")
    )
    assoc.rename(columns={"index": "dia_index"}, inplace=True)

    # verify all went well
    assert np.all(dia["diaSourceId"].iloc[assoc["dia_index"]].to_numpy() == assoc["diaSourceId"].to_numpy())

    # verify contents of the association table
    util.assoc_validate(dia, assoc)

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
    assoc["mpc_provid"] = df["unpacked_primary_provisional_designation"]
    assoc["mpc_packed"] = df["packed_primary_provisional_designation"]

    assert len(assoc) == totalNumObs

    # sort the association table by object
    assoc.sort_values(["mpc_provid"], inplace=True)

    # create the output array for SSSource
    sss = np.zeros(totalNumObs, dtype=schema.SSSourceDtype)
    sss.dtype.itemsize, len(sss), f"{sss.nbytes:,}"

    #
    # construct SSSource -- start with easily vectorizable columns
    #
    sss["diaSourceId"] = assoc["diaSourceId"].values
    sss["ssObjectId"] = util.packed_ascii_to_uint64_le(assoc["mpc_packed"])
    sss["designation"] = assoc["mpc_provid"]

    df = dia[["ra", "dec", "midpointMjdTai"]].iloc[assoc["dia_index"]]
    ra, dec, t = (
        df["ra"].to_numpy(),
        df["dec"].to_numpy(),
        Time(df["midpointMjdTai"].to_numpy(), format="mjd", scale="tai"),
    )

    sss["elongation"] = util.solar_elongation_ndarray(ra, dec, t)

    # Observer barycentric state for every observation, in one vectorized
    # call (it has a large fixed cost per call), carried per row of assoc so
    # compute_sssource_entry gets its object's slice.
    robs, vobs = util.observatory_barycentric_posvel("X05", t)
    robs = robs.to_value(u.au)
    vobs = vobs.to_value(u.km / u.s)
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

    # JPL planet and ASSIST asteroid ephemeris files, from the
    # SSP_ASSIST_PLANETS and SSP_ASSIST_ASTEROIDS environment variables.
    ephem = open_ephem()

    util.group_by(
        [sss, assoc], "ssObjectId", partial(compute_sssource_entry, mpcorb=mpcorb, dia=dia, ephem=ephem)
    )

    totalNumObjects = np.unique(sss["ssObjectId"]).size
    print(f"{totalNumObjects:,} unique objects with {len(sss):,} total observations.")

    util.struct_to_parquet(sss, f"{output_dir}/sssource.parquet")
