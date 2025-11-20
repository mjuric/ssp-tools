import pandas as pd
import numpy as np
from . import photfit
from . import util
from . import schema
from .flags import flags_phot
from .moid import MOIDSolver, earth_orbit_J2000
import argparse
import sys

def nJy_to_mag(f_njy):
    """
    Convert flux density in nanoJanskys (nJy) to AB magnitude.

    Parameters
    ----------
    f_njy : float or array-like
        Flux density in nanoJanskys.

    Returns
    -------
    float or array-like
        AB magnitude corresponding to the input flux density.
    """
    return 31.4 - 2.5 * np.log10(f_njy)

def nJy_err_to_mag_err(f_njy, f_err_njy):
    """
    Convert flux error in nanoJanskys to magnitude error.

    Parameters
    ----------
    f_njy : float
        Flux in nanoJanskys.
    f_err_njy : float
        Flux error in nanoJanskys.

    Returns
    -------
    float
        Magnitude error.
    """
    return 1.085736 * (f_err_njy / f_njy)

def compute_ssobject_entry(row, sss):
    # just verify we didn't screw up something
    assert sss["ssObjectId"].nunique() == 1

    # start out optimistic
    row['flags'] = 0

    # Metadata columns
    row["ssObjectId"] = sss["ssObjectId"].iloc[0]
    row["firstObservationDate"] = sss["dia_midpointMjdTai"].min()
    # FIXME: here I assume we discover everything 7 days after first obsv. we should really pull this out of the obs_sbn table.
    row["discoverySubmissionDate"] = row["firstObservationDate"] + 7.
    row["arc"] = np.ptp(sss["dia_midpointMjdTai"])
    provID = row["unpacked_primary_provisional_designation"] = sss["unpacked_primary_provisional_designation"].iloc[0]

    # observation counts
    row["nObs"] = len(sss)

    # per band entries
    for band in "ugrizy":
        df = sss[sss["dia_band"] == band]

        # set defaults for this band (NULL)
        row[f'{band}_Chi2'] = -1
        row[f'{band}_G12'] = np.nan
        row[f'{band}_G12Err'] = np.nan
        row[f'{band}_H'] = np.nan
        row[f'{band}_H_{band}_G12_Cov'] = np.nan
        row[f'{band}_HErr'] = np.nan
        row[f'{band}_nObsUsed'] = -1

        nBandObs = len(df)
        row[f"{band}_nObs"] = nBandObs
        if nBandObs > 0:
            paMin, paMax = df["phaseAngle"].min(), df["phaseAngle"].max()
            row[f"{band}_phaseAngleMin"] = paMin
            row[f"{band}_phaseAngleMax"] = paMax

            if nBandObs > 1:
                # do the absmag/slope fits, if there are at least two data points
                H, G12, sigmaH, sigmaG12, covHG12, chi2dof, nobsv = \
                    photfit.fitHG12(df["dia_psfMag"], df["dia_psfMagErr"], df["phaseAngle"], df["topocentricDist"], df["heliocentricDist"])
                nDof = 2
                #print(provID, band, H, G12, sigmaH, sigmaG12, covHG12, chi2dof, nobsv)

                # mark the fit failure
                if np.isnan(G12):
                    row['flags'] |= flags_phot[band]

                row[f'{band}_Chi2'] = chi2dof * nDof
                row[f'{band}_G12'] = G12
                row[f'{band}_G12Err'] = sigmaG12
                row[f'{band}_H'] = H
                row[f'{band}_H_{band}_G12_Cov'] = covHG12
                row[f'{band}_HErr'] = sigmaH
                row[f'{band}_nObsUsed'] = nobsv
            else:
                row[f'{band}_nObsUsed'] = 0

    # Extendedness
    row["extendednessMin"] = sss["dia_extendedness"].min()
    row["extendednessMax"] = sss["dia_extendedness"].max()
    row["extendednessMedian"] = sss["dia_extendedness"].median()

def compute_ssobject(sss, dia, mpcorb):
    #
    # Construct a joined/expanded SSSource table
    #

    # join DiaSource bits
    num = len(sss)
    sss = sss.merge(dia.add_prefix("dia_"), left_on="diaSourceId", right_on="dia_diaSourceId", how="inner")
    assert num == len(sss), f"Mismatch in number of SSSource rows after DiaSource join {num} vs {len(sss)}"
    del sss["dia_diaSourceId"]

    # add magnitude columns
    sss["dia_psfMag"] = nJy_to_mag(sss["dia_psfFlux"])
    sss["dia_psfMagErr"] = nJy_err_to_mag_err(sss["dia_psfFlux"], sss["dia_psfFluxErr"])

    #
    # Pre-create the empty array
    #
    totalNumObjects = np.unique(sss["ssObjectId"]).size
    obj = np.zeros(totalNumObjects, dtype=schema.ssObjectDtype)

    #
    # compute per-object quantities
    #
    util.group_by([sss], "ssObjectId", compute_ssobject_entry, out=obj)

    #
    # compute columns that can be efficiently computed in a vector fashon
    #

    # Tisserand J
    vals = obj["unpacked_primary_provisional_designation"].astype("U")  # bytes → unicode str
    mask = mpcorb["unpacked_primary_provisional_designation"].isin(vals)
    q, e, i, node, argperi = util.unpack(mpcorb["q e i node argperi".split()][mask])
    a = q / (1. - e)
    obj["tisserand_J"] = util.tisserand_jupiter(a, e, i)

    # MOID computation
    solver = MOIDSolver()
    for i, el_obj in enumerate(zip(a, e, i, node, argperi)):
        (moid, deltaV, eclon, trueEarth, trueObject) = solver.compute(earth_orbit_J2000(), el_obj)
        row = obj[i]
        row["MOIDEarth"] = moid
        row["MOIDEarthDeltaV"] = deltaV
        row["MOIDEarthEclipticLongitude"] = eclon
        row["MOIDEarthTrueAnomaly"] = trueEarth
        row["MOIDEarthTrueAnomalyObject"] = trueObject

    return obj

def main():
    """
    CLI entry point for building SSObject table from SSSource, DiaSource, and MPC orbit data.
    """
    parser = argparse.ArgumentParser(
        description="Build SSObject table from SSSource, DiaSource, and MPC orbit Parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ssp-build-ssobject sssource.parquet dia_sources.parquet mpc_orbits.parquet --output ssobject.parquet
        """
    )

    parser.add_argument(
        "sssource_parquet",
        help="Path to SSSource Parquet file"
    )
    parser.add_argument(
        "diasource_parquet",
        help="Path to DiaSource Parquet file"
    )
    parser.add_argument(
        "mpcorb_parquet",
        help="Path to MPC orbits Parquet file"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Path to output SSObject Parquet file"
    )
    parser.add_argument(
        "--reraise",
        action="store_true",
        help="Re-raise exceptions instead of exiting gracefully (for debugging)"
    )

    args = parser.parse_args()

    try:
        # Load SSSource
        print(f"Loading SSSource from {args.sssource_parquet}...")
        sss = pd.read_parquet(args.sssource_parquet, engine="pyarrow", dtype_backend="pyarrow").reset_index(drop=True)
        num = len(sss)
        print(f"Loaded {num:,} SSSource rows")

        # Load DiaSource with required columns
        dia_columns = ["diaSourceId", "midpointMjdTai", "ra", "dec", "extendedness", "band", "psfFlux", "psfFluxErr"]
        print(f"Loading DiaSource from {args.diasource_parquet}...")
        dia = pd.read_parquet(args.diasource_parquet, engine="pyarrow", dtype_backend="pyarrow", columns=dia_columns).reset_index(drop=True)
        print(f"Loaded {len(dia):,} DiaSource rows")

        # Ensure diaSourceId is uint64
        assert np.all(dia["diaSourceId"] >= 0)
        dia["diaSourceId"] = dia["diaSourceId"].astype("uint64[pyarrow]")

        # Load MPC orbits
        mpcorb_columns = ["unpacked_primary_provisional_designation", "a", "q", "e", "i", "node", "argperi", "peri_time",
                         "mean_anomaly", "epoch_mjd", "h", "g"]
        print(f"Loading MPC orbits from {args.mpcorb_parquet}...")
        mpcorb = pd.read_parquet(args.mpcorb_parquet, engine="pyarrow", dtype_backend="pyarrow",
                                columns=mpcorb_columns).reset_index(drop=True)
        print(f"Loaded {len(mpcorb):,} MPC orbit rows")

        # Compute SSObject
        print("Computing SSObject data...")
        obj = compute_ssobject(sss, dia, mpcorb)

        # Save result
        print(f"Saving {len(obj):,} SSObject rows to {args.output}...")
        util.struct_to_parquet(obj, args.output)

        print(f"Success! Created SSObject with {len(obj):,} objects")
        print(f"Row size: {obj.dtype.itemsize:,} bytes, Total size: {obj.nbytes:,} bytes")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)

if __name__ == "__main__":
    input_dir = "./analysis/inputs"
    output_dir = "./analysis/outputs"

    #
    # Loads
    #

    # load SSObject
    sss = pd.read_parquet(f'{output_dir}/sssource.parquet', engine="pyarrow",  dtype_backend="pyarrow").reset_index(drop=True)
    num = len(sss)

    # load corresponding DiaSource
    dia_columns = ["diaSourceId", "midpointMjdTai", "ra", "dec", "extendedness", "band", "psfFlux", "psfFluxErr"]
    dia = pd.read_parquet(f'{input_dir}/dia_sources.parquet', engine="pyarrow",  dtype_backend="pyarrow", columns=dia_columns).reset_index(drop=True)

    # FIXME: I'm not sure why the datatype is int and not uint here. Investigate upstream...
    assert np.all(dia["diaSourceId"] >= 0)
    dia["diaSourceId"] = dia["diaSourceId"].astype("uint64[pyarrow]")

    # Load mpcorb
    mpcorb = pd.read_parquet(f'{input_dir}/mpc_orbits.parquet', engine="pyarrow",  dtype_backend="pyarrow",
                             columns=["unpacked_primary_provisional_designation", "a", "q", "e", "i", "node", "argperi", "peri_time",
                                      "mean_anomaly", "epoch_mjd", "h", "g"]
            ).reset_index(drop=True)

    #
    # Business logic
    #
    obj = compute_ssobject(sss, dia, mpcorb)

    #
    # Save
    #
    util.struct_to_parquet(obj, f"{output_dir}/ssobject.parquet")

    print(f"row_length={obj.dtype.itemsize:,} bytes, rows={len(obj):,}, {obj.nbytes:,} bytes total")
