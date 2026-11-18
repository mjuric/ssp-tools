# Solar elongation reference values

`horizons_sot.csv`: 319 rows of 81 objects. Every value comes from JPL Horizons; the file holds no Rubin measurements.

| Column | Meaning |
|---|---|
| `designation` | the object's MPC designation |
| `mjd_tai` | the time, MJD (TAI) |
| `ra`, `dec` | Horizons' astrometric RA and Dec of the object [deg, ICRF] |
| `sot` | Horizons' S-O-T: the Sun–observer–target angle, i.e. the solar elongation [deg, 0.001° or better] |

Where the values come from:
- The rows are the 2026-11-16 delivery QA's Horizons sample (from `horizons_03_compare.py`), queried with `EPHEM_TYPE=OBSERVER` from Rubin's site `X05` (`CENTER='X05@399'`), with `QUANTITIES='1,3,9,19,20,23,24,27,43'`, `REF_SYSTEM=ICRF`, `EXTRA_PREC=YES`, and `TIME_TYPE=TT`.
- The fixture was made from those cached responses, so making it took no new Horizons queries.

`ssp.util.solar_elongation_ndarray`, given `ra`, `dec` and `mjd_tai`, must reproduce `sot` to within 0.001°. (`sot` is the apparent elongation, with aberration.)
