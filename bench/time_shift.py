"""The SSObservation/NearbySSO time-shift allowance
(docs/design/shutter-timing.md, "SSObservation vs. NearbySSO").

Until AP corrects DiaSource times, SSObservation predicts at the
shutter-corrected ``midpointMjdTai`` and NearbySSO at the DiaSource's own
(visit) time. At the same DiaSource the two predictions differ by about the
object's rate times
Δt = SSObservation.midpointMjdTai - DiaSource.midpointMjdTai: within 0.24 s for
most visits, but up to ~2 s for header-timed (degraded) ones. Every
allowance uses each row's own Δt; nothing assumes a bound on it below
DT_MAX_S, a sanity limit.
The checks that compare the two tables add the allowances below on top of
their own tolerances. Where Δt == 0 every allowance is exactly 0, so those
rows are held to today's tolerances.

Rules (Δt in days; every allowance is 0 where Δt == 0 or Δt is unknown):

- position (ephRa, ephDec): the motion over Δt is taken out first, as a
  vector: the residual
  |(NearbySSO - SSObservation) + (ephRateRa, ephRateDec) Δt|
  (the two tables' mean rates; on the tangent plane) must be within the
  check's tolerance + RESID_REL_MARGIN |rate| |Δt| + SAFETY x α Δt^2 / 2
  + ABS_MARGIN_MAS, α the angular-acceleration bound below (where SSObservation
  has no ranges: REL_MARGIN |rate| |Δt| + ABS_MARGIN_MAS). (At most
  |rate| |Δt| (1 + REL_MARGIN) + ABS_MARGIN_MAS on top of the tolerance,
  and much stricter, along and across the track.) At Δt = 0 the plain
  separation is compared, as before;
- ephOffset, and the decisions "beyond the match radius" and "a nearer
  object" [mas]: |rate| |Δt| (1 + REL_MARGIN) + ABS_MARGIN_MAS, |rate| =
  the larger of the two tables' hypot(ephRateRa, ephRateDec);
- ephRateRa / ephRateDec [deg/d]: SAFETY x a bound on the on-sky angular
  acceleration x |Δt|, the bound being
  (A_OBSERVER + GM_sun / r^2 + GM_earth / Δ^2) / Δ + 2 |Δdot| ω / Δ
  + ω^2 (1 + |tan dec|)  [rad/d^2]
  (A_OBSERVER: the observer's diurnal plus Earth's orbital acceleration;
  r, Δ, Δdot: SSObservation's helioRange, topoRange, topoRangeRate;
  ω the rate);
- ephVmag [mag]: SAFETY x (5/ln 10 (|Δdot|/Δ + |rdot|/r) + PHASE_SLOPE x
  (ω + v_max / r)) |Δt|;
- tail position angles [deg]: SAFETY x (ω (1 + |tan dec| + 1/f) + ω_w / f)
  |Δt|, f the sky fraction of the tail vector w and ω_w its direction's
  rate (|v|/r for the anti-Sun vector, GM_sun / (r^2 |v|) for the
  anti-motion one).

A missing range or rate (NaN) leaves only the |rate| |Δt| term where it
applies and 0 elsewhere: the check stays strict rather than passing
blindly.
"""

from __future__ import annotations

import numpy as np

#: |Δt| beyond this [s] means the two tables aren't at the same DiaSource's
#: exposure: a sanity limit only. The correction is <= 0.24 s for most
#: visits, but header-timed (degraded) visits shift by up to ~2 s (WP S1's
#: real-data run: 1.94 s), so it is well above that.
DT_MAX_S = 10.0
SECONDS_PER_DAY = 86400.0
MAS_PER_DEG = 3.6e6
#: Relative margin on |rate| |Δt| (float32 rates, the curvature of the
#: track over a few seconds: ~1e-5 of it for a close NEO at 2 s) and an
#: absolute one [mas].
REL_MARGIN = 0.02
#: The relative margin on the motion-corrected residual, where its
#: curvature term is computed (float32 rates: 6e-8; the mean of the two
#: tables' rates): 0.2%, so 5 mas show at 2 s and 10 deg/d (833 mas).
RESID_REL_MARGIN = 0.002
ABS_MARGIN_MAS = 0.05
#: Safety factor on the second-order bounds (rates, V, angles).
SAFETY = 2.0

AU_KM = 149597870.7
KM_S_TO_AU_D = SECONDS_PER_DAY / AU_KM
GM_SUN = 2.9591220828559115e-04        # au^3/d^2
GM_EARTH = GM_SUN / 332946.0487
#: The observer's acceleration [au/d^2]: Earth's rotation at the equator
#: (omega^2 R) plus Earth's heliocentric orbit (GM_sun / 0.983^2).
A_OBSERVER = (2 * np.pi / 0.99726958) ** 2 * 6378.137 / AU_KM + GM_SUN / 0.983**2
#: An upper bound on a heliocentric speed, as v <= sqrt(2 GM / r) + V_EXCESS
#: [au/d] (V_EXCESS: ~87 km/s, above any known interstellar object's).
V_EXCESS = 0.05
#: An upper bound on |dV/d(phase)| [mag/rad] (0.1 mag/deg, steeper than the
#: opposition surge of the H, G system).
PHASE_SLOPE = 0.1 * 180.0 / np.pi


def _a(x):
    return np.asarray(x, dtype=np.float64)


def dt_days(t_ssobservation, t_dia):
    """Δt [d] = SSObservation.midpointMjdTai - DiaSource.midpointMjdTai; 0
    where either is unknown (NaN), so those rows are held to the strict
    tolerance."""
    d = _a(t_ssobservation) - _a(t_dia)
    return np.where(np.isfinite(d), d, 0.0)


def rate_deg_day(rate_ra, rate_dec, *more):
    """The larger of the total on-sky rates [deg/d], hypot(ra, dec), of one
    or more (rate_ra, rate_dec) pairs; NaN-safe (fmax)."""
    r = np.hypot(_a(rate_ra), _a(rate_dec))
    for k in range(0, len(more), 2):
        r = np.fmax(r, np.hypot(_a(more[k]), _a(more[k + 1])))
    return r


def position_mas(rate, dt):
    """The position (and ephOffset) allowance [mas] for a total rate [deg/d]
    and Δt [d]: |rate| |Δt| (1 + REL_MARGIN) + ABS_MARGIN_MAS; 0 at Δt = 0.
    A NaN rate gives 0 (strict)."""
    dt, rate = _a(dt), np.nan_to_num(np.abs(_a(rate)), nan=0.0)
    a = rate * np.abs(dt) * MAS_PER_DEG * (1.0 + REL_MARGIN) + ABS_MARGIN_MAS
    return np.where(dt != 0, a, 0.0)


def position_margin_mas(rate, dt, accel=None):
    """The margin [mas] on the motion-corrected position residual:
    RESID_REL_MARGIN |rate| |Δt| + SAFETY accel Δt^2 / 2 + ABS_MARGIN_MAS
    where ``accel`` (the angular-acceleration bound [rad/d^2],
    `angular_acceleration`) is known, else REL_MARGIN |rate| |Δt| +
    ABS_MARGIN_MAS; 0 at Δt = 0."""
    dt, rate = _a(dt), np.nan_to_num(np.abs(_a(rate)), nan=0.0)
    acc = np.full(dt.shape, np.nan) if accel is None else np.broadcast_to(_a(accel), dt.shape)
    known = np.isfinite(acc)
    curv = np.degrees(SAFETY * np.where(known, acc, 0.0) * dt**2 / 2) * MAS_PER_DEG
    rel = np.where(known, RESID_REL_MARGIN, REL_MARGIN)
    a = rel * rate * np.abs(dt) * MAS_PER_DEG + curv + ABS_MARGIN_MAS
    return np.where(dt != 0, a, 0.0)


def motion_residual_mas(ra_s, dec_s, ra_n, dec_n, rate_ra, rate_dec, dt):
    """|(NearbySSO - SSObservation) + rate Δt| [mas] on the tangent plane at
    SSObservation's position: what's left of the two predictions' difference
    once the motion over Δt = t_SSObservation - t_DiaSource is taken out
    (NearbySSO, at the earlier time by Δt, is behind by rate x Δt).
    ``rate_ra`` includes cos(dec) [deg/d]. NaN rates leave the plain
    difference."""
    dec_s, dt = _a(dec_s), _a(dt)
    dra = (_a(ra_n) - _a(ra_s) + 180.0) % 360.0 - 180.0
    dxi = dra * np.cos(np.radians(dec_s)) + np.nan_to_num(_a(rate_ra)) * dt
    deta = _a(dec_n) - dec_s + np.nan_to_num(_a(rate_dec)) * dt
    return np.hypot(dxi, deta) * MAS_PER_DEG


def angular_acceleration(rate, topo_range, topo_range_rate, helio_range, dec):
    """An upper bound [rad/d^2] on the rate of change of the on-sky rate
    vector (in the local RA/Dec basis); NaN where an input is missing."""
    w = np.radians(np.abs(_a(rate)))
    delta, r = _a(topo_range), _a(helio_range)
    ddot = np.abs(_a(topo_range_rate)) * KM_S_TO_AU_D
    with np.errstate(divide="ignore", invalid="ignore"):
        acc = (A_OBSERVER + GM_SUN / r**2 + GM_EARTH / delta**2) / delta
        return acc + 2.0 * ddot * w / delta + w**2 * (1.0 + np.abs(np.tan(np.radians(_a(dec)))))


def rate_allowance(dt, rate, topo_range, topo_range_rate, helio_range, dec):
    """The ephRateRa/ephRateDec allowance [deg/d]; 0 at Δt = 0 or where the
    bound can't be computed."""
    dt = _a(dt)
    a = SAFETY * np.degrees(angular_acceleration(rate, topo_range, topo_range_rate, helio_range, dec)) \
        * np.abs(dt)
    return np.where((dt != 0) & np.isfinite(a), a, 0.0)


def vmag_allowance(dt, rate, topo_range, topo_range_rate, helio_range, helio_range_rate):
    """The ephVmag allowance [mag]; 0 at Δt = 0 or where it can't be
    computed."""
    dt = _a(dt)
    delta, r = _a(topo_range), _a(helio_range)
    with np.errstate(divide="ignore", invalid="ignore"):
        dist = 5.0 / np.log(10.0) * (np.abs(_a(topo_range_rate)) * KM_S_TO_AU_D / delta
                                     + np.abs(_a(helio_range_rate)) * KM_S_TO_AU_D / r)
        vmax = np.sqrt(2.0 * GM_SUN / r) + V_EXCESS
        phase = PHASE_SLOPE * (np.radians(np.abs(_a(rate))) + vmax / r)
    a = SAFETY * (dist + phase) * np.abs(dt)
    return np.where((dt != 0) & np.isfinite(a), a, 0.0)


def pa_allowance_deg(dt, rate, sky_frac, dec, w_rate):
    """The tail position-angle allowance [deg]: SAFETY x (ω (1 + |tan dec| +
    1/f) + ω_w / f) |Δt|, ω the on-sky rate [deg/d -> rad/d], f the tail
    vector's sky fraction, ω_w [rad/d] the rate of its direction. 0 at
    Δt = 0 or where it can't be computed; inf where f = 0 (the angle is
    undefined there)."""
    dt = _a(dt)
    w = np.radians(np.abs(_a(rate)))
    f = _a(sky_frac)
    with np.errstate(divide="ignore", invalid="ignore"):
        b = w * (1.0 + np.abs(np.tan(np.radians(_a(dec)))) + 1.0 / f) + _a(w_rate) / f
    a = SAFETY * np.degrees(b) * np.abs(dt)
    return np.where((dt != 0) & ~np.isnan(a), a, 0.0)


def anti_sun_direction_rate(helio_pos_au, helio_vel_kms):
    """|v| / r [rad/d]: an upper bound on the rate of the heliocentric
    radius vector's direction (3 x N inputs)."""
    r = np.linalg.norm(_a(helio_pos_au), axis=0)
    v = np.linalg.norm(_a(helio_vel_kms), axis=0) * KM_S_TO_AU_D
    with np.errstate(divide="ignore", invalid="ignore"):
        return v / r


def anti_motion_direction_rate(helio_pos_au, helio_vel_kms):
    """|a| / |v| [rad/d] with |a| = GM_sun / r^2 (x 1.1 for the planets and
    non-gravs): an upper bound on the rate of the velocity's direction."""
    r = np.linalg.norm(_a(helio_pos_au), axis=0)
    v = np.linalg.norm(_a(helio_vel_kms), axis=0) * KM_S_TO_AU_D
    with np.errstate(divide="ignore", invalid="ignore"):
        return 1.1 * GM_SUN / r**2 / v


def summary_line(dt):
    """A report line on the Δt distribution."""
    dt_s = np.abs(_a(dt)) * SECONDS_PER_DAY
    nz = dt_s[dt_s > 0]
    if not len(nz):
        return "time shift SSObservation - DiaSource: 0 on every compared row (strict tolerances)"
    return (f"time shift SSObservation - DiaSource: 0 on {int((dt_s == 0).sum()):,} rows (strict), nonzero "
            f"on {len(nz):,} (|dt| median {np.median(nz):.3f} s, max {nz.max():.3f} s); allowances per "
            f"bench/time_shift.py")


def too_large(dt):
    """Mask of rows with |Δt| > DT_MAX_S."""
    return np.abs(_a(dt)) * SECONDS_PER_DAY > DT_MAX_S
