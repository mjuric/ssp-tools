"""Non-gravitational force parameters of MPC orbits (comets and Yarkovsky
asteroids), for ASSIST.

See docs/design/nongrav.md. The MPC's fitted non-gravitational coefficients
are in ``mpc_orb_jsonb.CAR`` (``coefficient_names`` beyond x..vz, with
``coefficient_values`` and the ``covariance``); the units and the g(r) were
checked against JPL SBDB (the design doc's "units check"):

- comets ("yc" model): A1, A2[, A3] in au/day^2, with the Marsden (1973)
  water-ice g(r);
- asteroids ("yarkovski"/"yarkovsky", both spellings occur): one
  coefficient, a transverse A2 in units of 1e-10 au/day^2, with
  g(r) = (r / 1 au)^-2.

ASSIST's acceleration is A_i * g(r) along the radial, transverse and normal
directions, g(r) = alpha (r/r0)^-nm (1 + (r/r0)^nn)^-nk; set
``particle_params`` to (A1, A2, A3) and the g(r) constants on the Extras
(one orbit per simulation, so per orbit).
"""

import json
from typing import NamedTuple

import numpy as np

#: g(r) constants per model, as ASSIST Extras attributes.
G_OF_R = {
    # Marsden, Sekanina & Yeomans (1973) water-ice sublimation
    "comet": dict(alpha=0.1112620426, r0=2.808, nm=2.15, nn=5.093, nk=4.6142),
    # g = (r / 1 au)^-2 (ASSIST's default)
    "yarkovsky": dict(alpha=1.0, r0=1.0, nm=2.0, nn=5.093, nk=0.0),
}

#: The MPC's Yarkovsky coefficient is A2 in units of this (au/day^2).
YARKOVSKY_UNIT = 1e-10

_YARKOVSKY_NAMES = ("yarkovski", "yarkovsky")
_COMET_NAMES = ("A1", "A2", "A3")
_STATE = ("x", "y", "z", "vx", "vy", "vz")


class NonGrav(NamedTuple):
    """An orbit's non-gravitational parameters.

    ``A`` is (A1, A2, A3) in au/day^2 (zeros where not fitted); ``model`` is
    "" (none), "comet" or "yarkovsky"; ``fitted`` says which of A1..A3 were
    fitted (and are in the covariance); ``cov`` is the covariance of the
    fitted parameters in the CAR order (x, y, z, vx, vy, vz, then the fitted
    A's), as the MPC gives it but with the A's in au/day^2, or None.
    """
    A: np.ndarray
    model: str
    fitted: np.ndarray
    cov: "np.ndarray | None"


NONE = NonGrav(np.zeros(3), "", np.zeros(3, bool), None)


def nongrav_params(mpc_orb_jsonb):
    """The NonGrav of one ``mpc_orb_jsonb`` (a JSON string, a dict, or None).
    Orbits without a non-gravitational fit give ``NONE``. Raises ValueError
    for a coefficient it doesn't know."""
    if mpc_orb_jsonb is None:
        return NONE
    j = json.loads(mpc_orb_jsonb) if isinstance(mpc_orb_jsonb, (str, bytes)) else mpc_orb_jsonb
    car = j.get("CAR") or {}
    names = list(car.get("coefficient_names") or [])
    if tuple(names[:6]) != _STATE or len(names) == 6:
        return NONE
    values = list(car.get("coefficient_values") or [])
    extra = names[6:]
    A = np.zeros(3)
    fitted = np.zeros(3, bool)
    scale = np.ones(len(names))
    if all(n in _COMET_NAMES for n in extra):
        model = "comet"
        for k, n in enumerate(extra):
            i = _COMET_NAMES.index(n)
            A[i] = values[6 + k]
            fitted[i] = True
    elif len(extra) == 1 and extra[0] in _YARKOVSKY_NAMES:
        model = "yarkovsky"
        A[1] = values[6] * YARKOVSKY_UNIT
        fitted[1] = True
        scale[6] = YARKOVSKY_UNIT
    else:
        raise ValueError(f"unknown non-gravitational coefficients {extra}")
    cov = _covariance(car.get("covariance") or {}, len(names))
    if cov is not None:
        cov = cov * np.outer(scale, scale)
    return NonGrav(A, model, fitted, cov)


def _covariance(cov, n):
    """The n x n covariance from the MPC's upper-triangle ``covIJ`` keys,
    or None if any entry is missing."""
    out = np.empty((n, n))
    for i in range(n):
        for k in range(i, n):
            v = cov.get(f"cov{i}{k}")
            if v is None:
                return None
            out[i, k] = out[k, i] = v
    return out


def apply(extras, ng):
    """Set an ASSIST Extras for one orbit's NonGrav: ``particle_params``
    (A1, A2, A3) and the g(r) constants. A no-op for ``model == ""``."""
    if not ng.model:
        return
    for k, v in G_OF_R[ng.model].items():
        setattr(extras, k, v)
    extras.particle_params = np.asarray(ng.A, dtype=np.float64)


def from_orbit(orbit):
    """The NonGrav of one ``ssp.nearbysso._contract.ORBIT_DTYPE`` row (its
    ``ng_*`` fields; ``cov`` is None: the full covariance is the row's
    ``cov_full``)."""
    model = str(orbit["ng_model"])
    if not model:
        return NONE
    return NonGrav(np.array(orbit["ng_A"], dtype=np.float64), model,
                   np.array(orbit["ng_fitted"], dtype=bool), None)
