"""Offline sanity tests for the pure-numpy parts of ssp.ephem_assist.

These do not require ASSIST or any JPL ephemeris file. They exercise
Kepler element conversion, the ecliptic→equatorial rotation, and the
light-time corrector against analytic ground truth.
"""

import unittest
import numpy as np

from ssp.ephem_assist import (
    GM_SUN,
    C_AU_PER_DAY,
    OBLIQUITY_J2000,
    solve_kepler,
    kepler_to_helio_ecliptic,
    ecliptic_to_equatorial,
    _light_time_correct,
    _vector_to_radec,
)


class TestKepler(unittest.TestCase):

    def test_solver_circular(self):
        # e=0 → E = M
        M = np.linspace(-np.pi, np.pi, 17)
        E = solve_kepler(M, 0.0)
        np.testing.assert_allclose(E, ((M + np.pi) % (2 * np.pi)) - np.pi, atol=1e-14)

    def test_solver_high_eccentricity(self):
        # e=0.95: still must satisfy the equation to 1e-13
        e = 0.95
        M = np.linspace(-np.pi, np.pi, 257)
        E = solve_kepler(M, e)
        residual = E - e * np.sin(E) - (((M + np.pi) % (2 * np.pi)) - np.pi)
        self.assertLess(np.max(np.abs(residual)), 1e-12)

    def test_circular_orbit_at_perihelion(self):
        """Circular 1-AU orbit, M=0: position (1,0,0), velocity (0,n,0)
        in heliocentric ecliptic frame, where n = sqrt(GM/a^3).
        """
        X, V = kepler_to_helio_ecliptic(
            a=1.0, e=0.0, inc_rad=0.0, Omega_rad=0.0, omega_rad=0.0, M_rad=0.0,
        )
        n = np.sqrt(GM_SUN)  # AU/day, since a=1
        np.testing.assert_allclose(X, [1.0, 0.0, 0.0], atol=1e-13)
        np.testing.assert_allclose(V, [0.0, n, 0.0], atol=1e-13)

    def test_eccentric_orbit_at_perihelion(self):
        """At M=0 (perihelion) with omega=0, Omega=0, inc=0:
        r = a(1-e), v = sqrt(GM (1+e)/(a(1-e))).
        """
        a, e = 2.0, 0.3
        X, V = kepler_to_helio_ecliptic(
            a=a, e=e, inc_rad=0.0, Omega_rad=0.0, omega_rad=0.0, M_rad=0.0,
        )
        r = a * (1.0 - e)
        v = np.sqrt(GM_SUN * (1.0 + e) / (a * (1.0 - e)))
        np.testing.assert_allclose(X, [r, 0.0, 0.0], atol=1e-13)
        np.testing.assert_allclose(V, [0.0, v, 0.0], atol=1e-13)

    def test_energy_and_angular_momentum_conservation(self):
        """For arbitrary elements, the produced state should reproduce
        the input semi-major axis and eccentricity to ~1e-12.
        """
        rng = np.random.default_rng(1)
        for _ in range(20):
            a = rng.uniform(1.0, 5.0)
            e = rng.uniform(0.0, 0.6)
            inc = rng.uniform(0.0, np.pi / 3)
            Om = rng.uniform(0.0, 2 * np.pi)
            om = rng.uniform(0.0, 2 * np.pi)
            M = rng.uniform(0.0, 2 * np.pi)

            X, V = kepler_to_helio_ecliptic(a, e, inc, Om, om, M)
            r = np.linalg.norm(X)
            v2 = float(np.dot(V, V))
            energy = 0.5 * v2 - GM_SUN / r
            a_back = -GM_SUN / (2.0 * energy)
            h = np.cross(X, V)
            evec = np.cross(V, h) / GM_SUN - X / r
            e_back = float(np.linalg.norm(evec))
            self.assertAlmostEqual(a_back, a, places=10)
            self.assertAlmostEqual(e_back, e, places=10)


class TestObliquity(unittest.TestCase):

    def test_obliquity_value(self):
        """IAU76/80 J2000 obliquity (MPC/JPL ecliptic) is 23°26'21.448"."""
        self.assertAlmostEqual(np.rad2deg(OBLIQUITY_J2000), 84381.448 / 3600.0, places=12)

    def test_x_axis_invariant(self):
        """The vernal equinox direction (1,0,0) is shared by both frames."""
        v = np.array([1.0, 0.0, 0.0])
        np.testing.assert_allclose(ecliptic_to_equatorial(v), v, atol=1e-15)

    def test_pole_rotates_correctly(self):
        """Ecliptic pole (0,0,1) → tilted by ε about x-axis, lands in y-z."""
        v = np.array([0.0, 0.0, 1.0])
        out = ecliptic_to_equatorial(v)
        self.assertAlmostEqual(out[0], 0.0, places=15)
        self.assertAlmostEqual(out[1], -np.sin(OBLIQUITY_J2000), places=15)
        self.assertAlmostEqual(out[2], np.cos(OBLIQUITY_J2000), places=15)


class TestLightTime(unittest.TestCase):

    def test_zero_velocity_stationary_object(self):
        """If the object is at rest and observer at the same point,
        light-time correction should leave the offset essentially zero.
        """
        X_t = np.array([[1.5], [0.0], [0.0]])
        V_t = np.zeros((3, 1))
        sun = np.zeros((3, 1))
        r_obs = X_t.copy()
        rho = _light_time_correct(X_t, V_t, sun, r_obs)
        self.assertLess(np.linalg.norm(rho), 1e-15)

    def test_implies_correct_light_time(self):
        """Object 1 AU away from observer → dt_lt ≈ 1/c_AU/day. Position
        should be back-propagated by V·dt_lt.
        """
        X_t = np.array([[2.0], [0.0], [0.0]])         # 2 AU from sun
        V_t = np.array([[0.0], [0.0172], [0.0]])      # ≈ Earth speed
        sun = np.zeros((3, 1))
        r_obs = np.array([[1.0], [0.0], [0.0]])       # 1 AU from sun, 1 AU from object
        rho = _light_time_correct(X_t, V_t, sun, r_obs).ravel()

        dt_lt_expected = 1.0 / C_AU_PER_DAY
        # Object should appear shifted in -y by V_y·dt_lt.
        self.assertAlmostEqual(rho[0], 1.0, places=8)
        self.assertAlmostEqual(rho[1], -0.0172 * dt_lt_expected, places=10)
        self.assertAlmostEqual(rho[2], 0.0, places=12)


class TestVectorToRaDec(unittest.TestCase):

    def test_principal_axes(self):
        cases = [
            (np.array([[1.0], [0.0], [0.0]]), 0.0, 0.0),
            (np.array([[0.0], [1.0], [0.0]]), 90.0, 0.0),
            (np.array([[-1.0], [0.0], [0.0]]), 180.0, 0.0),
            (np.array([[0.0], [-1.0], [0.0]]), 270.0, 0.0),
            (np.array([[0.0], [0.0], [1.0]]), 0.0, 90.0),
            (np.array([[0.0], [0.0], [-1.0]]), 0.0, -90.0),
        ]
        for vec, ra_expect, dec_expect in cases:
            ra, dec = _vector_to_radec(vec)
            self.assertAlmostEqual(float(np.asarray(ra).ravel()[0]), ra_expect, places=10)
            self.assertAlmostEqual(float(np.asarray(dec).ravel()[0]), dec_expect, places=10)


if __name__ == "__main__":
    unittest.main()
