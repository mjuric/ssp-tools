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
    solve_kepler_hyperbolic,
    kepler_to_helio_ecliptic,
    cometary_to_helio_ecliptic,
    ecliptic_to_equatorial,
    _emission_state,
    _light_time_correct,
    _vector_to_radec,
)


def _state_to_q_e(X, V):
    """Perihelion distance and eccentricity from a heliocentric state."""
    r = np.linalg.norm(X)
    h = np.cross(X, V)
    evec = np.cross(V, h) / GM_SUN - X / r
    e = float(np.linalg.norm(evec))
    q = float(np.dot(h, h)) / (GM_SUN * (1.0 + e))
    return q, e


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


class TestCometary(unittest.TestCase):

    def test_hyperbolic_solver(self):
        for e in (1.0001, 1.2, 3.0, 20.0):
            M = np.linspace(-50.0, 50.0, 201)
            H = solve_kepler_hyperbolic(M, e)
            residual = e * np.sinh(H) - H - M
            self.assertLess(np.max(np.abs(residual)), 1e-10)

    def test_matches_classical_elements(self):
        """For elliptic orbits, (q, e, t - tp) must reproduce the (a, e, M)
        conversion."""
        rng = np.random.default_rng(2)
        for _ in range(50):
            a = rng.uniform(0.6, 60.0)
            e = rng.uniform(0.0, 0.95)
            inc, Om, om = rng.uniform(0.0, np.pi), rng.uniform(0, 2 * np.pi), rng.uniform(0, 2 * np.pi)
            dt = rng.uniform(-2e4, 2e4)
            n = np.sqrt(GM_SUN / a ** 3)
            X1, V1 = kepler_to_helio_ecliptic(a, e, inc, Om, om, n * dt)
            X2, V2 = cometary_to_helio_ecliptic(a * (1 - e), e, inc, Om, om, dt)
            np.testing.assert_allclose(X2, X1, rtol=0, atol=1e-11 * a)
            np.testing.assert_allclose(V2, V1, rtol=0, atol=1e-11 * np.linalg.norm(V1))

    def test_at_perihelion(self):
        for e in (0.0, 0.5, 0.9999, 1.0001, 2.5):
            q = 1.3
            X, V = cometary_to_helio_ecliptic(q, e, 0.0, 0.0, 0.0, 0.0)
            v = np.sqrt(GM_SUN * (1.0 + e) / q)
            np.testing.assert_allclose(X, [q, 0.0, 0.0], atol=1e-13)
            np.testing.assert_allclose(V, [0.0, v, 0.0], atol=1e-13)

    def test_recovers_q_and_e(self):
        """Including near-parabolic and hyperbolic orbits, as in mpc_orbits."""
        rng = np.random.default_rng(3)
        for e in (0.3, 0.97, 0.9995, 1.0005, 1.005, 1.5):
            for _ in range(10):
                q = rng.uniform(0.2, 8.0)
                inc, Om, om = rng.uniform(0.0, np.pi), rng.uniform(0, 2 * np.pi), rng.uniform(0, 2 * np.pi)
                dt = rng.uniform(-3000.0, 3000.0)
                X, V = cometary_to_helio_ecliptic(q, e, inc, Om, om, dt)
                q_back, e_back = _state_to_q_e(X, V)
                self.assertAlmostEqual(q_back / q, 1.0, places=9)
                self.assertAlmostEqual(e_back, e, places=9)

    def test_matches_rebound(self):
        """Cross-check against REBOUND's element conversion, elliptic and
        hyperbolic."""
        import rebound
        rng = np.random.default_rng(4)
        for e in (0.1, 0.8, 1.3, 4.0):
            for _ in range(10):
                q = rng.uniform(0.3, 10.0)
                inc, Om, om = rng.uniform(0.0, np.pi), rng.uniform(0, 2 * np.pi), rng.uniform(0, 2 * np.pi)
                dt = rng.uniform(-1000.0, 1000.0)
                a = q / (1.0 - e)
                M = np.sqrt(GM_SUN / abs(a) ** 3) * dt
                sim = rebound.Simulation()
                sim.G = GM_SUN
                sim.add(m=1.0)
                sim.add(primary=sim.particles[0], m=0.0, a=a, e=e, inc=inc, Omega=Om, omega=om, M=M)
                p = sim.particles[1]
                X, V = cometary_to_helio_ecliptic(q, e, inc, Om, om, dt)
                np.testing.assert_allclose(X, [p.x, p.y, p.z], rtol=0, atol=1e-10 * np.linalg.norm(X))
                np.testing.assert_allclose(V, [p.vx, p.vy, p.vz], rtol=0, atol=1e-10 * np.linalg.norm(V))

    def test_parabolic_rejected(self):
        with self.assertRaises(ValueError):
            cometary_to_helio_ecliptic(1.0, 1.0, 0.0, 0.0, 0.0, 10.0)


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


    def test_emission_state_matches_two_body(self):
        """The Taylor-expanded emission-time state must match exact two-body
        propagation back by tau, for a light time of 5.5 h (a TNO)."""
        a, e = 40.0, 0.2
        X, V = kepler_to_helio_ecliptic(a, e, 0.3, 1.0, 2.0, 1.0)
        X_t, V_t = X.reshape(3, 1), V.reshape(3, 1)
        sun = np.zeros((3, 1))
        r_obs = np.array([[1.0], [0.0], [0.0]])
        X_em, V_em, tau = _emission_state(X_t, V_t, sun, r_obs)
        self.assertAlmostEqual(
            tau[0], np.linalg.norm(X_em[:, 0] - r_obs[:, 0]) / C_AU_PER_DAY, places=12
        )
        n = np.sqrt(GM_SUN / a ** 3)
        X_ex, V_ex = kepler_to_helio_ecliptic(a, e, 0.3, 1.0, 2.0, 1.0 - n * tau[0])
        np.testing.assert_allclose(X_em[:, 0], X_ex, rtol=0, atol=1e-12)
        # 1e-12 AU/day ~ 2 um/s: the dropped (1/2) jerk tau^2 velocity term
        np.testing.assert_allclose(V_em[:, 0], V_ex, rtol=0, atol=1e-12)


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
