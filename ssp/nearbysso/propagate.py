"""WP2: per-orbit ASSIST propagation with variational equations, and the
on-sky error ellipse. See ``_contract.CoarseTrack``."""


def coarse(orbit, t, obs_pos, ephem):
    raise NotImplementedError("WP2")


def ellipse_at(track, t):
    raise NotImplementedError("WP2")
