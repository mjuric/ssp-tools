"""WP3: DiaSources, visits, candidate visits for an orbit, and matching.
See ``_contract.VISIT_DTYPE``."""


def read_dia(path, t_lo_mjd=None, t_hi_mjd=None):
    raise NotImplementedError("WP3")


def build_visits(dia):
    raise NotImplementedError("WP3")


class DiaIndex:
    def __init__(self, dia, visits):
        raise NotImplementedError("WP3")

    def match(self, visit_idx, ra, dec, radius_arcsec):
        raise NotImplementedError("WP3")


class VisitIndex:
    def __init__(self, visits):
        raise NotImplementedError("WP3")

    def candidates(self, track, margin_arcsec):
        raise NotImplementedError("WP3")
