"""Small IERS-A (finals2000A) files for the tests, cut from the file bundled
with astropy (astropy-iers-data), so nothing is shipped or downloaded.

The format is fixed-width: column 17 (0-based 16) is the polar-motion
I/P flag, 58 (57) the UT1-UTC I/P flag and 59-68 (58:68) UT1-UTC (s).
"""

import shutil
from pathlib import Path

from astropy.utils import iers

BUNDLED = Path(iers.IERS_A_FILE)

PM_FLAG, UT1_FLAG, UT1 = 16, 57, slice(58, 68)


def _lines():
    return BUNDLED.read_text().splitlines(keepends=True)


def first_predicted(lines):
    return next(i for i, line in enumerate(lines)
                if len(line) > UT1_FLAG and (line[PM_FLAG] == "P" or line[UT1_FLAG] == "P"))


def write_finals(path, before=40, after=40, promote=0, ut1_offset=0.0, predicted=True):
    """A finals2000A file at ``path``: ``before`` measured and ``after``
    predicted lines around the bundled file's first predicted one. With
    ``promote`` N, the first N predicted lines are marked measured, so the
    predictions start N days later; ``ut1_offset`` [s] is added to every
    UT1-UTC (so the table differs from the bundled one); without
    ``predicted``, only the measured lines. Returns the path."""
    lines = _lines()
    i = first_predicted(lines)
    out = lines[i - before:i + (after if predicted else 0)]
    for k in range(before, min(before + promote, len(out))):
        s = list(out[k])
        s[PM_FLAG] = s[UT1_FLAG] = "I"
        out[k] = "".join(s)
    if ut1_offset:
        for k, line in enumerate(out):
            v = line[UT1].strip()
            if v:
                out[k] = line[:UT1.start] + f"{float(v) + ut1_offset:10.7f}" + line[UT1.stop:]
    Path(path).write_text("".join(out))
    return Path(path)


def copy_bundled(path):
    """The whole bundled file (for builds whose steps need real coverage)."""
    shutil.copyfile(BUNDLED, path)
    return Path(path)
