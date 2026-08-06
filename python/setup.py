"""Wheel build hooks.

Everything declarative lives in pyproject.toml; this exists only to place and tag the
wheel correctly, neither of which is expressible there.

The package contains no compiled Python extension -- cffi runs in ABI mode and opens
libhtcondordb_client with dlopen -- so setuptools would call it pure: it would install the
shared library into purelib and tag the wheel py3-none-any. Both are wrong. The library is
platform-specific, so it belongs in platlib (auditwheel refuses to repair a wheel with a
shared library in purelib) and the wheel needs a platform tag.

But the package embeds no CPython ABI either, so it does not need a tag per Python
version. The result is py3-none-<platform>: one wheel per OS and architecture, installable
on any Python 3.
"""

from setuptools import setup
from setuptools.dist import Distribution

try:  # setuptools >= 70 vendors the command
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover - older setuptools
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


class BinaryDistribution(Distribution):
    """A distribution that is impure despite building no extension.

    This is what moves the bundled library into platlib. Reporting extension modules is
    the only lever setuptools offers for saying "platform-specific" when the platform
    dependence comes from a file rather than from a compile step.
    """

    def has_ext_modules(self):
        return True


class bdist_wheel(_bdist_wheel):
    def get_tag(self):
        _, _, platform = super().get_tag()
        # Impure, so the default tag is interpreter-specific (cp312-cp312-...). Nothing
        # here links the CPython API, so widen it back to every Python 3.
        return "py3", "none", platform


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": bdist_wheel})
