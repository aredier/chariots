"""enum for the different types of subversions"""
import enum


class VersionType(enum.Enum):
    """am enum to give the three subversion types used in the chariots framework"""
    MAJOR = 'major'
    MINOR = 'minor'
    PATCH = 'patch'
