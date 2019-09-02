import enum


# TODO rename to SUBVERSIONTYPE for consistency and clarity
class VersionType(enum.Enum):
    """am enum to give the three subversion types used in the chariots framework"""
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"
