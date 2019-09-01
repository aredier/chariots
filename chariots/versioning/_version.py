import hashlib
import time
from typing import Optional

from .._helpers.typing import Hash
from ._version_type import VersionType


class Version:
    """
    a chariot version with three subversions (major, minor, patch)
    each version is a hash of the versioned fields that it is using
    the order of versions is determined by the timestamp of the version creation
    """

    def __init__(self, major: Optional[Hash] = None, minor: Optional[Hash] = None,
                 patch: Optional[Hash] = None, creation_time: Optional[float] = None):
        """
        ONLY PROVIDE ARGUMENTS IF YOU ARE LOADING A VALID VERSION

        :param major: the starting hash of the major version
        :param minor: the starting hash of the minor version
        :param patch: the starting hash of the patch version
        :param creation_time: the starting creation time of the version
        """
        self._major = major or hashlib.sha1()
        self._minor = minor or hashlib.sha1()
        self._patch = patch or hashlib.sha1()
        self._creation_time = creation_time or time.time()

    def __add__(self, other):
        if not isinstance(other, Version):
            raise ValueError("can only add Version with version, got {}".format(type(other)))
        result = Version()
        result.update_major(self.major.encode("utf-8"))
        result.update_major(other.major.encode("utf-8"))
        result.update_minor(self.minor.encode("utf-8"))
        result.update_minor(other.minor.encode("utf-8"))
        result.update_patch(self.patch.encode("utf-8"))
        result.update_patch(other.patch.encode("utf-8"))
        return result

    def __hash__(self):
        return hash(str(self))

    @property
    def creation_time(self) -> float:
        """
        the creation time of the op
        :return: the timestamp of the creation time
        """
        return self._creation_time

    def __gt__(self, other: "Version") -> bool:
        if self == other:
            return False
        return self.creation_time > other.creation_time

    def __eq__(self, other: "Version") -> bool:
        return (self.major == other.major and self.minor == other.minor and
                self.patch == other.patch)

    @property
    def major(self) -> str:
        """
        the hash of the major subversion
        :return: the hash string
        """
        if isinstance(self._major, str):
            return self._major
        return self._major.hexdigest()

    @property
    def minor(self) -> str:
        """
        the hash of the minor subversion
        :return: the hash string
        """
        if isinstance(self._minor, str):
            return self._minor
        return self._minor.hexdigest()

    @property
    def patch(self) -> str:
        """
        the hash of the patch subversion
        :return: the hash string
        """
        if isinstance(self._patch, str):
            return self._patch
        return self._patch.hexdigest()

    def update_major(self, input_bytes: bytes) -> "Version":
        """
        updates the major version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._major.update(input_bytes)
        return self

    def update_minor(self, input_bytes: bytes) -> "Version":
        """
        updates the minor version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._minor.update(input_bytes)
        return self

    def update_patch(self, input_bytes: bytes) -> "Version":
        """
        updates the patch version with some bytes

        :param input_bytes: bytes to update with
        :return: the updated version
        """
        self._patch.update(input_bytes)
        return self

    def update(self, version_type: VersionType, input_bytes: bytes) -> "Version":
        """
        updates the corresponding version of this version with some bytes

        :param version_type: the version to update
        :param input_bytes: the bytes to update with
        :return: the updated version
        """
        if version_type is VersionType.MAJOR:
            return self.update_major(input_bytes)
        if version_type is VersionType.MINOR:
            return self.update_minor(input_bytes)
        if version_type is VersionType.PATCH:
            return self.update_patch(input_bytes)
        raise ValueError("you provided an invalid version type: {}".format(version_type))

    def __repr__(self):
        return "<Version, major:{}, minor: {}, patch: {}>".format(self.major[:5], self.minor[:5], self.patch[:5])

    def __str__(self):
        hash_str = ".".join((self.major, self.minor, self.patch))
        return "_".join((hash_str, str(self._creation_time)))

    @classmethod
    def parse(cls, version_string: str) -> "Version":
        """
        parses a string representation of a saved version and returns a valid Version object

        :param version_string:
        :return: the version represented by the version string
        """
        hash_str, creation_time = version_string.split("_")
        major, minor, patch = hash_str.split(".")
        return cls(major, minor, patch, float(creation_time))
