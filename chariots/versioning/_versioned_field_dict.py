import collections
from typing import Any, Iterator, Mapping

from ._version_type import VersionType
from ._version import Version
from ._versioned_field import VersionedField


class VersionedFieldDict(collections.MutableMapping):
    """
    a versioned field dict acts as a normal dictionary but the values as interpreted as versioned fields when it is
    a VersionedClass class attribute
    """

    def __init__(self, default_version=VersionType.MAJOR, *args, **kwargs):
        self.default_version = default_version
        self.store = dict()
        self.update(dict(*args, **kwargs))

    def __delitem__(self, key) -> None:
        del self.store[key]

    def __getitem__(self, key) -> Any:
        return self.store[key].value

    def __len__(self) -> int:
        return len(self.store)

    def __iter__(self) -> Iterator[str]:
        return iter(self.store)

    def __setitem__(self, key: str, value: Any) -> None:
        if not isinstance(value, VersionedField):
            value = VersionedField(value, self.default_version)
        if not isinstance(key, str):
            raise TypeError("`VersionedFieldDict` keys must be strings")
        self.store[key] = value

    @property
    def version_dict(self) -> Mapping[str, "Version"]:
        """
        proprety to retrieve the name of the fields and the Versions associated to each of them
        :return: the mapping with the key and the version of the value
        """
        return {attr_name: Version().update(attr_value.affected_version, attr_value.__chariots_hash__.encode("utf-8"))
                for attr_name, attr_value in self.store.items() if isinstance(attr_value, VersionedField)}