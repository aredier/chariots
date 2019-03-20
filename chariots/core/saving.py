import operator
import os
import json
import shutil
from abc import ABC
from abc import abstractmethod
from abc import abstractclassmethod
from typing import Text
from typing import IO
from typing import Tuple
from tempfile import TemporaryDirectory

from chariots.core.versioning import Version


class Saver(ABC):
    """
    is responsible for persisting a tempfile (on the file system, remote, over a socket, go wild ...)
    """

    @abstractmethod
    def persist(self, temp_res_dir: Text, validity_version: Version, **identifiers):
        """
        method that determines how the the Saver should persist a dir. the identifiers and 
        validity checksum should be recoverable after the destruction of the Saver by a saver of the
        class. The temp_res_dir itself will be destroyed once out of this function and should be 
        copied in order to persist
        
        Arguments:
            temp_res_dir -- the temporary file in which the serialized object is
            validity_version -- the version of the serilized object
            identiiers -- intifiers to find a all the versions of the serialized object in
                perpetuity and throughout the unniverse
        """
    
    @abstractmethod
    def load(self, temp_dir: Text,  **identifiers) -> Version:
        """
        loads the object corresponding to identifier and returns the Version of the object of the 
        persisted object and the path to the directory containing the resulting serialized object
        
        Arguments:
            temp_dir -- the temp directory in which to copy the serialized object
        
        Returns:
            Version, path
        """


class Savable(ABC):
    """
    Savable Object
    """

    @abstractmethod
    def _serialize(self, temp_dir: Text):
        """serializes this object into temp_dir
        
        Arguments:
            temp_dir -- the temporary directory into which to save this object
        """
    
    def save(self, saver: Saver):
        """
        saves the object using the saver's `persist` method
        """
        with TemporaryDirectory() as temp_dir:
            self._serialize(temp_dir)
            with open(os.path.join(temp_dir, "_versioned_fields.json"), "w") as version_fields_file:
                json.dump(self.checksum().all_fields, version_fields_file)
            saver.persist(temp_dir, self.checksum(), **self.identifiers())
    
    @abstractclassmethod
    def _deserialize(cls, temp_dir: Text) -> "Savable":
        """
        defines how this object from it's serialized version persent in dir

        Arguments:
            dir -- the directory in which is present the saved version of the target object

        Returns:
            the initialised object corresponding to serialized format in dir
        """

    def load_serialized_fields(self, **fields):
        """
        loads the serialized fields once the object has been deserialized
        """
        for field_name, field_value in fields.items():
            setattr(self, field_name, field_value)

    @classmethod
    def load(cls, saver: Saver) -> "Savable":
        """
        creates an instance from the serialized data of the saver
        """
        with TemporaryDirectory() as temp_dir:

            old_version = saver.load(temp_dir, **cls.identifiers())
            with open(os.path.join(temp_dir, "_versioned_fields.json"), "r") as version_field_file:
                versioned_fields = json.load(version_field_file)

            # should distinguish saving version vs runtime version
            instance =  cls._deserialize(temp_dir)
        if old_version.major > cls.checksum().major:
            raise ValueError(f"saved {cls.__name__} is deprecated")
        instance.load_serialized_fields(**versioned_fields)
        return instance
    
    # TODO use class property for those two
    # https://stackoverflow.com/questions/5189699/how-to-make-a-class-property
    @abstractclassmethod
    def checksum(cls) -> Version:
        """
        validity cehcksum that verifies the data saved is still valid
        """

    @classmethod
    def identifiers(cls):
        """
        identifies the object (in order for the saver to know which data to fetch at loading time)
        """
        return {}
        

class FileSaver(Saver):

    def __init__(self, root: Text = "/tmp/chariots"):
        self.root = root
    
    def _generate_dir_path(self, **identifiers):
        identifiers = map(operator.itemgetter(1), sorted(identifiers.items(), key=operator.itemgetter(0)))
        return os.path.join(self.root, *identifiers)
    
    def _generate_file_path(self, version: Version, **identifier):
        return os.path.join(self._generate_dir_path(**identifier), str(version))


    def persist(self, temp_res_dir: Text, validity_checksum: Text, **identifiers):
        shutil.make_archive(self._generate_file_path(str(validity_checksum), **identifiers), "zip", temp_res_dir)
        
    def load(self, temp_dir: Text,  **identifiers) -> Tuple[Version, Text]:
        save_dir = self._generate_dir_path(**identifiers)
        file_ls = os.listdir(save_dir)
        versions = [Version.parse(file_name) for file_name in file_ls]
        latest_version = None
        latest_model_file = None
        for file_name, version in zip(file_ls, versions):
            if latest_version is None or version > latest_version:
                latest_version = version
                latest_model_file = file_name
        shutil.unpack_archive(os.path.join(save_dir, latest_model_file), temp_dir, "zip")
        return latest_version


