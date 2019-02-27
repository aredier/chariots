import operator
import os
import shutil
from abc import ABC
from abc import abstractmethod
from abc import abstractclassmethod
from typing import Text
from typing import IO
from typing import Tuple
from tempfile import TemporaryFile


class Saver(ABC):
    """
    is responsible for persisting a tempfile (on the file system, remote, over a socket, go wild ...)
    """

    @abstractmethod
    def persist(self, result_file: IO, validity_checksum: Text, **identifiers):
        """
        method that determines how the the Saver should persist a file. the identifiers and 
        validity checksu should be recoverable after the destruction of the Saver by a saver of the
        class
        """
        pass
    
    @abstractmethod
    def load(self, **identifiers) -> Tuple[Text, IO]:
        """
        loads from identifiers a file. it returns a chacksum and an IO from which to read the data
        """
        pass


class Savable(ABC):
    """
    Savable Object
    """

    @abstractmethod
    def _serialize(self, temp_file: IO):
        """
        how to serialise the object to a file
        """
    
    def save(self, saver: Saver):
        """
        saves the object using the saver's `persist` method
        """
        with TemporaryFile() as tempfile:
            self._serialize(tempfile)
            tempfile.seek(0)
            saver.persist(tempfile, self.checksum(), **self.identifiers())
    
    @abstractclassmethod
    def _deserialize(cls, file: IO) -> "Savable":
        """
        how to deserialize a the data and create an object
        """

    @classmethod
    def load(cls, saver: Saver) -> "Savable":
        """
        creates an instance from the serialized data of the saver
        """
        old_checksum, saved_io= saver.load(**cls.identifiers())
        if old_checksum != cls.checksum():
            raise ValueError(f"saved {cls.__name__} is deprecated")
        res = cls._deserialize(saved_io)
        saved_io.close()
        return res
    
    # TODO use class property for those two
    # https://stackoverflow.com/questions/5189699/how-to-make-a-class-property
    @abstractclassmethod
    def checksum(self):
        """
        validity cehcksum that verifies the data saved is still valid
        """

    @classmethod
    def identifiers(self):
        """
        identifies the object (in order for the saver to know which data to fetch at loading time)
        """
        return {}
        

class FileSaver(Saver):

    def __init__(self, root: Text = "/tmp/chariots"):
        self.root = root
    
    def _generate_path(self, **identifiers):
        identifiers = map(operator.itemgetter(1), sorted(identifiers.items(), key=operator.itemgetter(0)))
        return os.path.join(self.root, *identifiers)


    def persist(self, result_file: IO, validity_checksum: Text, **identifiers):
        file_str = result_file.read()
        save_dir = self._generate_path(**identifiers)
        os.makedirs(save_dir, exist_ok=True)
        with open(os.path.join(save_dir, validity_checksum), "w+b") as file:
            file.write(file_str)
        
    def load(self, **identifiers) -> Tuple[Text, IO]:
        save_dir = self._generate_path(**identifiers)
        file_ls = os.listdir(save_dir)
        if len(file_ls) > 1:
            raise ValueError(f"more than one file with identifiers {identifiers}")
        checksum = file_ls[0]
        file = open(os.path.join(save_dir, checksum), "r+b") 
        #shutil.rmtree(save_dir)
        return checksum, file 


