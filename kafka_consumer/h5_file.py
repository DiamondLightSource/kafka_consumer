import logging
from pathlib import Path
from time import time
from typing import Optional

from h5py import File

from kafka_consumer.utils import array_from_flatbuffer, datatype_conversion

DATA_PATH = "entry/instrument/detector/data"
DATA_LINK_PATH = "entry/data"

log = logging.getLogger(__name__)


class MismatchedDimensions(Exception):
    """Raised when NDArray dimensions are different from the first array"""

    def __init__(self, first_array_dims, array_dims):
        self.first_array_dims = first_array_dims
        self.array_dims = array_dims

    def __str__(self):
        msg = (
            f"Array dims {self.array_dims} "
            + "do not match dims of first array in "
            + f"dataset {self.first_array_dims}"
        )
        return msg


class MismatchedDataype(Exception):
    """Raised when NDArray datatype is different from the first array"""

    def __init__(self, first_array_dtype, array_dtype):
        self.first_array_dtype = first_array_dtype
        self.array_dtype = array_dtype

    def __str__(self):
        msg = (
            f"Array dtype {self.array_dtype} "
            + "does not match dtype of first array in "
            + f"dataset {self.first_array_dtype}"
        )
        return msg


class H5File:
    """Handle de-serialization of arrays and writing of datasets"""

    def __init__(self):
        self.total_write_time = 0

    def create(
        self,
        filepath: Path,
        filename: str,
        num_arrays: int,
        first_array_id: Optional[int] = None,
    ):
        """Create h5file and set up groups

        Args:
            filepath: Path to h5file
            filename: Name of h5file
            num_arrays: Number of arrays to write to file
            first_array_id: ID of first NDArray to write
        """
        self.f = File(Path(filepath) / filename, "w", libver="latest")
        self.f.create_group(DATA_LINK_PATH)
        self.f.create_group(DATA_PATH)

        self.num_arrays = num_arrays
        self.array_index = 0
        self.array_count = 0
        self.data_dtype = None
        self.data_dims = None
        self.array_offset = first_array_id

    def add_array_from_flatbuffer(self, flatbuffer_array: bytes):
        """Deserialize flabbuffer and write array data to dataset

        Args:
            flatbuffer_array: FlatBuffer serialized NDArray
        """
        array = array_from_flatbuffer(flatbuffer_array)

        is_valid = self._check_array_id_and_increment_index(array)
        if is_valid:
            if self.array_count == 0:
                self._create_data_dataset(array)
            else:
                self._check_array(array)

            self._append_array(array)
            self.array_count += 1
        return is_valid

    def _check_array_id_and_increment_index(self, array):
        if self.array_offset is not None:
            if self.array_offset <= array.Id() < self.array_offset + self.num_arrays:
                log.debug(f"Received array with valid ID: {array.Id()}")
                log.debug(
                    f"Range: "
                    f"{self.array_offset} - {self.array_offset + self.num_arrays -1}"
                )
                self.array_index = array.Id() - self.array_offset
            else:
                log.debug(
                    f"Dropping array - Array ID: {array.Id()} outside provided range: "
                    f"{self.array_offset} - {self.array_offset + self.num_arrays -1}"
                )
                return False
        else:
            self.array_index = self.array_count
        return True

    def _append_array(self, array):
        self._extend_dataset()
        tic = time()
        arr_data = (
            array.PDataAsNumpy().view(self.data_dtype).reshape(array.DimsAsNumpy())
        )
        self.data.id.write_direct_chunk((0, 0, self.array_index), arr_data.tobytes())
        toc = time()
        self.total_write_time += toc - tic

    def _extend_dataset(self):
        if self.array_index + 1 > self.data.shape[2]:
            self.data.resize(self.array_index + 1, axis=2)

    def _create_data_dataset(self, array):
        # Get the array dimensions and create the 'data' dataset
        self.data_dims = array.DimsAsNumpy()
        self.data_dtype = datatype_conversion.get(array.DataType())
        self.data = self.f[DATA_PATH].create_dataset(
            "data",
            tuple(self.data_dims.tolist()) + (1,),
            dtype=self.data_dtype,
            maxshape=tuple(self.data_dims.tolist()) + (self.num_arrays,),
            chunks=tuple(self.data_dims.tolist()) + (1,),
        )
        # create the hardlink
        self.f[f"{DATA_LINK_PATH}/data"] = self.data

    def _check_array(self, array):
        # Check the dims and dtype are the same as the first array
        if not (array.DimsAsNumpy() == self.data_dims).all():
            raise (MismatchedDimensions(self.data_dims, array.DimsAsNumpy()))
        if datatype_conversion.get(array.DataType()) != self.data_dtype:
            raise (
                MismatchedDataype(
                    self.data_dtype, datatype_conversion.get(array.DataType())
                )
            )
