from pathlib import Path

from h5py import File

from kafka_consumer.FB_Tables.NDArray import NDArray
from kafka_consumer.utils import datatype_conversion

DATA_PATH = "entry/instrument/detector/data"
DATA_LINK_PATH = "entry/data"
INST_PATH = "entry/instrument"
DET_ATTR_PATH = "entry/instrument/detector/NDAttributes"
INST_ATTR_PATH = "entry/instrument/NDAttributes"

EPICS_TS_SEC = "NDArrayEpicsTSSec"
EPICS_TS_NSEC = "NDArrayEpicsTSnSec"
ID = "NDArrayUniqueId"
TIMESTAMP = "NDArrayTimeStamp"


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
    def __init__(self):
        pass

    def create(self, filepath, filename, num_arrays):
        self.f = File(Path(filepath) / filename, "w")
        self.f.create_group(DATA_LINK_PATH)
        self.f.create_group(INST_PATH)
        self.f.create_group(INST_ATTR_PATH)
        self.f.create_group(DATA_PATH)
        self.f.create_group(DET_ATTR_PATH)

        # Create instrument NDAttribute datasets
        self.f[DET_ATTR_PATH].create_dataset(EPICS_TS_SEC, (num_arrays,))
        self.f[DET_ATTR_PATH].create_dataset(EPICS_TS_NSEC, (num_arrays,))
        self.f[DET_ATTR_PATH].create_dataset(ID, (num_arrays,), dtype="u4")
        self.f[DET_ATTR_PATH].create_dataset(TIMESTAMP, (num_arrays,))

        self.num_arrays = num_arrays
        self.array_index = 0
        self.data_dtype = None
        self.data_dims = None

    def add_array_from_flatbuffer(self, flatbuffer_array):
        array_buf = bytearray(flatbuffer_array)
        array = NDArray.GetRootAs(array_buf, 0)

        if self.array_index == 0:
            self._handle_first_array(array)
        else:
            self._check_array(array)

        self.data[:, :, self.array_index] = (
            array.PDataAsNumpy().view(self.data_dtype).reshape(array.DimsAsNumpy())
        )
        self._add_attributes(array)
        self.array_index += 1

    def _handle_first_array(self, array):
        # Get the array dimensions and create the 'data' dataset
        self.data_dims = array.DimsAsNumpy()
        self.data_dtype = datatype_conversion.get(array.DataType())
        self.data = self.f[DATA_PATH].create_dataset(
            "data",
            tuple(self.data_dims.tolist()) + (self.num_arrays,),
            dtype=self.data_dtype,
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

    def _add_attributes(self, array):
        self.f[f"{DET_ATTR_PATH}/{EPICS_TS_SEC}"][
            self.array_index
        ] = array.EpicsTS().SecPastEpoch()
        self.f[f"{DET_ATTR_PATH}/{EPICS_TS_NSEC}"][
            self.array_index
        ] = array.EpicsTS().Nsec()
        self.f[f"{DET_ATTR_PATH}/{ID}"][self.array_index] = array.Id()
        self.f[f"{DET_ATTR_PATH}/{TIMESTAMP}"][self.array_index] = array.TimeStamp()
