from pathlib import Path
from time import time

from h5py import File

from kafka_consumer.utils import array_from_flatbuffer, datatype_conversion

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

    def create(self, filepath, filename, num_arrays, first_array_id=None):
        self.f = File(Path(filepath) / filename, "w")
        self.f.create_group(DATA_LINK_PATH)
        self.f.create_group(INST_PATH)
        self.f.create_group(INST_ATTR_PATH)
        self.f.create_group(DATA_PATH)
        self.f.create_group(DET_ATTR_PATH)

        # Create instrument NDAttribute datasets
        self.f[INST_ATTR_PATH].create_dataset(EPICS_TS_SEC, (num_arrays,))
        self.f[INST_ATTR_PATH].create_dataset(EPICS_TS_NSEC, (num_arrays,))
        self.f[INST_ATTR_PATH].create_dataset(ID, (num_arrays,), dtype="u4")
        self.f[INST_ATTR_PATH].create_dataset(TIMESTAMP, (num_arrays,))

        self.num_arrays = num_arrays
        self.array_index = 0
        self.array_count = 0
        self.data_dtype = None
        self.data_dims = None

        if first_array_id:
            self.array_offset = first_array_id
        else:
            self.array_offset = None

    def add_array_from_flatbuffer(self, flatbuffer_array):
        array = array_from_flatbuffer(flatbuffer_array)

        if self._check_array_id_and_increment_index(array):
            if self.array_count == 0:
                self._create_data_dataset(array)
                self._create_ndattr_datasets(array)
            else:
                self._check_array(array)

            self._append_array(array)
            self._append_instrument_attributes(array)
            self._append_detector_attributes(array)
            print(f"Array count is {self.array_count}")
            self.array_count += 1

    def _check_array_id_and_increment_index(self, array):
        if self.array_offset:
            if self.array_offset <= array.Id() < self.array_offset + self.num_arrays:
                self.array_index = array.Id() - self.array_offset
            else:
                print(f"Dropping ID: {array.Id()} as outside range")
                return False
        else:
            self.array_index = self.array_count
        return True

    def _append_array(self, array):
        self._extend_dataset()
        print(f"dset size is: {self.data.shape}")
        tic = time()
        print(f"Unique ID is {array.Id()} and array index is {self.array_index}")
        self.data[:, :, self.array_index] = (
            array.PDataAsNumpy().view(self.data_dtype).reshape(array.DimsAsNumpy())
        )
        toc = time()
        print(f"Time appending dataset: {toc-tic}")

    def _extend_dataset(self):
        if self.array_index+1 > self.data.shape[2]:
            tic = time()
            self.data.resize(self.array_index+1, axis=2)
            toc = time()
            print(f"Time take to extend: {toc - tic}")
        else:
            print("Resize not required")

    def _create_data_dataset(self, array):
        # Get the array dimensions and create the 'data' dataset
        self.data_dims = array.DimsAsNumpy()
        self.data_dtype = datatype_conversion.get(array.DataType())
        self.data = self.f[DATA_PATH].create_dataset(
            "data",
            tuple(self.data_dims.tolist()) + (1,),
            dtype=self.data_dtype,
            maxshape=tuple(self.data_dims.tolist()) + (self.num_arrays,),
            chunks=tuple(self.data_dims.tolist()) + (1,)
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

    def _append_instrument_attributes(self, array):
        self.f[f"{INST_ATTR_PATH}/{EPICS_TS_SEC}"][
            self.array_index
        ] = array.EpicsTS().SecPastEpoch()
        self.f[f"{INST_ATTR_PATH}/{EPICS_TS_NSEC}"][
            self.array_index
        ] = array.EpicsTS().Nsec()
        self.f[f"{INST_ATTR_PATH}/{ID}"][self.array_index] = array.Id()
        self.f[f"{INST_ATTR_PATH}/{TIMESTAMP}"][self.array_index] = array.TimeStamp()

    def _attach_NDAttr_attrs(self, attr, group):
        group.attrs.create("NDAttrDescription", data=attr.PDescription())
        group.attrs.create("NDAttrName", data=attr.PName())
        group.attrs.create("NDAttrSource", data=attr.PSource())
        group.attrs.create("NDAttrSourceType", data="Unknown")

    def _append_detector_attributes(self, array):
        for idx in range(array.PAttributeListLength()):
            attr = array.PAttributeList(idx)
            self.f[f"{DET_ATTR_PATH}/{attr.PName().decode()}"][
                self.array_index
            ] = attr.PDataAsNumpy().view(datatype_conversion.get(attr.DataType()))

    def _create_ndattr_datasets(self, array):
        for idx in range(array.PAttributeListLength()):
            attr = array.PAttributeList(idx)
            self.f[DET_ATTR_PATH].create_dataset(
                f"{attr.PName().decode()}",
                shape=(self.num_arrays,),
            )
            self._attach_NDAttr_attrs(
                attr, self.f[f"{DET_ATTR_PATH}/{attr.PName().decode()}"]
            )
