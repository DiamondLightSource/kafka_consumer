from pathlib import Path

from h5py import File

from kafka_consumer.FB_Tables.NDArray import NDArray

DATA_PATH = "entry/instrument/detector/data"
DATA_LINK_PATH = "entry/data"
INST_PATH = "entry/instrument"
DET_ATTR_PATH = "entry/instrument/detector/NDAttributes"
INST_ATTR_PATH = "entry/instrument/NDAttributes"

EPICS_TS_SEC = "NDArrayEpicsTSSec"
EPICS_TS_NSEC = "NDArrayEpicsTSnSec"
ID = "NDArrayUniqueId"
TIMESTAMP = "NDArrayTimeStamp"


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

    def add_array_from_flatbuffer(self, flatbuffer_array):
        array_buf = bytearray(flatbuffer_array)
        array = NDArray.GetRootAs(array_buf, 0)

        if self.array_index == 0:
            # If it's the first array, get the array dimensions and create
            # the 'data' dataset
            dims = array.DimsAsNumpy()
            self.data = self.f[DATA_PATH].create_dataset(
                "data", tuple(dims.tolist()) + (self.num_arrays,)
            )
            # create the hardlink
            self.f[f"{DATA_LINK_PATH}/data"] = self.data

        self.data[:, :, self.array_index] = array.PDataAsNumpy().reshape(
            array.DimsAsNumpy()
        )
        self._add_attributes(array)
        self.array_index += 1

    def _add_attributes(self, array):
        self.f[f"{DET_ATTR_PATH}/{EPICS_TS_SEC}"][
            self.array_index
        ] = array.EpicsTS().SecPastEpoch()
        self.f[f"{DET_ATTR_PATH}/{EPICS_TS_NSEC}"][
            self.array_index
        ] = array.EpicsTS().Nsec()
        self.f[f"{DET_ATTR_PATH}/{ID}"][self.array_index] = array.Id()
        self.f[f"{DET_ATTR_PATH}/{TIMESTAMP}"][self.array_index] = array.TimeStamp()
