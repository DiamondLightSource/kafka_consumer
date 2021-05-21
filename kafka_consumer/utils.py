from numpy import dtype

from kafka_consumer.FB_Tables.NDArray import NDArray

# This converts the NDArray DType to numpy datatypes
# TODO Find a better way of doing this
datatype_conversion = {
    0: dtype("int8"),
    1: dtype("uint8"),
    2: dtype("int16"),
    3: dtype("uint16"),
    4: dtype("int32"),
    5: dtype("uint32"),
    6: dtype("float32"),
    7: dtype("float64"),
    8: dtype("str"),
}


def array_from_flatbuffer(buffer):
    array_buf = bytearray(buffer)
    array = NDArray.GetRootAs(array_buf, 0)
    return array
