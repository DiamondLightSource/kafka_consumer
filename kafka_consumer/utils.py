from numpy import dtype

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
