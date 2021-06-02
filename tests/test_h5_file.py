import numpy as np
import pytest

from kafka_consumer.h5_file import H5File, MismatchedDataype, MismatchedDimensions
from kafka_consumer.utils import datatype_conversion


@pytest.fixture
def h5file(tmp_path):
    h5file = H5File()
    # Specify a starting ID of 0 and request 10 arrays total
    h5file.create(tmp_path, "test_file.h5", 10, 0)
    return h5file


@pytest.fixture
def mock_array_from_flatbuffer(mocker):
    return mocker.patch("kafka_consumer.h5_file.array_from_flatbuffer")


@pytest.fixture(autouse=True)
def mock_create_dataset(mocker):
    mocker.patch("kafka_consumer.h5_file.File.create_dataset")


def create_mock_array(mocker, id=0, dims=(100, 100), datatype=0):
    mock_array = mocker.MagicMock()
    mock_array.DimsAsNumpy.return_value = np.array(dims)
    mock_array.PDataAsNumpy.return_value = np.zeros(
        dims, dtype=datatype_conversion.get(0)
    )
    mock_array.Id.return_value = id
    mock_array.DataType.return_value = datatype
    return mock_array


def test_array_outside_range(mocker, h5file, mock_array_from_flatbuffer):
    mock_array = create_mock_array(mocker, 20)
    mock_array_from_flatbuffer.return_value = mock_array
    # mock_array has ID 20 - should be rejected
    h5file.add_array_from_flatbuffer(mocker.MagicMock())
    assert h5file.array_count == 0


def test_array_inside_range(mocker, h5file, mock_array_from_flatbuffer):
    mock_array = create_mock_array(mocker, 7)
    mock_array_from_flatbuffer.return_value = mock_array
    # mock_array has ID 7 - should be accepted
    h5file.add_array_from_flatbuffer(mocker.MagicMock())
    assert h5file.array_count == 1


def test_wrong_dimensions_raises_error(mocker, h5file, mock_array_from_flatbuffer):
    mock_array = create_mock_array(mocker)
    mock_array_from_flatbuffer.return_value = mock_array
    h5file.add_array_from_flatbuffer(mocker.MagicMock())

    mock_wrong_size_array = create_mock_array(mocker, 1, (50, 50))
    mock_array_from_flatbuffer.return_value = mock_wrong_size_array
    with pytest.raises(MismatchedDimensions):
        h5file.add_array_from_flatbuffer(mocker.MagicMock())


def test_wrong_datatype_raises_error(mocker, h5file, mock_array_from_flatbuffer):
    mock_array = create_mock_array(mocker, 0, (100, 100), 0)
    mock_array_from_flatbuffer.return_value = mock_array
    h5file.add_array_from_flatbuffer(mocker.MagicMock())

    mock_wrong_size_array = create_mock_array(mocker, 1, (100, 100), 1)
    mock_array_from_flatbuffer.return_value = mock_wrong_size_array
    with pytest.raises(MismatchedDataype):
        h5file.add_array_from_flatbuffer(mocker.MagicMock())
