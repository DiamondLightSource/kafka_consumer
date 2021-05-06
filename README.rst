kafka_consumer
===========================

|code_ci| |docs_ci| |coverage| |pypi_version| |license|

Simple kafka consumer

============== ==============================================================
PyPI           ``pip install kafka_consumer``
Source code    https://github.com/dls-controls/kafka_consumer
Documentation  https://dls-controls.github.io/kafka_consumer
============== ==============================================================

To consume a given number of arrays and write to an HDF file:

.. code:: python

    from kafka_consumer import consume_and_write

    consume_and_write(broker, group, topics, num_arrays)

.. |code_ci| image:: https://github.com/dls-controls/kafka_consumer/workflows/Code%20CI/badge.svg?branch=master
    :target: https://github.com/dls-controls/kafka_consumer/actions?query=workflow%3A%22Code+CI%22
    :alt: Code CI

.. |docs_ci| image:: https://github.com/dls-controls/kafka_consumer/workflows/Docs%20CI/badge.svg?branch=master
    :target: https://github.com/dls-controls/kafka_consumer/actions?query=workflow%3A%22Docs+CI%22
    :alt: Docs CI

.. |coverage| image:: https://codecov.io/gh/dls-controls/kafka_consumer/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/dls-controls/kafka_consumer
    :alt: Test Coverage

.. |pypi_version| image:: https://img.shields.io/pypi/v/kafka_consumer.svg
    :target: https://pypi.org/project/kafka_consumer
    :alt: Latest PyPI version

.. |license| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0
    :alt: Apache License

..
    Anything below this line is used when viewing README.rst and will be replaced
    when included in index.rst

See https://dls-controls.github.io/kafka_consumer for more detailed documentation.
