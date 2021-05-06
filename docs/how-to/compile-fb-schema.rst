How to re-compile flatbuffers NDArray schema
============================================

To deserialize NDArrays produced by ADKafka, the `schema <https://github.com/ess-dmsc/ad-kafka-interface/blob/master/ADKafka/ADKafkaApp/src/NDArray_schema.fbs>`_ must be compiled for python using `flatc <https://google.github.io/flatbuffers/flatbuffers_guide_building.html>`_.

To overwrite the auto-generated code in the :code:`FB_Tables` directory:

.. code-block::

   flatc --python -o FB_Tables/ NDArray_schema.fbs


