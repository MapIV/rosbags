.. image:: https://gitlab.com/ternaris/rosbags/badges/master/pipeline.svg
   :target: https://gitlab.com/ternaris/rosbags/-/commits/master
   :alt: pipeline status

.. image:: https://gitlab.com/ternaris/rosbags/badges/master/coverage.svg
   :target: https://gitlab.com/ternaris/rosbags/-/commits/master
   :alt: coverage report

.. image:: https://img.shields.io/pypi/pyversions/rosbags
   :alt: python versions

=======
Rosbags
=======

Rosbags is the **pure python** library for everything rosbag. It contains:

- **highlevel** easy-to-use interfaces,
- **rosbag2** reader and writer,
- **rosbag1** reader and writer,
- **extensible** type system with serializers and deserializers,
- **efficient converter** between rosbag1 and rosbag2,
- and more.

Rosbags does not have any dependencies on the ROS software stacks and can be used on its own or alongside ROS1 or ROS2.

Rosbags was developed for `MARV <https://gitlab.com/ternaris/marv-robotics>`_, which requires a fast, correct, and flexible library to read, manipulate, and write the various rosbag file formats.


Getting started
===============

Rosbags is published on PyPI and does not have any special dependencies. Simply install with pip::

   pip install rosbags


Read and deserialize messages from rosbag1 or rosbag2 files:

.. code-block:: python

   from pathlib import Path

   from rosbags.highlevel import AnyReader

   # create reader instance and open for reading
   with AnyReader([Path('/home/ros/rosbag_2020_03_24')]) as reader:
       connections = [x for x in reader.connections if x.topic == '/imu_raw/Imu']
       for connection, timestamp, rawdata in reader.messages(connections=connections):
            msg = reader.deserialize(rawdata, connection.msgtype)
            print(msg.header.frame_id)


Convert between rosbag versions::

   # Convert "foo.bag", result will be "foo/"
   rosbags-convert foo.bag

   # Convert "bar", result will be "bar.bag"
   rosbags-convert bar

   # Convert "foo.bag", save the result as "bar"
   rosbags-convert foo.bag --dst /path/to/bar

   # Convert "bar", save the result as "foo.bag"
   rosbags-convert bar --dst /path/to/foo.bag


Documentation
=============

Read the `documentation <https://ternaris.gitlab.io/rosbags/>`_ for further information.

.. end documentation


Contributing
============

Thank you for considering to contribute to rosbags.

To submit issues or create merge requests please follow the instructions provided in the `contribution guide <https://gitlab.com/ternaris/rosbags/-/blob/master/CONTRIBUTING.rst>`_.

By contributing to rosbags you accept and agree to the terms and conditions laid out in there.


Development
===========

Clone the repository and setup your local checkout::

   git clone https://gitlab.com/ternaris/rosbags.git
   
   cd rosbags
   python -m venv venv
   . venv/bin/activate
   
   pip install -r requirements-dev.txt
   pip install -e .


This creates a new virtual environment with the necessary python dependencies and installs rosbags in editable mode. The rosbags code base uses pytest as its test runner, run the test suite by simply invoking::

   pytest


To build the documentation from its source run sphinx-build::

   sphinx-build -a docs public


The entry point to the local documentation build should be available under ``public/index.html``.


Support
=======

Professional support is available from `Ternaris <https://ternaris.com>`_.
