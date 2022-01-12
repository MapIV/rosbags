# Copyright 2020-2021  Ternaris.
# SPDX-License-Identifier: Apache-2.0
"""Rosbag1 to Rosbag2 Converter."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

import csv
import pathlib
import os

#import pandas as pd
#df=pd.read_csv('md5sum_list.csv',names=[msgtype,md5sum])


from rosbags.rosbag2 import Reader, ReaderError
from rosbags.rosbag1 import Writer, WriterError
#from rosbags.rosbag2.connection import Connection as WConnection
from rosbags.rosbag1.reader import Connection as WConnection

from rosbags.serde import cdr_to_ros1, ros1_to_cdr
from rosbags.typesys import get_types_from_msg, register_types
from rosbags.typesys.msg import denormalize_msgtype, generate_msgdef


if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Optional

    from rosbags.rosbag2.reader import Connection as RConnection

LATCH = """
- history: 3
  depth: 0
  reliability: 1
  durability: 1
  deadline:
    sec: 2147483647
    nsec: 4294967295
  lifespan:
    sec: 2147483647
    nsec: 4294967295
  liveliness: 1
  liveliness_lease_duration:
    sec: 2147483647
    nsec: 4294967295
  avoid_ros_namespace_conventions: false
""".strip()
cid=0


class ConverterError(Exception):
    """Converter Error."""


def convert_connection(rconn: RConnection) -> WConnection:
    """Convert rosbag1 connection to rosbag2 connection.

    Args:
        rconn: Rosbag1 connection.

    Returns:
        Rosbag2 connection.

    """
#    print(md5dict)

    #print(os.path.dirname(__file__)+'/definition/msgdef/'+denormalize_msgtype(rconn.msgtype)+'/msgdef.txt')
    
    filename=''

    msgdef=''
    try:
        with open(os.path.dirname(__file__)+'/definition/msgdef/'+denormalize_msgtype(rconn.msgtype)+'/msgdef.txt', encoding="utf-8") as f:
            msgdef = f.read()
    except OSError as e:
        print(e)
        return WConnection(-1,None,None,None,None,None,None,None)
    
    filename=os.path.dirname(__file__)+'/definition/msgdef/'+denormalize_msgtype(rconn.msgtype)+'/md5sum.txt'
    md5sum=''
    try:
        with open(filename, encoding="utf-8") as f:
            tmp=f.read().splitlines()
            md5sum = tmp[0]
    except OSError as e:
        print(e)
        print(filename)
        exit()


    global cid

    return WConnection(
        cid,
        rconn.topic,
        rconn.msgtype,
        msgdef, #msgdef
        md5sum, #md5sum
        None, #callerid
        1 if rconn.offered_qos_profiles != '' else None, #latching
        None
    )
    cid=cid+1

def convert(src: Path, dst: Optional[Path]) -> None:
    """Convert Rosbag1 to Rosbag2.

    Args:
        src: Rosbag1 path.
        dst: Rosbag2 path.

    Raises:
        ConverterError: An error occured during reading, writing, or
            converting.

    """

    dst = dst if dst else src.with_suffix('')
    if dst.exists():
        raise ConverterError(f'Output path {str(dst)!r} exists already.')


    msglist=[]
    try:
        with Reader(src) as reader, Writer(dst) as writer:
            typs: dict[str, Any] = {}
            connmap: dict[int, WConnection] = {}

            for rconn in reader.connections.values():
                print('********************************************************************************\n')
                print('[TopicName]:'+rconn.topic+'\n[TopicType]:'+rconn.msgtype)
                candidate = convert_connection(rconn)
                if candidate.cid==-1:
                    print("SKIP!\n\n")
                    continue
                existing = next((x for x in writer.connections.values() if x == candidate), None)
                can_dict = {'topic': candidate.topic, 'msgtype': candidate.msgtype, 'msgdef': candidate.msgdef, 'md5sum': candidate.md5sum ,'callerid': candidate.callerid,'latching': candidate.latching}
                wconn = existing if existing else writer.add_connection(**can_dict)
                connmap[rconn.id] = wconn
                #print(wconn.msgdef)
                print('\n\n')
                typs.update(get_types_from_msg(wconn.msgdef, wconn.msgtype))
                msglist.append(rconn.msgtype)
            register_types(typs)

            print('********************************************************************************\n')
            print('START Conversion!\n\n')

            for rconn, timestamp, data in reader.messages():
                #data = ros1_to_cdr(data, rconn.msgtype)
                if rconn.msgtype in msglist:
                    #print(type(data))
                    data = cdr_to_ros1(data, rconn.msgtype, rconn.topic)
                    writer.write(connmap[rconn.id], timestamp, data)
    except ReaderError as err:
        raise ConverterError(f'Reading source bag: {err}') from err
    except WriterError as err:
        raise ConverterError(f'Writing destination bag: {err}') from err
    except Exception as err:  # pylint: disable=broad-except
        raise ConverterError(f'Converting rosbag: {err!r}') from err
