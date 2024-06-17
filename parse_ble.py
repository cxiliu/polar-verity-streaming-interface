'''
This contains classes from the PolarPy library. The original library is available at:
https://github.com/wideopensource/polarpy.git
'''

from io import BytesIO
from enum import IntEnum

class PPGFrameType(IntEnum):
    PPGFrameType24 = 0,
    PPGFrameTypeDelta = 128

class ACCFrameType(IntEnum):
    ACCFrameType8 = 0,
    ACCFrameType16 = 1,
    ACCFrameType24 = 2,
    ACCFrameTypeDelta = 128

class MeasurementType(IntEnum):
    PPG = 1,
    ACC = 2,
    GYRO = 5,
    MAG = 6, 
    SDK_MODE = 9

class SampleRateSetting(IntEnum):
    SampleRate26 = 0x001a
    SampleRate52 = 0x0034
    SampleRate55 = 0x0037
    SampleRate135 = 0x0082
    SampleRateUnknown = -1

class Constants:
    def frame_size(measurement_type: MeasurementType, frame_type) -> int:
        if MeasurementType.ACC == measurement_type:
            if ACCFrameType.ACCFrameType8 == frame_type:
                return 3
            if ACCFrameType.ACCFrameType16 == frame_type:
                return 6
            if ACCFrameType.ACCFrameType24 == frame_type:
                return 9
            return 0
        if MeasurementType.PPG == measurement_type:
            if PPGFrameType.PPGFrameType24 == frame_type:
                return 12
            return 0
        return 0
    def sample_period(sample_rate: SampleRateSetting) -> int: #in microseconds
        if SampleRateSetting.SampleRate135 == sample_rate:
            return 1000000 / 135
        if SampleRateSetting.SampleRate55 == sample_rate:
            return 1000000 / 55
        if SampleRateSetting.SampleRate52 == sample_rate:
            return 1000000 / 52
        if SampleRateSetting.SampleRate26 == sample_rate:
            return 1000000 / 26
        return 0
    
class StreamReader:
    def __init__(self, data: str, epoch_us: int):
        self._data_len = len(data)
        self._bytes_remaining = self._data_len
        self._stream = BytesIO(data)
        self._read_next_byte()
        self._epoch_us = epoch_us
        self.EOF = False

    def _read_next_byte(self) -> int:
        next_byte = self._stream.read(1) # read one byte
        self.EOF = 0 == len(next_byte)
        self._next_byte = -1 if self.EOF else next_byte[0]

    def _pull_byte(self):
        b = self._next_byte
        self._read_next_byte()
        self._bytes_remaining -= 1
        return b

    def parse_nbit_integers(self, bit_size: int, expected_count: int) -> list[int]:
        bit_buffer = 0
        bit_count = 0
        integers = []
        mask = (1 << bit_size) - 1  # Mask for extracting integers

        while self._bytes_remaining > 0:
            bit_buffer = (bit_buffer << 8) | self._pull_byte()
            bit_count += 8

            while bit_count >= bit_size:
                bit_count -= bit_size
                integer = (bit_buffer >> bit_count) & mask
                integers.append(integer)
                if len(integers) == expected_count:
                    return integers
                bit_buffer &= ~(mask << bit_count)

    def pull_int8(self):
        return self._pull_byte()
    
    def pull_int10(self):
        l = self._pull_byte()
        h = self._pull_byte() & 0x03 # keep last 2 bits
        v = (l & 0xff) + ((h & 0xff) << 8) & 0x03ff
        if v >= 0x200:
            v = -(0x3ff - v)
        return v

    def pull_int16(self):
        l = self._pull_byte()
        h = self._pull_byte()
        v = l + (h << 8)
        if v >= 0x8000:
            v = -(0xffff - v)
        return v

    def pull_int22(self) -> int:
        l = self._pull_byte()
        m = self._pull_byte()
        h = self._pull_byte() & 0x3f #keep last 6 bits
        v = (l & 0xff) + ((m & 0xff) << 8) + ((h & 0xff) << 16) & 0x3fffff
        if v >= 0x200000:
            v = -(0x3fffff - v)
        return v
    
    def pull_int64(self):
        d0 = self._pull_byte()
        d1 = self._pull_byte()
        d2 = self._pull_byte()
        d3 = self._pull_byte()
        d4 = self._pull_byte()
        d5 = self._pull_byte()
        d6 = self._pull_byte()
        d7 = self._pull_byte()
        v = d0 + (d1 << 8) + (d2 << 16) + (d3 << 24) + \
            (d4 << 32) + (d5 << 40) + (d6 << 48) + (d7 << 56)
        return v

    def pull_timestamp(self) -> int:
        timestamp_us = self.pull_int64() / 1000 # timestamp is in nanoseconds, convert to microseconds
        return timestamp_us

