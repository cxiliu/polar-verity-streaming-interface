''' 
Stream data from polar verity sense device and write HR/ACC/PPG data to file in data/ folder

Usage: python stream.py -id <RECORD_ID> -duration <LENGTH>
- id: integer, unique identifier for the recording (default: 1)
- duration: integer, duration of the recording in seconds (default: 30)

RECORD_ID = 0: print current state for 2 seconds
'''

import bleak
import asyncio
import struct
from termcolor import colored
from datetime import datetime
import winsound
import os.path
from parse_ble import Constants, StreamReader, SampleRateSetting, MeasurementType, PPGFrameType, ACCFrameType

RECORD_ID = 1 
LENGTH = 30 # seconds

RUN_TRIAL = False

# UUID for stream settings and data from Polar device
PMD_CONTROL = "FB005C81-02E7-F387-1CAD-8ACD2D8DF0C8"
PMD_DATA    = "FB005C82-02E7-F387-1CAD-8ACD2D8DF0C8"


''' 
Standard Bluetooth Read
https://www.bluetooth.com/specifications/assigned-numbers/
'''
uuid16_dict = {v: k for k, v in bleak.uuids.uuid16_dict.items()}
HR_UUID = "0000{0:x}-0000-1000-8000-00805f9b34fb".format(
    uuid16_dict.get("Heart Rate Measurement")
)
BT_UUID = "0000{0:x}-0000-1000-8000-00805f9b34fb".format(
    uuid16_dict.get("Battery Level")
)


'''
OP_CODE
0x01 Get setting
0x02 Request Measurement start
0x03 Request Measurement stop
'''
REQUEST_SETTING_PPG = bytearray([0x01, 0x01]) # request setting, data type
REQUEST_SETTING_ACC = bytearray([0x01, 0x02]) # request setting, data type


'''
START_STREAM
Format: Start measurement, Data, [setting_type(FLAG), array_length(1), option]
Available sampling rate and range options for SDK Mode
https://github.com/polarofficial/polar-ble-sdk/blob/master/documentation/SdkModeExplained.md
'''
SDK_STOP = bytearray([0x03, 0x09]) # to disable alternative frequencies
SDK_WRITE = bytearray([0x02, 0x09]) # to enable alternative frequencies

ACC_STOP = bytearray([0x03, 0x02]) # stop measurement, data type
ACC_WRITE = bytearray([0x02, 0x02, 
                       0x00, 0x01, 0x34, 0x00, # sample rate - 52Hz (fixed frequency when SDK mode is off)
                    #    0x00, 0x01, 0x1a, 0x00, # sample rate - 26Hz 
                       0x01, 0x01, 0x10, 0x00, # resolution - 16bit
                       0x02, 0x01, 0x08, 0x00, # range - 8G
                       0x04, 0x01, 0x03]) # three channels
PPG_STOP = bytearray([0x03, 0x01]) # stop measurement, data type
PPG_WRITE = bytearray([0x02, 0x01, 
                       0x00, 0x01, 0x37, 0x00, # sample rate - 55Hz (fixed frequency when SDK mode is off)
                    #    0x00, 0x01, 0x1c, 0x00, # sample rate - 28Hz
                    #    0x00, 0x01, 0x87, 0x00, # sample rate - 135Hz too noisy
                       0x01, 0x01, 0x16, 0x00, # resolution - 22bit 
                       0x04, 0x01, 0x04]) # four channels

# Polar devices give a timestamp since Jan 1 2000, 00:00:00 GMT
start_us = datetime(2000, 1, 1, 0, 0, 0).timestamp() * 1000000
def handle_hr_data(sender, data):
    hr_val, = struct.unpack_from("<B", data, 1)
    timestamp_us = datetime.utcnow().timestamp()*1000000 - start_us
    LOG_FILE_HR.write(str([int(timestamp_us), hr_val])+'\n')
    LOG_FILE_HR.flush()
    print (f"HR: {hr_val} bpm")

def handle_simp_data(sender, data):
    hr_val, = struct.unpack_from("<B", data, 1)
    print (f"HR: {hr_val} bpm")

LOG_FILE_PPG, LOG_FILE_ACC, LOG_FILE_HR = None, None, None

'''
PMD Control response
0xF0: Response code
0x01: OP Code (get measurement setting) / 0x02: OP Code (request measurement start) / 0x03 (request stop measurement)
0x01 / 0x02: Measurement Type (PPG / ACC data)
0x00: Error Code (success) / 08 (invalid sampling rate)/ 03 (not supported)
0x00: More (last packet)
'''
def handle_control_callback(sender, data):
    print("PMD Control response: "+ ", ".join(hex(b) for b in data))


def handle_pmd_data(sender, data):
    reader = StreamReader(data, 0)

    # header
    data_type = reader.pull_int8()
    end_timestamp_us = reader.pull_timestamp()
    frame_type = reader.pull_int8()
    channels = 0
    if data_type == MeasurementType.PPG:
        # reference sample (resolution = 22 bit, channels = 4)
        assert frame_type == PPGFrameType.PPGFrameTypeDelta
        ppg0_base = reader.pull_int22()
        ppg1_base = reader.pull_int22()
        ppg2_base = reader.pull_int22()
        ambient_base = reader.pull_int22()
        channels = 4
        period = Constants.sample_period(SampleRateSetting.SampleRate55)
    elif data_type == MeasurementType.ACC:
        # reference sample (resolution = 16 bit, channels = 3)
        assert frame_type == 129 #ACCFrameType.ACCFrameTypeDelta (this appears to be different from documentation)
        x_base = reader.pull_int16()
        y_base = reader.pull_int16()
        z_base = reader.pull_int16()
        channels = 3
        period = Constants.sample_period(SampleRateSetting.SampleRate52)
    else:
        print("ERROR: unexpected frame type")
        return
    
    # data frames
    total_samples = 0
    data = []
    if data_type == MeasurementType.PPG:
        while not reader.EOF:
            size_in_bits = reader._pull_byte()
            samples_count = reader._pull_byte()
            total_samples += samples_count
            # delta_sample_size = math.ceil((size_in_bits * channels)/8)
            integers = reader.parse_nbit_integers(size_in_bits, samples_count * channels)
            print(colored(f"PPG: {samples_count} samples (delta_size = {size_in_bits} bits)", \
                          'green' if len(integers) == samples_count*channels else 'red'))

            for i in range(samples_count):
                ppg0, ppg1, ppg2, ambient = integers[i*channels:(i+1)*channels]
                if ppg0 is not None:
                    ppg0_base += ppg0
                    ppg1_base += ppg1
                    ppg2_base += ppg2
                    ambient_base += ambient
                else:
                    print(colored(f"PPG: {samples_count} samples (delta_size = {size_in_bits} bits)", 'light_red'))
                    return
                data.append([0, ppg0_base, ambient_base])
        
    elif data_type == MeasurementType.ACC:
        while not reader.EOF:
            size_in_bits = reader._pull_byte()
            samples_count = reader._pull_byte()
            # delta_sample_size = math.ceil((size_in_bits * channels)/8)
            integers = reader.parse_nbit_integers(size_in_bits, samples_count * channels)
            print(colored(f"ACC: {samples_count} samples (delta_size = {size_in_bits} bits)", \
                          'light_blue' if len(integers) == samples_count*channels else 'yellow'))
            
            for i in range(samples_count):
                x, y, z = integers[i*channels:(i+1)*channels]
                if x is not None:
                    x_base += x
                    y_base += y
                    z_base += z
                else:
                    print(colored(f"ACC: {samples_count} samples (delta_size = {size_in_bits} bits)", 'light_yellow'))
                    return
                data.append([0, x_base, y_base, z_base])

    # write to log with corrected timestamp
    start_timestamp_us = end_timestamp_us - (total_samples - 1) * period
    for chunk in data:
        start_timestamp_us += period
        chunk[0] = int(start_timestamp_us)
        if data_type == MeasurementType.PPG:
            LOG_FILE_PPG.write(str(chunk)+'\n')
            LOG_FILE_PPG.flush()
        elif data_type == MeasurementType.ACC:
            LOG_FILE_ACC.write(str(chunk)+'\n')
            LOG_FILE_ACC.flush()
    
    
async def get_polar_address():
    async with bleak.BleakScanner() as scanner:
        for i in range(10):
            await asyncio.sleep(1.0)
            for d in scanner.discovered_devices:
                if d.name is not None and d.name[:11] == "Polar Sense":
                    print(d.name, d.address)
                    return d.address

async def main():
    global LOG_FILE_PPG, LOG_FILE_ACC, LOG_FILE_HR
    client = None

    try:
        # find and connect to polar device
        ADDRESS = await get_polar_address()
        if ADDRESS is None:
            print("issue finding device")
            return
        client = bleak.BleakClient(ADDRESS)
        
        if not await client.connect():
            print("issue connecting to device")
            return
        print( colored(f"connected to device: {client.is_connected}", "green"))

        # print all available services
        # services = await client.get_services()
        # for service in services:
        #     print(service.handle, service.description)
        
        # make sure mtu is min 232
        # print(client.mtu_size)

        if RUN_TRIAL:
            LOG_FILE_PPG = open(f"data/{RECORD_ID}-ppg.txt", "w")
            LOG_FILE_ACC = open(f"data/{RECORD_ID}-acc.txt", "w")
            LOG_FILE_HR = open(f"data/{RECORD_ID}-hr.txt", "w")
            
            await client.start_notify(PMD_CONTROL, handle_control_callback)
            await client.start_notify(PMD_DATA, handle_pmd_data)

            # # check available features from device
            # print(await client.read_gatt_char(PMD_CONTROL))

            duration = 1000  # milliseconds
            freq = 400  # Hz
            winsound.Beep(freq, duration)

            await client.write_gatt_char(PMD_CONTROL, ACC_WRITE, response=True)
            await client.write_gatt_char(PMD_CONTROL, PPG_WRITE, response=True)


            await client.start_notify(HR_UUID, handle_hr_data)
        else:
            await client.start_notify(HR_UUID, handle_simp_data)
            char = await client.read_gatt_char(BT_UUID)
            bt_val = struct.unpack_from("B", char, 0)
            print(colored(f"Battery Level: {bt_val[0]}%", 'green'))

        # sleep for X seconds while data is being received
        await asyncio.sleep(LENGTH)

        if RUN_TRIAL:
            await client.write_gatt_char(PMD_CONTROL, ACC_STOP, response=True)
            await client.write_gatt_char(PMD_CONTROL, PPG_STOP, response=True)

            winsound.Beep(freq, duration)

            # stop data streams and disconnect from client
            await client.stop_notify(PMD_DATA)
            await client.stop_notify(HR_UUID)
            await client.stop_notify(PMD_CONTROL)
        else:
            # await client.stop_notify(HR_UUID)
            await client.stop_notify(BT_UUID)
        
        await client.disconnect()

    finally:
        if LOG_FILE_PPG:
            LOG_FILE_PPG.close()
            LOG_FILE_ACC.close()
            LOG_FILE_HR.close()
        if client is not None:
            await client.disconnect()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-id', type=int, default=1, help='recording id')
    parser.add_argument('-duration', type=int, default=30, help='recording duration')
    args = parser.parse_args()
    RECORD_ID = args.id

    if RECORD_ID == 0:
        # print current state for 2 seconds
        RUN_TRIAL = False
        LENGTH = 2
        print(f"Printing current state for {LENGTH} seconds ... ")
    else:
        # record data for {duration} seconds
        RUN_TRIAL = True
        LENGTH = args.duration
        if os.path.isfile(f"data/{RECORD_ID}-ppg.txt"):
            print("File already exists. Do you want to overwrite it? (y/n)")
            response = input()
            if response.lower() != "y":
                print("Exitting. ")
                exit()
        print (f"Streaming data for id {RECORD_ID} for {LENGTH} seconds...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())