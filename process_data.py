'''
Various helper functions to parse, resample, aggregate the sensor data.

Usage: python process_data.py
- The script will read the raw data from the log files and output aggregated data in CSV format.
- The aggregated data includes HRV (RMSSD), HR and ACC values.
- The script will output the data in the data folder.
'''


import neurokit2 as nk
import csv
import numpy as np

# recording to remove from start / end (seconds)
OFFSET = 6

# convert timestamp relative to a start point (seconds)
start_us = 0
def us_to_seconds(raw_us):
    return int((int(raw_us)-start_us) / 1000000)

# compute hrv (RMSSD) using PPG signals
def compute_hrv_peak_to_peak(data):
    try:
        filtered = nk.signal_filter(data, sampling_rate = 55, lowcut = 0.5, highcut = 5, method = "butterworth", order = 2)
        peaks, info = nk.ppg_peaks(filtered, method = "elgendi", sampling_rate = 55, correct_artifacts=True, show = False)
        if len(info['PPG_Peaks']) < 5:
            return None
        first = info['PPG_Peaks'][1]
        last = info['PPG_Peaks'][-2]
        hrv_time = nk.hrv_time(peaks[first:last], sampling_rate = 55, show = False)
        hrv_freq = nk.hrv_frequency(peaks[first:last], sampling_rate=55, show=False, normalize=True)
        rmssd = hrv_time["HRV_RMSSD"].loc[0]
        lf = hrv_freq["HRV_HF"].loc[0]
        # lf = hrv_freq["HRV_LF"].loc[0]
        result = [0, 0]
        if rmssd == rmssd: # check if NaN
            result[0] = rmssd
        if lf == lf: # check if NaN
            result[1] = lf
        return tuple(result), info['PPG_Peaks'][1:-1]
    except Exception as e:
        return None

def write_csv(filename, record_id, data, init=False):
    with open(filename, 'w' if init else 'a') as f:
        writer = csv.writer(f)
        row = [record_id, ] + data
        writer.writerow(row)

########################################
#        read raw data from log        #
########################################


def get_ppg_raw(filename, tail=0):
    ts, ppg0s, ambients = [], [], []
    with open(filename, 'r') as myfile:
        graph_data_ppg = myfile.readlines()
        if tail > 0 and len(graph_data_ppg) >= tail:
            graph_data_ppg = graph_data_ppg[-tail:]   
        for line in graph_data_ppg:
            values = line[1:-2].split(',')
            if len(values) != 3: continue
            t, ppg0, ambient = values
            ts.append(int(t))
            ppg0s.append(int(ppg0))
            # ambients.append(int(ambient))
    if not ts: return
    ppg0s, ts = zip(*sorted(zip(ppg0s, ts), key=lambda x: x[1]))
    return ppg0s, ts

def get_acc_raw(filename, tail=0):
    ts, acc = [], []
    with open(filename, 'r') as myfile:
        graph_data_acc = myfile.readlines()
        if tail > 0 and len(graph_data_acc) >= tail:
            graph_data_acc = graph_data_acc[-tail:]        
        for line in graph_data_acc:
            values = line[1:-2].split(',')
            if len(values) != 4: continue
            t, x, y, z = values
            ts.append(int(t))
            mean = int( (abs(int(x))+abs(int(y))+abs(int(z))) / (3*1000) )
            acc.append(mean)
    if not ts: return
    acc, ts = zip(*sorted(zip(acc, ts), key=lambda x: x[1]))
    return acc, ts

def get_hr_raw(filename, tail=0):
    ts, hr = [], []
    with open(filename, 'r') as myfile:
        graph_data_hr = myfile.readlines()
        if tail > 0 and len(graph_data_hr) >= tail:
            graph_data_hr = graph_data_hr[-tail:]
        for line in graph_data_hr:
            values = line[1:-2].split(',')
            if len(values) != 2: continue
            t, h = values
            ts.append(int(t))
            hr.append(int(h))
    if not ts: return
    hr, ts = zip(*sorted(zip(hr, ts), key=lambda x: x[1]))
    return hr, ts


########################################
#         resample + aggregate         #
########################################


# resample HR by chopping off the first and last X seconds of the recording
def resample_hr(data, ts, synced_start, synced_end, rate = 1):
    current_time = synced_start
    resampled = []
    for i in range(len(data)-1):
        if ts[i] >= current_time and ts[i] <= synced_end:
            resampled.append(data[i])
            current_time += int (1000000 / rate)
            count = 0

            # HR is sampled at 1Hz, if the target frequency is higher, repeat the same value
            while count <= rate and ts[i+1] >= current_time:
                resampled.append(data[i])
                count += 1
                current_time += int (1000000 / rate)
    return resampled

# resample ACC by averaging over a window according to required frequency 
def resample_acc(data, ts, synced_start, synced_end, rate = 1):
    current_time = synced_start
    resampled = []
    temp_sum = []
    for i in range(len(data)):
        if ts[i] >= current_time and ts[i] <= synced_end:
            temp_sum.append(data[i])
            resampled.append(sum(temp_sum) / len(temp_sum))
            current_time += int (1000000 / rate)
        else:
            temp_sum.append(data[i])
    return resampled

# resample PPG by computing the HRV over a window according to required frequency
def resample_ppg(data, ts, synced_start, synced_end, rate = 1):
    window_size = 5 * 55 # total 5x2=10 seconds
    current_time = synced_start
    resampled = []
    for i in range(len(data)):
    # for i in range(window_size, len(data)-window_size, int(55/rate)):
        if ts[i] >= current_time and ts[i] <= synced_end:
            res = compute_hrv_peak_to_peak(data[i-window_size:i+window_size])
            if res:
                hrv, peaks = res
                resampled.append(hrv[0])
            else:
                resampled.append(0)
            current_time += int (1000000 / rate)
    return resampled






if __name__ == "__main__":
    hrv_file = "data/aggregated_hrv.csv"
    hr_file = "data/aggregated_hr.csv"
    acc_file = "data/aggregated_acc.csv"

    LENGTH = 30 #(DURATION-WINDOWx2)

    write_csv(hrv_file, "record_id", [i for i in range(LENGTH)], init=True)
    write_csv(hr_file, "record_id", [i for i in range(LENGTH)], init=True)
    write_csv(acc_file, "record_id", [i for i in range(LENGTH)], init=True)

    np.seterr(divide='ignore', invalid='ignore')

    frequency = 1 #Hz

    for record_id in [1,2,3,4]: # replace with the ids you have
        print(f"ID: {record_id} ...")

        # use hr log as reference for start and end time
        filename = f"data/{record_id}-hr.txt"
        hr, hr_ts = get_hr_raw(filename)
        ts_start = hr_ts[OFFSET-1]
        ts_end = hr_ts[-OFFSET]

        # each trial should contain {LENGTH} entries 
        print(f"    Generating {int(us_to_seconds(ts_end) - us_to_seconds(ts_start))} samples ...")

        # parse and resample ppg to output hrv values within a 10-second window
        filename = f"data/{record_id}-ppg.txt"
        ppg, ts = get_ppg_raw(filename)
        hrv_resampled = resample_ppg(ppg, ts, ts_start, ts_end, rate=frequency)

        # parse and resample acc to output average acc values
        filename = f"data/{record_id}-acc.txt"
        acc, acc_ts = get_acc_raw(filename)
        acc_resampled = resample_acc(acc, acc_ts, ts_start, ts_end, rate=frequency)
        
        # parse and resample hr to output average hr values
        hr_resampled = resample_hr(hr, hr_ts, ts_start, ts_end, rate=frequency)
        print(f"    HRV {len(hrv_resampled)} ACC {len(acc_resampled)} HR {len(hr_resampled)} samples") 

        write_csv(hrv_file, record_id, hrv_resampled)
        write_csv(hr_file, record_id, hr_resampled)
        write_csv(acc_file, record_id, acc_resampled)

        print(f"    Written to file.")