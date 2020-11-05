import pandas as pd
from datetime import datetime as dt

TEXT_TO_VAL = [
    {'name': 'id', 'start': '$oid":"', 'end': '"', 'type': str},
    {'name': 'delay', 'start': '"delay":', 'end': ',', 'type': float},
    {'name': 'congestion', 'start': '"congestion":', 'end': ',', 'type': bool},
    {'name': 'lineId', 'start': '"lineId":"', 'end': '"', 'type': str},
    {'name': 'vehicleId', 'start': '"vehicleId":', 'end': ',', 'type': str},
    {'name': 'timestamp', 'start': '"$numberLong":"', 'end': '"', 'type': dt.fromtimestamp},  # TODO: Change type to dt.datetime
    {'name': 'areaId', 'start': '"areaId":', 'end': ',', 'type': str},
    {'name': 'areaId1', 'start': '"areaId1":', 'end': ',', 'type': str},
    {'name': 'areaId2', 'start': '"areaId2":', 'end': ',', 'type': str},
    {'name': 'areaId3', 'start': '"areaId3":', 'end': ',', 'type': str},
    {'name': 'gridID', 'start': '"gridID":"', 'end': '"', 'type': str},
    {'name': 'actualDelay', 'start': '"actualDelay":', 'end': ',', 'type': float},
    {'name': 'longitude', 'start': '"longitude":', 'end': ',', 'type': float},
    {'name': 'latitude', 'start': '"latitude":', 'end': ',', 'type': float},
    {'name': 'currentHour', 'start': '"currentHour":', 'end': ',', 'type': float},
    {'name': 'dateTypeEnum', 'start': '"dateTypeEnum":"', 'end': '"', 'type': str},
    {'name': 'angle', 'start': '"angle":', 'end': ',', 'type': float},
    {'name': 'ellapsedTime', 'start': '"ellapsedTime":', 'end': ',', 'type': float},
    {'name': 'vehicleSpeed', 'start': '"vehicleSpeed":', 'end': ',', 'type': float},
    {'name': 'distanceCovered', 'start': '"distanceCovered":', 'end': ',', 'type': float},
    {'name': 'journeyPatternId', 'start': '"journeyPatternId":"', 'end': '"', 'type': str},
    {'name': 'direction', 'start': '"direction":', 'end': ',', 'type': float},
    {'name': 'busStop', 'start': '"busStop":', 'end': ',', 'type': str},  # TODO: Check if type isn't only float/int
    {'name': 'poiId', 'start': '"poiId":', 'end': ',', 'type': str},
    {'name': 'poiId2', 'start': '"poiId2":', 'end': ',', 'type': str},
    {'name': 'systemTimestamp', 'start': '"systemTimestamp":', 'end': ',', 'type': float},
    {'name': 'calendar', 'start': '"$numberLong":"', 'end': '"', 'type': dt.fromtimestamp},  # TODO: Change type to dt.datetime
    {'name': 'filteredActualDelay', 'start': '"filteredActualDelay":', 'end': ',', 'type': float},
    {'name': 'atStop', 'start': '"atStop":', 'end': ',', 'type': bool},
    {'name': 'dateType', 'start': '"dateType":', 'end': ',', 'type': str},  # TODO: Check if type is actually float
    {'name': 'justStopped', 'start': '"justStopped":', 'end': ',', 'type': bool},
    {'name': 'justLeftStop', 'start': '"justLeftStop":', 'end': ',', 'type': bool},
    {'name': 'probability', 'start': '"probability":', 'end': ',', 'type': float},
    {'name': 'anomaly', 'start': '"anomaly":', 'end': ',', 'type': bool},
    {'name': 'loc_type', 'start': '"loc":{"type":"', 'end': '"', 'type': str},
    # {'name': 'coordinates', 'start': '"coordinates":', 'end': '}', 'type': list}, TODO: Extract after loop
]

def txt_to_dataframe(filename, condition):
    all_data_dict = {}
    for key in TEXT_TO_VAL:
        all_data_dict[key['name']] = []
    tick = dt.now()
    with open(filename) as f:
        for i, line in enumerate(f):
            if i % 100000 == 0:
                td = (dt.now() - tick).seconds
                print(str(i / 100000) + "*10^5, " + str(len(all_data_dict["id"])) + str(round(td, 3)) + " sec, pace: " + str(round(i+1 / (td + 0.0001), 3)) + " rows/sec")
            if condition['name'] == 'num_rows' and i == condition['value']:
                break
            if condition['name'] == 'count':
                continue
            key = [x for x in TEXT_TO_VAL if x['name'] == condition['name']]
            if len(key) > 0:
                key = key[0]
                value = line.split(key['start'])[1].split(key['end'])[0]
                if value != condition['value']:
                    continue
            for key in TEXT_TO_VAL:
                value = line.split(key['start'])[1].split(key['end'])[0]
                if value.lower() == 'false':
                    value = False
                elif value.lower() =='true':
                    value = True
                elif key['name'] in ['timestamp', 'calendar']:
                    value = int(int(value)/1000)
                value = key['type'](value)
                # print(f"\t {key['name']}: {value} ({type(value)})")
                all_data_dict[key['name']].append(value)
    print("finished")
    if condition['name'] == 'count':
        td = (dt.now() - tick).total_seconds()
        print(str(i / 10000) + "*10^5, " + str(len(all_data_dict["id"])) + str(round(td, 3)) + " sec, pace: " + str(round(i+1 / td, 3)) + " rows/sec")
        return pd.DataFrame(data=[i+1], columns=['num_of_rows'])
    df = pd.DataFrame.from_dict(all_data_dict).set_index('id')
    return df




file = input("enter file name: ")
if file == '':
    file = '/datashare/busFile'
    print("chosen file: " + file)
cond_orig = input("enter condition: ")
if cond_orig == '':
    cond_orig = 'num_rows 200'
cond = {'name': cond_orig.split()[0], 'value': cond_orig.split()[1]}





df = txt_to_dataframe(file, cond)
print(df)
df.to_csv('sampled_' + cond_orig + '.csv')
