import websockets
import asyncio
from asyncio.exceptions import IncompleteReadError 
from websockets.exceptions import ConnectionClosedError
import nest_asyncio

import datetime
import json
import time
from collections import deque
import csv
import os

nest_asyncio.apply()


url = "wss://ws.blockchain.info/inv"
path = "raw_data"

Exists = os.path.exists(path)
if Exists!=True:
   os.makedirs(path)


def custom_parse(payload):
    prev_out = payload['x']['inputs'][0]['prev_out']
    len_inputs = len(payload['x']['inputs'])
    out = payload['x']['out'][0]
    len_out =len(payload['x']['out'])
    payload_extr = {'prev_addr': prev_out['addr'],
                    'prev_value' : prev_out['value'],
                    'out_ddr' : out['addr'], 
                    'out_value': out['value'], 
                    'len_inputs' : len_inputs, 
                    'len_out' : len_out}
    return payload_extr

def prepare_writer(location):
    csv_file = open(location, 'a')
    w = csv.DictWriter(csv_file, ['prev_addr', 'prev_value', 'out_ddr', 'out_value', 'len_inputs', 'len_out'])
    w.writeheader()
    return csv_file, w


async def main():

    websocket = await websockets.connect(url, ping_interval=None)
    await websocket.send("""{"op":"unconfirmed_sub"}""")

    current_day_and_hour = datetime.datetime.now().strftime('%Y-%d-%m-%H')
    previous_day_and_hour = current_day_and_hour

    csv_file, w = prepare_writer(f'raw_data/output-{previous_day_and_hour}.csv')
    
    while True:
        if not websocket.open:
            try:
                print('Websocket is NOT connected. Reconnecting...')
                websocket = await websockets.connect(url,ping_interval=None)
                await websocket.send("""{"op":"unconfirmed_sub"}""")

            except:
                print('Unable to reconnect, trying again.')


        current_day_and_hour = datetime.datetime.now().strftime('%Y-%d-%m-%H')

        if current_day_and_hour!=previous_day_and_hour:
            previous_day_and_hour = current_day_and_hour
            csv_file.close()
            csv_file, w = prepare_writer(f'raw_data/output-{previous_day_and_hour}.csv')

        try:
            payload = json.loads(await websocket.recv())
            payload_extr = custom_parse(payload)
            w.writerow(payload_extr)        
        except IncompleteReadError:
            print('Incomplete Read Error. Continuing.')
            continue
        except ConnectionClosedError:
            print('Connection Closed Error. Continuing.')
            continue

loop = asyncio.get_event_loop()
loop.run_until_complete(main())









