from typing import List

from pydantic import BaseModel
from MQTTDbConn import MQTTDbConn
from fastapi import FastAPI
import uvicorn

import json


class MQTTData(BaseModel):
    log_id: int
    timestamp: int
    mac_addr: str
    type: str
    sensor_id: int
    reading: float


if __name__ == "__main__":
    mqttClient = MQTTDbConn("testdb", "127.0.0.1", 1883, "dev/#")
    mqttClient.start()

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"message": "Hello World."}


    @app.get("/data/all")
    async def root():
        macs = []
        # returns mac_addr and min, max timestamps
        for row in mqttClient.getUniqueMacAddresses():
            print(row)
            macs.append(row)

        for mac in macs:
            for mac_row in mqttClient.getAllDataForSensor(mac[0]):
                print(mac_row)
            print("===")

        # extract min and max data (begin, end) - ok
        # TODO: build json according to szymonaszek's mock (to be talked about)

        data = {}
        # data['datetime'] = {'from': macs[0], 'to': macs[1], 'timezone_offset': 7200}
        json_data = json.dumps(data)
        # ret = mqttClient.getAllData()
        # data = []
        # for row in ret:
        #     data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4], reading=row[5]))

        return json_data


    @app.get("/data/test", response_model=List[MQTTData])
    async def root():
        ret = mqttClient.getAllDataTest()
        data = []
        for row in ret:
            data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4], reading=row[5]))

        return data

    @app.get("/data/temp", response_model=List[MQTTData])
    async def root():
        ret = mqttClient.getTempData()
        data = []
        for row in ret:
            data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4],
                                 reading=row[5]))

        return data

    uvicorn.run(app, host="127.0.0.1", port=8000)
