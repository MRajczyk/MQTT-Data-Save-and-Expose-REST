from typing import List

from pydantic import BaseModel
from MQTTDbConn import MQTTDbConn
from fastapi import FastAPI
import uvicorn


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


    @app.get("/data/all", response_model=List[MQTTData])
    async def root():
        macs = []
        for row in mqttClient.getUniqueMacAddresses():
            macs.append(row[0])

        print(macs)

        for mac in macs:
            for mac_row in mqttClient.getAllDataForSensor(mac):
                print(mac_row)
            print("===")

        # ret = mqttClient.getAllData()
        # data = []
        # for row in ret:
        #     data.append(MQTTData(log_id=row[0], timestamp=row[1], mac_addr=row[2], type=row[3], sensor_id=row[4], reading=row[5]))

        return [] # data


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
