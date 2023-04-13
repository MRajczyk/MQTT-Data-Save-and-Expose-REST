from typing import List

from pydantic import BaseModel
from MQTTDbConn import MQTTDbConn
from fastapi import FastAPI
import uvicorn


class MQTTData(BaseModel):
    id: str
    topic: str
    data: str


if __name__ == "__main__":
    mqttClient = MQTTDbConn("testdb", "127.0.0.1", 1883, ("test", "test2"))
    mqttClient.start()

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"message": "Hello World."}

    @app.get("/data", response_model=List[MQTTData])
    async def root():
        ret = mqttClient.returnAllData()
        data = []
        for row in ret:
            data.append(MQTTData(id=row[0], topic=row[1], data=row[2]))

        return data

    uvicorn.run(app, host="127.0.0.1", port=8000)
