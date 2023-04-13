import paho.mqtt.client as paho
import DbConnection as Db
import threading
import util


class MQTTDbConn(threading.Thread):
    # def __init__(self, dbName):
    def __init__(self, dbname, ipAddr, port, topics):
        threading.Thread.__init__(self)

        self.dbName = dbname
        self.ipAddr = ipAddr
        self.port = port
        self.topics = topics

    def run(self):
        self.dbConn = Db.DbConnection(self.dbName)

        self.client = paho.Client()
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        self.client.connect(self.ipAddr, self.port, 60)
        for topic in self.topics:
            self.client.subscribe(topic, 0)
        while self.client.loop() == 0:
            pass

    def on_message(self, mosq, obj, msg):
        # print("%-20s %d %s" % (msg.topic, msg.qos, msg.payload))
        # mosq.publish('pong', 'ack', 0)
        # print("===== ALL DATA STORED IN THE DATABASE: =====")
        # self.printLogs()
        # print('')
        self.saveMessage(msg.topic, util.convertBytesToString(msg.payload))

    def on_publish(self, mosq, obj, mid):
        pass

    def returnAllData(self):
        return self.dbConn.executeSQL("SELECT * FROM mqtt_logs")

    def deleteAllData(self):
        return self.dbConn.executeSQL("DELETE FROM mqtt_logs WHERE id != -1")

    def printLogs(self):
        for row in self.dbConn.executeSQL("SELECT * FROM mqtt_logs"):
            print(row)

    def saveMessage(self, mess_topic, mess_payload):
        params = (mess_topic, mess_payload)
        self.dbConn.executeSQL(f"INSERT INTO mqtt_logs VALUES (NULL, ?, ?)", params)
        self.dbConn.commit()
