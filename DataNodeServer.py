import sys
import os
import time
import psutil
from flask import Flask
from flask_restful import Api, Resource, reqparse
import requests
import constants
import threading

app = Flask(__name__)
api = Api(app)

'''
Map of the entire storeage in the current DataNode i.e BlockData
Structure:
{
  "blockID": {
                     filename: "filename1",
                 },
  "blockID": {
                    filename: "filename2",
                 },
}
'''
BlockList = {}
DATA_DIR = None


def addBlockList(blockNumber, size):
    BlockList[blockNumber] = {"size": size}


def storeBlockData(filename, data):
    # TODO: This is not fault tolerant. What if the system crashes after open() below ?
    f = open(os.path.join(DATA_DIR, filename), "w")
    f.write(data)
    f.close();


def getBlockData(filename):
    f = open(os.path.join(DATA_DIR, filename), "r")
    data = f.read()
    f.close()
    return data


def sendHeartBeats(name):
    task = {"DataNodeName": name}
    while True:
        try:
            resp = requests.post('http://127.0.0.1:5002/heartbeat/', data=task)
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
            else:
                print("successfully sent heartbeat")
        except requests.exceptions.ConnectionError as err:
            print("failed to send heartbeat to NN:", err)

        time.sleep(constants.HEARTBEAT_INTERVAL)


def sendBlockReport(name):
    while True:
        du = psutil.disk_usage(DATA_DIR)
        task = {"AvailableCapacity": du.free, "TotalCapacity": du.total, "BlockReport": BlockList}
        try:
            resp = requests.post('http://127.0.0.1:5002/BlockReport/' + name, data=task)
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
            else:
                print("successfully sent block report")
        except requests.exceptions.ConnectionError as err:
            print("failed to send block report to NN:", err)

        time.sleep(constants.BLOCKREPORT_INTERVAL)


# send heartbeat to Name Node in a given time frame.
class Heartbeat(Resource):
    def post(self):
        # TODO: make a post request to Name Node
        return ''


class BlockData(Resource):
    def post(self, blockNumber):
        """
        This methods store the provided data in the provided block number
        The response structure will look like this:

        Response:
        {
             status: "successful"
        }

        :param blockNumber:
        :param data:
        :return:
        """

        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("size")
        args = parser.parse_args()
        if (args["size"] and args["data"]):
            addBlockList(blockNumber, args["size"])
            storeBlockData(blockNumber, args["data"])
            return {"status": "successful"}
        return {"status": "failed"}, 404

    def get(self, blockNumber):
        """
        This methods return the data in the given block number to the client
        The response structure will look like this:

        Response:
        {
            data: "actual data",
        }

        :param blockNumber:
        :return:
        """
        if (blockNumber in BlockList):
            data = getBlockData(blockNumber)
            return {"data": data}
        return "block not found", 404


class DummyAPI(Resource):
    def get(self):
        return "Hello World!"

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("name")
        args = parser.parse_args()
        return args['name']


api.add_resource(BlockData, "/BlockData/<string:blockNumber>")
api.add_resource(DummyAPI, "/")

if __name__ == '__main__':
    dn_port = int(sys.argv[1])
    DATA_DIR = "./blockDataList_" + str(dn_port)
    try:
        os.mkdir(DATA_DIR)
    except FileExistsError:
        pass

    threading.Thread(target=sendHeartBeats, args=(str(dn_port),)).start()
    threading.Thread(target=sendBlockReport, args=(str(dn_port),)).start()
    print(psutil.virtual_memory().available)
    print(psutil.virtual_memory().total)
    app.run(port=dn_port)
