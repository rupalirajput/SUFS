import sys
import os
import time
import psutil
import requests
import constants
import threading

from flask import Flask
from flask_restful import Api, Resource, request
from flask import Flask, abort
import werkzeug.exceptions as HTTPStatus
from readerwriterlock import rwlock

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
gLock = rwlock.RWLockRead()
BlockList = None
DATA_DIR = None


def storeBlockData(filename, data):
    # TODO: This is not fault tolerant. What if the system crashes after open() below ?
    f = open(os.path.join(DATA_DIR, filename), "w")
    f.write(data)
    f.close()


def getBlockData(filename):
    """

    :type filename: str
    """
    f = open(os.path.join(DATA_DIR, filename), "r")
    data = f.read()
    f.close()
    return data

def scanData(dir):
    """

    :type dir: str
    """
    BlockList = {}
    for blockNumber in os.listdir(dir):
        BlockList[blockNumber] = {"size": os.path.getsize(os.path.join(dir, blockNumber))}

    return BlockList


def sendHeartBeats(name):
    task = {"DataNodeName": name}
    while True:
        try:
            resp = requests.post('http://127.0.0.1:5002/heartbeat/', json=task)
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
            else:
                print("successfully sent heartbeat")
        except requests.exceptions.ConnectionError as err:
            print("failed to send heartbeat to NN:", err)

        time.sleep(constants.HEARTBEAT_INTERVAL)


def sendBlockReport(name):
    global BlockList

    while True:
        du = psutil.disk_usage(DATA_DIR)
        with gLock.gen_rlock():
            task = {"AvailableCapacity": du.free, "TotalCapacity": du.total, "BlockReport": BlockList.copy()}

        try:
            resp = requests.post('http://127.0.0.1:5002/BlockReport/' + name, json=task)
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
        The response is:

        - 400 for bad request.
        - 200 for success

        :param blockNumber:
        :param data:
        :return:
        """
        global BlockList

        args = request.get_json(force=True)
        if "size" not in args or "data" not in args or type(args["size"]) != int:
            abort(HTTPStatus.BadRequest.code)

        with gLock.gen_wlock():
            BlockList[blockNumber] = {"size": args["size"]}

        storeBlockData(blockNumber, args["data"])

    def get(self, blockNumber):
        """
        This methods return the data in the given block number to the client
        The response structure will look like this:

        Response Codes:
        - 404 - block not found
        - 200 - {"data": <bytes>}

        :param blockNumber:
        :return:
        """
        global BlockList

        with gLock.gen_rlock():
            if blockNumber not in BlockList:
                abort(HTTPStatus.NotFound.code)

        data = getBlockData(blockNumber)
        return {"data": data}


class DummyAPI(Resource):
    def get(self):
        return "Hello World!"

    def post(self):
        args = request.get_json(force=True)
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

    # Scan the data before sending blocklist.
    BlockList = scanData(DATA_DIR)

    threading.Thread(target=sendHeartBeats, args=(str(dn_port),)).start()
    threading.Thread(target=sendBlockReport, args=(str(dn_port),)).start()
    print(psutil.virtual_memory().available)
    print(psutil.virtual_memory().total)
    app.run(port=dn_port)
