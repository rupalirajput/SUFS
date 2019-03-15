import sys
import os
import time
import psutil
import requests
import constants
import threading
import socket

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
nameNodeIP = ""


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
    global nameNodeIP

    task = {"DataNodeName": name}
    while True:
        try:
            resp = requests.post('http://' + nameNodeIP + ':5000/heartbeat/', json=task)
            #resp = requests.post('http://127.0.0.1:5002/heartbeat/', json=task)
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
            else:
                print("successfully sent heartbeat")
        except requests.exceptions.ConnectionError as err:
            print("failed to send heartbeat to NN:", err)

        time.sleep(constants.HEARTBEAT_INTERVAL)


def sendBlockReport(name):
    global BlockList
    global nameNodeIP

    while True:
        du = psutil.disk_usage(DATA_DIR)
        with gLock.gen_rlock():
            task = {"AvailableCapacity": du.free, "TotalCapacity": du.total, "BlockReport": BlockList.copy()}

        try:
            resp = requests.post('http://'+ nameNodeIP +':5000/BlockReport/' + name, json=task)
            #resp = requests.post('http://127.0.0.1:5002/BlockReport/' + name, json=task)
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
            else:
                print("successfully sent block report")
        except requests.exceptions.ConnectionError as err:
            print("failed to send block report to NN:", err)

        time.sleep(constants.BLOCKREPORT_INTERVAL)


class SendCopy(Resource):

    def post(self):
        args = request.get_json(force=True)
        print(args)
        if ("block_id" not in args or "target_dn" not in args):
            abort(HTTPStatus.BadRequest.code)
        data = getBlockData(args["block_id"]);
        task = {"size": len(data), "data": data}
        resp = requests.post("http://" + args["target_dn"] + "/BlockData/" + args["block_id"], json=task)
        #resp = requests.post("http://127.0.0.1:" + args["target_dn"] + "/BlockData/" + args["block_id"], json=task)
        if resp.status_code != 200:
            print("Error code:" + str(resp.status_code))
        else:
            print("successfully send copy")


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
api.add_resource(SendCopy, "/SendCopy")

if __name__ == '__main__':
    dn_port = int(sys.argv[1])
    nameNodeIP = sys.argv[2]

    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    nodeID = IPAddr + ":" +str(dn_port)
    print("IP: " + nodeID)


    DATA_DIR = "./blockDataList_" + str(nodeID)
    try:
        os.mkdir(DATA_DIR)
    except FileExistsError:
        pass

    # Scan the data before sending blocklist.
    BlockList = scanData(DATA_DIR)

    # threading.Thread(target=sendHeartBeats, args=(str(dn_port),)).start()
    # threading.Thread(target=sendBlockReport, args=(str(dn_port),)).start()

    threading.Thread(target=sendHeartBeats, args=(nodeID,)).start()
    threading.Thread(target=sendBlockReport, args=(nodeID,)).start()
    print(psutil.virtual_memory().available)
    print(psutil.virtual_memory().total)
    #app.run(port=dn_port)
    app.run(host='0.0.0.0', port=dn_port)