import sys
import time
from flask import Flask
from flask_restful import Api, Resource, reqparse
import requests
import constants
import threading

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('file')

'''
Map of the entire storeage in the current DataNode i.e BlockData
Structure:
{
  "/block1": {
                     data: "actual data1",
                 },
  "/block2": {
                    data: "actual data2",
                 },
}
'''
BlockData = {}


def sendHeartBeats(name):
    while True:
        task = {"DataNodeName": name}
        resp = requests.post('http://127.0.0.1:5002/heartbeat/', json=task)
        if resp.status_code != 200:
            print("Error code: " + str(resp.status_code))
        else:
            print(resp.json())

        time.sleep(constants.HEARTBEAT_INTERVAL)

# send heartbeat to Name Node in a given time frame.
class Heartbeat(Resource):
    def post(self):
        # TODO: make a post request to Name Node
        return ''


# send block report from DataNodes
class BlockReport(Resource):
    def post(self, blockreport):
        # TODO: make a post request contain the block report
        return ''


class BlockData(Resource):
    def post(self, blockNumber, data):
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
        if (blockNumber != ""):
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
        if (blockNumber != ""):
            return BlockData.blockNumber
        return "block not found", 404

api.add_resource(BlockData, "/BlockData/<string:blockNumber>")

if __name__ == '__main__':
    dn_port = int(sys.argv[1])
    threading.Thread(target=sendHeartBeats, args=(str(dn_port),)).start()
    app.run(port = dn_port)
