from flask import Flask
from flask_restful import Api, Resource, reqparse
import time
import constants
import requests

app = Flask(__name__)
api = Api(app)

LastSeenDNs = {}
listOfBlocksAndDNs = {}

'''
Map of the entire filesystem i.e FSData
Structure:
{
  "/home/file1": {
                    "DN-1": [ (0, 10), (1, 10) ],
                    "DN-2": [ (2, 10), (5, 10) ],
                    "DN-3": [ (2, 10), (0, 10), (1, 10) ],
                    "DN-4": [ (2, 10), (5, 10) ],
                    "DN-5": [ (1, 10), (5, 10), (0, 10) ],
                 },
  "/home/file2": {
                    "DN-2": [ (2, 10), (4, 10) ],
                    "DN-3": [ (2, 10), (4, 10), (1, 10) ],
                    "DN-5": [ (1, 10), (4, 10), (0, 10) ],
                 },
}
'''
FSData = {}

def getActiveDNs():
    ActiveDNs = []
    currentTime = time.monotonic()
    for DataNodeName, timeSeen in LastSeenDNs.items():
        if (currentTime - timeSeen) < constants.HEARTBEAT_TIMEOUT:
            ActiveDNs.append(DataNodeName)
    return ActiveDNs


# get list of active DataNodes
class Heartbeat(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("DataNodeName")
        args = parser.parse_args()

        LastSeenDNs[args["DataNodeName"]] = time.monotonic()
        return 'received heartbeat'


# check for heartbeat timeout if timeout occurs
# then remove DataNode from ActiveDNs list
class HeartbeatTimeout(Resource):
    def post(self):
        # TODO:
        return ''


# receiving block report from DataNodes
class BlockReport(Resource):
    def post(self, DNID):
        # TODO:
        return 'block report received'

    def get(self):
        """
        :return:
        """
        pass

def get_bytes(size, suffix):
    size = int(float(size))
    suffix = suffix.lower()

    if suffix == 'kb' or suffix == 'kib' or suffix == "mb" or suffix == "mib":
        return size
    elif suffix == 'gb' or suffix == 'gib':
        return size * 1024
    elif suffix == 'tb' or suffix == 'tib':
        return size * 1024 * 1024

    return False

class GetListOfBlocksAndDNs(Resource):
    def post(self):
        """
        This methods responds the client with the information about the available active's DNs and list of blocks.
        The response structure will look like this:

        Response:
        {
            "Availble DNs": [
                {
                    "name": <name>,
                    "available_capacity": <float>,
                },
                {
                ...
                }
            ],
            "file_chunks": [0, 64, 128, 192, 256, 320, 384, 448, 512, 576, 640, 704, 768, 832, 896, 960]
        }

        :param filename:
        :param filesize:
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument("filename")
        parser.add_argument("filesize")
        parser.add_argument("size_suffix")

        args = parser.parse_args()

        if (args["filename"] != ""):
            filesize = get_bytes(args["filesize"], args["size_suffix"])
            listOfBlocksAndDNs["file_chunks"] = list(range(0, filesize, constants.BLOCKSIZE))
            listOfBlocksAndDNs["DN_list"] = getActiveDNs()
            return listOfBlocksAndDNs
        return "file not found", 404


class SendFileStructure(Resource):
    def get(self, filename):
        """
        This methods responds the client with entire file structure
        The response structure will look like this:

        Response:
        {
            "/home/file1": {
                    "DN-1": [ (0, 10), (1, 10) ],
                    "DN-2": [ (2, 10), (5, 10) ],
                    "DN-3": [ (2, 10), (0, 10), (1, 10) ],
                    "DN-4": [ (2, 10), (5, 10) ],
                    "DN-5": [ (1, 10), (5, 10), (0, 10) ],
                 }
        }

        :param filename:
        :return:
        """
        if (filename != ""):
            return FSData
        return "file not found", 404


class DummyAPI(Resource):
    def get(self):
        return "Hello World!"
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("name")
        args = parser.parse_args()
        print(args['name'])
        return args['name']

api.add_resource(GetListOfBlocksAndDNs, "/GetListOfBlocksAndDNs/")
api.add_resource(Heartbeat, "/heartbeat/")
api.add_resource(SendFileStructure, "/filestructure/<string:filename>")
api.add_resource(BlockReport, "/BlockReport/<string:DNID>")
api.add_resource(DummyAPI, "/")


if __name__ == '__main__':
     app.run(port='5002')