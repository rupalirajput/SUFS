import math
import os

import humanfriendly
from flask import Flask, abort
from flask_restful import Api, Resource, reqparse
import time
import constants
from readerwriterlock import rwlock
import werkzeug.exceptions as HTTPStatus

app = Flask(__name__)
api = Api(app)

gLock = rwlock.RWLockRead()
LastSeenDNs = {}

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
    """
    NOTE: It is assumed that the caller will acquire the global data structure lock.

    :return:
    """
    ActiveDNs = []
    currentTime = time.monotonic()
    for DataNodeName, timeSeen in LastSeenDNs.items():
        if (currentTime - timeSeen) < constants.HEARTBEAT_TIMEOUT:
            ActiveDNs.append(DataNodeName)
    return ActiveDNs


def getDNsByAvailableCapacity():
    """
    NOTE: It is assumed that the caller will acquire the global data structure lock.

    :return:
    """
    dns_by_available_capacity = {}
    active_dns = getActiveDNs()
    for dnid, dn_details in filter(lambda e: e[0] in active_dns, FSData.items()):
        # TODO: Try to use the percentage capacity, but also avoid nodes with available capacity smaller than the block size.
        dns_by_available_capacity[dnid] = dn_details["AvailableCapacity"]

    return dns_by_available_capacity


# get list of active DataNodes
class Heartbeat(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("DataNodeName")
        args = parser.parse_args()
        with gLock.gen_wlock():
            LastSeenDNs[args["DataNodeName"]] = time.monotonic()


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
        parser = reqparse.RequestParser()
        parser.add_argument("AvailableCapacity")
        parser.add_argument("BlockReport")
        parser.add_argument("TotalCapacity")
        args = parser.parse_args()
        with gLock.gen_wlock():
            FSData[DNID] = {
                "BlockList": args["BlockReport"],
                "AvailableCapacity": int(args["AvailableCapacity"]),
                "TotalCapacity": int(args["TotalCapacity"])
            }
            print(FSData[DNID])

    def get(self):
        """
        :return:
        """
        pass


# {
# “File1.txt”: {
# “File1block1": [“127.0.0.1”, “127.0.0.2",],
# “File2block2”: [“127.0.0.3", “127.0.0.2”,]
# }

class AllocateBlocks(Resource):
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
        args = parser.parse_args()

        filesize = humanfriendly.parse_size(args["filesize"])
        allocation_table = {}
        with gLock.gen_rlock():
            dns_by_available_capacity = getDNsByAvailableCapacity()
            for block_num in range(math.ceil(filesize / constants.BLOCKSIZE)):
                block_id = args["filename"] + ".block." + str(block_num)
                allocation_table[block_id] = []
                for _ in range(constants.REPLICATION_FACTIOR):
                    # print(dns_by_available_capacity)
                    unique_dns = list(filter(lambda dnid: dnid not in allocation_table[block_id], dns_by_available_capacity))
                    # print("getting biggest dn from", unique_dns, "excluding", allocation_table[block_id])
                    try:
                        biggest_dn = max(unique_dns, key=dns_by_available_capacity.get)
                    except ValueError:
                        # If we cannot find a unique dana node for this block then tell the client that we do not have
                        # enough nodes.
                        abort(HTTPStatus.NotAcceptable.code)
                    else:
                        # TODO: IF this is the last block then it might be smaller than the actual blocksize.
                        #  So, to be super efficient, utilize the smaller size instead.
                        dns_by_available_capacity[biggest_dn] -= constants.BLOCKSIZE
                        allocation_table[block_id].append(biggest_dn)

        return allocation_table


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
        available_DNs = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, block_details in dn_details["BlockList"]:
                    fname, part = os.path.splitext(blockID)
                    if fname == filename:
                        if dnid in available_DNs:
                            available_DNs[dnid].append(part)
                        else:
                            available_DNs[dnid] = [part]

        if len(available_DNs) > 0:
            return available_DNs

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


api.add_resource(AllocateBlocks, "/AllocateBlocks/")
api.add_resource(Heartbeat, "/heartbeat/")
api.add_resource(SendFileStructure, "/filestructure/<string:filename>")
api.add_resource(BlockReport, "/BlockReport/<string:DNID>")
api.add_resource(DummyAPI, "/")

if __name__ == '__main__':
    app.run(port='5002')
