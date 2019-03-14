import math
import os
import random
import threading
import time

import requests

import constants

from readerwriterlock import rwlock
import werkzeug.exceptions as HTTPStatus
from flask import Flask, abort
from flask_restful import Api, Resource, request
from collections import Counter

app = Flask(__name__)
api = Api(app)

gLock = rwlock.RWLockRead()
LastSeenDNs = {}

'''
Map of the entire filesystem i.e FSData
Structure:
{
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    },
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    },
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    }
}
'''
FSData = {}


def rebalanceData():
    # Get the list of active dns for each blockid.
    activeDNs = None
    dnsByAvlCap = None
    with gLock.gen_rlock():
        activeDNs = getActiveDNs()
        dnsByAvlCap = getDNsByAvailableCapacity()
        allBlockIDs = {}
        for dnid, dnDetails in FSData.items():
            if dnid not in activeDNs:
                print("node failed: ", dnid)
                continue
            for blockid, _ in dnDetails["BlockList"].items():
                if blockid in allBlockIDs:
                    allBlockIDs[blockid].append(dnid)
                else:
                    allBlockIDs[blockid] = [dnid]

    for blockid, dns in allBlockIDs.items():
        if len(dns) >= constants.REPLICATION_FACTIOR:
            continue
        # Find the DNs which do not have a copy yet.
        possibleTargetDNs = list(set(activeDNs).difference(set(dns)))
        # Select the DNs which have the most available capacity.
        possibleTargetDNs.sort(key=dnsByAvlCap.get, reverse=True)
        # Iterate over the selected DNs.
        for dnid in possibleTargetDNs[:constants.REPLICATION_FACTIOR - len(dns)]:
            # This can be improved for better parallelization, but picking the ranom request DNID for now.
            requestDN = random.choice(dns)
            print("requesting DN(" + requestDN + ") to copy block " + blockid + " to DN(" + dnid + ")")
            resp = requests.post("http://127.0.0.1:" + requestDN + "/SendCopy", json={"block_id": blockid, "target_dn": dnid})
            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
                return
            # TODO: Now we should wait for next block request from dnid before creating more copies of the data.
            print(blockid + " written to " + dnid)


def redundancyManager():
    while True:
        try:
            rebalanceData()
        except Exception as err:
            print(err)
        time.sleep(3)


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


def getReplicaCount():
    """
        This function will give us the count of replica for each data block of each DN

    :return:
    """
    replicaCount = Counter(replicaCnt['BlockList'] for replicaCnt in FSData)
    print(replicaCount)
    return replicaCount


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
        args = request.get_json(force=True)
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
        args = request.get_json(force=True)
        if type(args["BlockReport"]) != dict:
            abort(HTTPStatus.BadRequest.code)

        with gLock.gen_wlock():
            FSData[DNID] = {
                "BlockList": args["BlockReport"],
                "AvailableCapacity": int(args["AvailableCapacity"]),
                "TotalCapacity": int(args["TotalCapacity"])
            }

    def get(self):
        """
        :return:
        """
        pass


class AllocateBlocks(Resource):
    def post(self):
        """
        This methods responds the client with the information about the available active's DNs and list of blocks.
        The response structure will look like this:

        Response:
        {
            # {
            # “File1.txt”: {
            # “File1block1": [“127.0.0.1”, “127.0.0.2",],
            # “File2block2”: [“127.0.0.3", “127.0.0.2”,]
            # }
        }
        :return:
        """
        args = request.get_json(force=True)
        if "filesize" not in args or "filename" not in args or type(args["filename"]) != str or type(
                args["filesize"]) != int:
            abort(HTTPStatus.BadRequest.code)

        allocation_table = {}
        with gLock.gen_rlock():
            dns_by_available_capacity = getDNsByAvailableCapacity()
            for block_num in range(math.ceil(args["filesize"] / constants.BLOCKSIZE)):
                block_id = args["filename"] + ".block-" + str(block_num)
                allocation_table[block_id] = []
                for _ in range(constants.REPLICATION_FACTIOR):
                    # print(dns_by_available_capacity)
                    unique_dns = list(
                        filter(lambda dnid: dnid not in allocation_table[block_id], dns_by_available_capacity))
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


class GetFileStructure(Resource):
    def get(self, filename):
        """
        This methods responds the client with entire file structure
        The response structure will look like this:

        Response:
        {
          "5003": [
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-3",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-2",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1"
          ],
          "5004": [
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-3",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-2",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1"
          ],
          "5005": [
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-3",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-2",
            "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1"
          ]
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
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if dnid in available_DNs:
                            available_DNs[dnid].append(blockID)
                        else:
                            available_DNs[dnid] = [blockID]

        if not available_DNs:
            abort(HTTPStatus.NotFound.code)

        return available_DNs


# TODO: What if the blocks are missing ? i.e. how do we verify that we have gotten the complete file ?
class GetFileBlocks(Resource):
    def get(self, filename):
        """
        This methods responds the client with the load-balanced set of data-nodes to read the blocks from.
        The response structure will look like this:

        Response:
        {
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-10" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-4" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-5" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-6" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-9" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-7" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-2" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-0" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-8" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-3" : "5003"
        }

        :type filename: str
        :param filename: name of the file
        :return:
        """
        blocks = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if blockID in blocks:
                            blocks[blockID].append(dnid)
                        else:
                            blocks[blockID] = [dnid]

        if not blocks:
            abort(HTTPStatus.NotFound.code)

        # Distribute the read load evenly among all data nodes.
        uniformDNs = {}
        counter = Counter()
        # Prever the blocks which have the lowest DN count.
        for blockID, dns in sorted(blocks.items(), key=lambda e: len(e[1])):
            selectedDN = None
            # First find a DN which has not been used earlier.
            for dn in dns:
                if dn not in uniformDNs.values():
                    selectedDN = dn
                    break
            else:
                # If we cannot find a unique DN then use the one with the lowest frequency.
                for dn, _ in counter.most_common()[::-1]:
                    # We cannot use a DN which doesn't serve this blockID, so filter out those which do.
                    if dn in dns:
                        selectedDN = dn
                        break

            # Record the block DN host for final read.
            uniformDNs[blockID] = selectedDN
            # Also update our stats so that we can avoid re-using this DN too much
            # and uniformly distribute the read-load over all the DNs.
            counter.update([selectedDN])

        return uniformDNs


# Get entire block structure with all DNs
class GetAllBlocksDNs(Resource):
    def get(self, filename):
        """
        This methods responds the client with the load-balanced set of data-nodes to read the blocks from.
        The response structure will look like this:

        Response:
        {
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-10" : "5005", "5004", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-4" : "5004", "5005", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1" : "5004", "5003" , "5005"
        }

        :type filename: str
        :param filename: name of the file
        :return:
        """
        blocks = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if blockID in blocks:
                            blocks[blockID].append(dnid)
                        else:
                            blocks[blockID] = [dnid]

        if not blocks:
            abort(HTTPStatus.NotFound.code)

        return blocks

class DummyAPI(Resource):
    def get(self):
        return "Hello World!"

    def post(self):
        args = request.get_json()
        print(args['name'])
        print(args['size'])
        return args['name']


api.add_resource(AllocateBlocks, "/AllocateBlocks/")
api.add_resource(Heartbeat, "/heartbeat/")
api.add_resource(GetFileStructure, "/filestructure/<string:filename>")
api.add_resource(GetFileBlocks, "/fileblocks/<string:filename>")
api.add_resource(GetAllBlocksDNs, "/AllBlocksDNs/<string:filename>")
api.add_resource(BlockReport, "/BlockReport/<string:DNID>")
api.add_resource(DummyAPI, "/")

if __name__ == '__main__':
    threading.Thread(target=redundancyManager).start()
    app.run(port='5002')
