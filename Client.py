import os
import urllib.request
import shutil
import requests
import base64
import math
import boto3
import sys
import os


# TODO: COMMENT ABOUT SSL CONTEXT/INSTALL in order to work with HTTPS

# Information about the current file in read or write
import constants

currentFileName = ""
currentFileSize = 0
currentNumberOfBlocks = 0
BLOCKSIZE = 67108864
writeResponse = None
readResponse = None
nameNodeIP = ""


# Downloads the file from the given S3 link
# Stores the file information in the global variables
# Stores:
# currentFileName = filename
# currentFileSize = filesize in bytes
# currentNumberOfBlocks = filesize / blocksize, 1 if less than blocksize
def download_from_s3(url):
    file_name = url.split('/')[-1]
    global currentFileName
    global currentFileSize
    global BLOCKSIZE
    global currentNumberOfBlocks

    # Download the file from `url` and save it locally under `file_name`:

    with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

    currentFileName = file_name
    currentFileSize = response.info().get_all("Content-Length")[0]
    if BLOCKSIZE >= float(currentFileSize):
        currentNumberOfBlocks = 1
    else:
        currentNumberOfBlocks = math.ceil(float(currentFileSize) / BLOCKSIZE)


# This method does a POST method to the NameNodeServer
# it will send the name of the file along with the size of the file
# so that the NameNode can generate a block:DataNode list.
# Format of JSON in POST requests:
# {
#   filename: 'file.txt',
#   filesize: 12345
# }

# Format is
# {
#   'blockID1': ['dataNodeIP1', 'dataNodeIP2, ... ],
#   'blockID2': ['dataNodeIP3', 'dataNodeIP4, ... ],
#   'blockID2': ['dataNodeIP7', 'dataNodeIP6, ... ],
# }
def putToNameNode():
    global currentFileName
    global currentFileSize
    global writeResponse
    global nameNodeIP

    task = {"filename": currentFileName, "filesize": currentFileSize}
    resp = requests.post('http://' + nameNodeIP + ':5000/AllocateBlocks/', json=task)
    # resp = requests.post('http://127.0.0.1:5002/AllocateBlocks/', json=task)
    if resp.status_code == 200:
        print("Successful post")
        print(resp.json())
        writeResponse = resp.json()
    elif resp.status_code == 406:
        print("HDFS does not have enough storage for replication; please consider adding more data nodes")
        sys.exit(1)
    elif resp.status_code == 409:
        print("file already exists")
        sys.exit(1)
    else:
        print("Error code: " + str(resp.status_code))
        sys.exit(1)


def getFromNameNode():
    # TODO: Receive list of DN's
    # TODO: Receive list of Blocks + Sort them?
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/fileblocks/' + currentFileName)
    if resp.status_code == 200:
        readResponse = resp.json()
        print(resp.json())
    elif resp.status_code == 404:
        print("file does not exist")
        sys.exit(1)
    elif resp.status_code == 416:
        print("file is incomplete, please finish uploading first")
        sys.exit(1)
    else:
        print("Error code: " + str(resp.status_code))


def getAllBlocksDNs():
    # TODO: Receive list of DN's
    # TODO: Receive list of Blocks + Sort them?
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/AllBlocksDNs/' + currentFileName)
    # resp = requests.get('http://127.0.0.1:5002/AllBlocksDNs/' + currentFileName)
    if resp.status_code != 200:
        print("Error code: " + str(resp.status_code))
    else:
        readResponse = resp.json()
        print(resp.json())


# This method parses through the block list and data nodes
# returned from the NameNodeServer and distributes all
# the blocks accordingly.
def putToDataNode():
    global currentFileName
    global currentNumberOfBlocks
    global writeResponse
    print(currentFileName)

    with open(currentFileName, 'rb') as f:
        # TODO: This is very slow; must be doing reads in parallel.
        for block in writeResponse:
            blockID = block
            replicaIPList = writeResponse[block]
            if replicaIPList:
                blockData = f.read(constants.BLOCKSIZE)
                for ip in replicaIPList:
                    send(blockID, ip, blockData)
            else:
                f.seek(constants.BLOCKSIZE, os.SEEK_CUR)
                # # Do not read, jump since we have no DNs to copy this block to.
                # s = min(constants.BLOCKSIZE, currentFileSize - 1 - f.tell())
                # if s == 0:
                #     continue
                # else:
                #     print(s, currentFileSize, f.tell())
                #     print("done")


# This method sends the given block to the given DataNodeServer
# Called after putToDataNode()
# Parameters:
# blockID - name of the block e.g. file.block.1
# ip - IP address of the DataNode the block should reside in
# blockData - the binary chunk of the data to be sent to the block
def send(blockID, ip, blockData):
    data = str(base64.b64encode(blockData))
    data = data[2: len(data) - 1]

    task = {"data": data, "size": blockData.__sizeof__()}
    resp = requests.post('http://' + ip + '/BlockData/' + blockID, json=task)
    # resp = requests.post('http://127.0.0.1:' + ip + '/BlockData/' + blockID, json=task)
    if resp.status_code != 200:
        print("Error code: " + str(resp.status_code))
    else:
        print(blockID + " written to " + ip)


# Reading block:ip
def getFromDataNode():
    blockArr = list()
    for block in readResponse:
        blockArr.append(block)

    blockArr.sort(key=lambda x: int(x.split('-')[-1]))
    with open("COPY" + currentFileName, "wb") as fh:
        for blockID in blockArr:
            ip = readResponse[blockID]
            resp = requests.get('http://' + ip + '/BlockData/' + blockID)
            # resp = requests.get('http://127.0.0.1:' + ip + '/BlockData/' + blockID)
            object = resp.json()
            fh.write(base64.b64decode(object["data"]))
    fh.close()


# Reading block:[IP1, IP2, IP3]
# def getFromDataNode():
#     blockArr = list()
#     for block in readResponse:
#         blockArr.append(block)
#
#     blockArr.sort(key=lambda x: int(x.split('-')[-1]))
#     with open("COPY" + currentFileName, "wb") as fh:
#         for blockID in blockArr:
#             print(readResponse[blockID])
#             ip = readResponse[blockID][0]
#             resp = requests.get('http://' + ip + '/BlockData/' + blockID)
#             # resp = requests.get('http://127.0.0.1:' + ip + '/BlockData/' + blockID)
#             object = resp.json()
#             fh.write(base64.b64decode(object["data"]))
#     fh.close()


def main():
    global nameNodeIP
    if len(sys.argv) < 3:
        print("Missing command line argument.")
        print("Need to enter Name Node's IP, aws access key id, aws secret")
        print("Terminating")
        exit()
    else:
        nameNodeIP = sys.argv[1]
        keyid = sys.argv[2]
        keysecret = sys.argv[3]
    # TODO: SET THE NN IP TO CLI ARG, FOR TESTING HARDCODE FOR NOW

    #Get user action + filename
    print("Welcome to the Seattle University File System (SUFS)!")
    action = input("Would you like to read or write a file? To read type 'read' to write type 'write'")
    action = action.lower()
    if action != "read" and action != "write":
        print("Invalid action, terminating")
        exit()

    file = input("What file would you like to read/write?")


    # Assuming block size is 64MB
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/sample_us.tsv"

    # 667MB File, should test 1GB
    url2 = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz"


    global currentFileName
    global currentFileSize
    # #currentFileName = file
    # currentFileName = "amazon_reviews_us_Electronics_v1_00.tsv.gz"
    # # currentFileName = "sctest.png"
    # currentFileSize = 698828243
    #
    # if action == "write":
    #     # download_from_s3(url2)
    #
    #     s3 = boto3.client('s3',
    #                       AWS_ACCESS_KEY_ID = "",
    #                       AWS_SECRET_ACCESS_KEY = ""
    #                       )
    #
    #     # Call S3 to list current buckets
    #     response = s3.list_buckets()
    #
    #     bk = s3.get_bucket('sufsloh')
    #     key = bk.lookup(currentFileName)
    #     currentFileSize = key.size
    #     # Get a list of all bucket names from the response
    #     # buckets = [bucket['Name'] for bucket in response['Buckets']]
    #     s3.download_file('sufsloh', "amazon_reviews_us_Electronics_v1_00.tsv.gz",
    #                      "amazon_reviews_us_Electronics_v1_00.tsv.gz")
    #     s3.download_file('sufsloh', currentFileName,
    #                      currentFileName)
    #     # Print out the bucket list
    #     # print("Bucket List: % s" % buckets)
    #
    #     #download_from_s3(url2)
    #     putToNameNode()
    #     putToDataNode()
    # else:
    currentFileName = file
    if action == "write":
        resource = boto3.resource('s3',
                                  aws_access_key_id=keyid,
                                  aws_secret_access_key=keysecret
                                  )
        bk = resource.Bucket('sufsloh')
        bk.download_file(currentFileName, currentFileName)
        currentFileSize = os.path.getsize(currentFileName)

        putToNameNode()
        putToDataNode()
        print("done putToDataNode")
    elif action == "read":
        getFromNameNode()
        if readResponse is not None:
            getFromDataNode()
        getAllBlocksDNs()



if __name__ == "__main__":
    main()
