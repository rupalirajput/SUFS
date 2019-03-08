import urllib.request
import shutil
import requests
import base64
import math

#TODO: COMMENT ABOUT SSL CONTEXT/INSTALL in order to work with HTTPS

#Information about the current file in read or write
currentFileName = ""
currentFileSize = 0
currentNumberOfBlocks = 0
BLOCKSIZE = 67108864
writeResponse = None
readResponse = None

#Downloads the file from the given S3 link
#Stores the file information in the global variables
#Stores:
#currentFileName = filename
#currentFileSize = filesize in bytes
#currentNumberOfBlocks = filesize / blocksize, 1 if less than blocksize
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
        currentNumberOfBlocks = math.ceil(float(currentFileSize)/BLOCKSIZE)

def read_in_chunks(file_object, CHUNK_SIZE=67108864):
    #67108864 = 64 MB in BLOCKS
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    global currentFileName

    f = open(currentFileName, 'rb')
    chunk = f.read(CHUNK_SIZE)
    while chunk:  # loop until the chunk is empty (the file is exhausted)
        print("BLOCK READ")
        chunk = f.read(CHUNK_SIZE)  # read the next chunk
    f.close()

# This method does a POST method to the NameNodeServer
# it will send the name of the file along with the size of the file
# so that the NameNode can generate a block:DataNode list.
# Format of JSON in POST requests:
#{
#   filename: 'file.txt',
#   filesize: 12345
#}

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

    task = {"filename": currentFileName, "filesize" : currentFileSize}
    resp = requests.post('http://127.0.0.1:5002/AllocateBlocks/', json=task)
    if resp.status_code == 200:
        print("Successful post")
        print(resp.json())
        writeResponse = resp.json()
    elif resp.status_code == 406:
        print("HDFS does not have enough storage for replication; please consider adding more data nodes")
    else:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))

def getFromNameNode():
    #TODO: Receive list of DN's
    #TODO: Receive list of Blocks + Sort them?
    global currentFileName
    global readResponse
    resp = requests.get('http://127.0.0.1:5002/fileblocks/' + currentFileName)
    if resp.status_code != 200:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))
    else:
        readResponse = resp.json()
        print(resp.json())

#WORKS FOR SINGLE DATA NODE STORING ALL BLOCKS
# def putToDataNode(blockID, chunk):
#     data = str(base64.b64encode(chunk))
#     data = data[2: len(data) - 1]
#
#
#     task = {"data": data, "size": str(chunk.__sizeof__())}
#     resp = requests.post('http://127.0.0.1:5005/BlockData/' + blockID, json=task)
#     if resp.status_code != 200:
#         # This means something went wrong.
#         #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
#         print("Error code: " + str(resp.status_code))
#     else:
#         print("Successful post")
#         print(resp.json())

#This method parses through the block list and data nodes
#returned from the NameNodeServer and distributes all
#the blocks accordingly.
def putToDataNode():
    global currentFileName
    global currentNumberOfBlocks
    global writeResponse
    CHUNK_SIZE = 67108864
    print(currentFileName)

    f = open(currentFileName, 'rb')
    blockData = f.read(CHUNK_SIZE)


    # TODO: This is very slow; must be doing reads in parallel.
    while writeResponse != None and blockData:
        for block in writeResponse:
            if blockData:
                blockID = block
                replicaIPList = writeResponse[block]
                for ip in replicaIPList:
                    send(blockID, ip, blockData)
                blockData = f.read(CHUNK_SIZE)

#This method sends the given block to the given DataNodeServer
#Called after putToDataNode()
#Parameters:
#blockID - name of the block e.g. file.block.1
#ip - IP address of the DataNode the block should reside in
#blockData - the binary chunk of the data to be sent to the block
def send(blockID, ip, blockData):
    data = str(base64.b64encode(blockData))
    data = data[2: len(data) - 1]

    task = {"data": data, "size": blockData.__sizeof__()}
    resp = requests.post('http://127.0.0.1:' + ip + '/BlockData/' + blockID, json=task)
    if resp.status_code != 200:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))
    else:
        print(blockID + " written to " + ip)

# def getFromDataNode():
#     blockID = "block"
#     newFile = open("newfileTest.tsv.gz", 'wb')
#     with open("newfileTest.tsv.gz", "wb") as  fh:
#         for i in range (1, (currentNumberOfBlocks + 1)):
#             resp = requests.get('http://127.0.0.1:5005/BlockData/' + str(blockID) + str(i))
#             object = resp.json()
#             #print(object)
#             #print(json.dumps(object))
#             currentblock = base64.b64decode(object["data"])
#
#             #newFile.write(currentblock)
#             fh.write(base64.b64decode(object["data"]))
#     newFile.close()

def getFromDataNode():
    blockArr = list()
    for block in readResponse:
        blockArr.append(block)

    blockArr.sort(key=lambda x: int(x.split('-')[-1]))
    with open("COPY" + currentFileName, "wb") as fh:
        for blockID in blockArr:
            ip = readResponse[blockID]
            resp = requests.get('http://127.0.0.1:' + ip + '/BlockData/' + blockID)
            object = resp.json()
            fh.write(base64.b64decode(object["data"]))
    fh.close()



    # with open(currentFileName, "wb") as  fh:
    #     for i in range(1, (currentNumberOfBlocks + 1)):
    #         resp = requests.get('http://127.0.0.1:5005/BlockData/' + str(blockID) + str(i))
    #         object = resp.json()
    #         # print(object)
    #         # print(json.dumps(object))
    #         currentblock = base64.b64decode(object["data"])
    #
    #         # newFile.write(currentblock)
    #         fh.write(base64.b64decode(object["data"]))
    # newFile.close()

def main():
    print("Welcome to the Seattle University File System (SUFS)!")

    action = input("Would you like to read or write a file? To read type 'read' to write type 'write'")
    action = action.lower()
    #TODO: Accept input for 'read' or 'write'
    #TODO: IMPLEMENT C-IN FOR URL TO WRITE
    #TODO: IMPLEMENT C-IN FOR FILE NAME WHEN READING
    #TODO: WRITE
    #TODO: Get URL from user via keyboard
    #TODO: Download the file
    #TODO: Attempt to read the file from local drive and 'divide' into blocks

    #Assuming block size is 64MB
    #Small Sample file
    #10240 = 10kB
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/sample_us.tsv"

    #667MB File, should test 1GB
    url2 = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz"
    # global currentFileName
    # global currentFileSize

    #

    global currentFileName
    global currentFileSize
    currentFileName = "amazon_reviews_us_Electronics_v1_00.tsv.gz"
    currentFileSize = 698828243
    if action == "write":

        download_from_s3(url2)
        putToNameNode()
        putToDataNode()
    elif action == "read":
        getFromNameNode()
        getFromDataNode()
    else:
        print("Invalid action. Terminating")
        exit()

if __name__ == "__main__":
    main()