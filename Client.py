import urllib.request
import shutil
import requests
import base64
import math
import boto3
import sys

#TODO: COMMENT ABOUT SSL CONTEXT/INSTALL in order to work with HTTPS

#Information about the current file in read or write
currentFileName = ""
currentFileSize = 0
currentNumberOfBlocks = 0
BLOCKSIZE = 67108864
writeResponse = None
readResponse = None
nameNodeIP = ""

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
    global nameNodeIP

    task = {"filename": currentFileName, "filesize" : currentFileSize}
    resp = requests.post('http://'+ nameNodeIP +':5000/AllocateBlocks/', json=task)
    #resp = requests.post('http://127.0.0.1:5002/AllocateBlocks/', json=task)
    if resp.status_code == 200:
        print("Successful post")
        print(resp.json())
        writeResponse = resp.json()
    elif resp.status_code == 406:
        print("HDFS does not have enough storage for replication; please consider adding more data nodes")
    elif resp.status_code == 409:
        print("File has already been allocated; pulling the allocation structure from namenode.")
        raise Exception("not implemented yet; the client needs to call the file blocks API and resend the data here")
    else:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))

def getFromNameNode():
    #TODO: Receive list of DN's
    #TODO: Receive list of Blocks + Sort them?
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/fileblocks/' + currentFileName)
    if resp.status_code != 200:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))
    else:
        readResponse = resp.json()
        print(resp.json())


def getAllBlocksDNs():
    #TODO: Receive list of DN's
    #TODO: Receive list of Blocks + Sort them?
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/AllBlocksDNs/' + currentFileName)
    #resp = requests.get('http://127.0.0.1:5002/AllBlocksDNs/' + currentFileName)
    if resp.status_code != 200:
        print("Error code: " + str(resp.status_code))
    else:
        readResponse = resp.json()
        print(resp.json())


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
    resp = requests.post('http://' + ip + '/BlockData/' + blockID, json=task)
    #resp = requests.post('http://127.0.0.1:' + ip + '/BlockData/' + blockID, json=task)
    if resp.status_code != 200:
        # This means something went wrong.
        #raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        print("Error code: " + str(resp.status_code))
    else:
        print(blockID + " written to " + ip)

#Reading block:ip
def getFromDataNode():
    blockArr = list()
    for block in readResponse:
        blockArr.append(block)

    blockArr.sort(key=lambda x: int(x.split('-')[-1]))
    with open("COPY" + currentFileName, "wb") as fh:
        for blockID in blockArr:
            ip = readResponse[blockID]
            resp = requests.get('http://' + ip + '/BlockData/' + blockID)
            #resp = requests.get('http://127.0.0.1:' + ip + '/BlockData/' + blockID)
            object = resp.json()
            fh.write(base64.b64decode(object["data"]))
    fh.close()


#Reading block:[IP1, IP2, IP3]
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
    if len(sys.argv) < 1:
        print("Missing command line argument.")
        print("Need to enter Name Node's IP")
        print("Terminating")
        exit()
    else:
        nameNodeIP = sys.argv[1]
    # TODO: SET THE NN IP TO CLI ARG, FOR TESTING HARDCODE FOR NOW
    # nameNodeIP = sys.argv[0]
    # validate it?

    print("Welcome to the Seattle University File System (SUFS)!")

    action = input("Would you like to read or write a file? To read type 'read' to write type 'write'")
    action = action.lower()
    file = input("What file would you like to read/write?")
    # TODO: Accept input for 'read' or 'write'
    # TODO: IMPLEMENT C-IN FOR URL TO WRITE
    # TODO: IMPLEMENT C-IN FOR FILE NAME WHEN READING
    # TODO: WRITE
    # TODO: Get URL from user via keyboard
    # TODO: Download the file
    # TODO: Attempt to read the file from local drive and 'divide' into blocks

    # Assuming block size is 64MB
    # Small Sample file
    # 10240 = 10kB
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/sample_us.tsv"

    # 667MB File, should test 1GB
    url2 = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz"
    # global currentFileName
    # global currentFileSize

    #

    global currentFileName
    global currentFileSize
    #currentFileName = file
    currentFileName = "amazon_reviews_us_Electronics_v1_00.tsv.gz"
    # currentFileName = "sctest.png"
    currentFileSize = 698828243
    # currentFileSize = 135000
    if action == "write":
        # download_from_s3(url2)
        s3 = boto3.client('s3',
                          aws_access_key_id = "",
                          aws_secret_access_key = ""
                          )

        # Call S3 to list current buckets
        response = s3.list_buckets()

        bk = s3.get_bucket('my_bucket_name')
        key = bk.lookup('my_key_name')
        currentFileSize = key.size
        # Get a list of all bucket names from the response
        # buckets = [bucket['Name'] for bucket in response['Buckets']]
        s3.download_file('sufsloh', "amazon_reviews_us_Electronics_v1_00.tsv.gz",
                         "amazon_reviews_us_Electronics_v1_00.tsv.gz")
        # Print out the bucket list
        # print("Bucket List: % s" % buckets)

        #download_from_s3(url2)
        putToNameNode()
        putToDataNode()
    elif action == "read":
        getFromNameNode()
        if readResponse is not None:
            getFromDataNode()
        getAllBlocksDNs()
    else:
        print("Invalid action. Terminating")
        exit()

if __name__ == "__main__":
    main()