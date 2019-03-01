import urllib.request
import shutil
import requests

#TODO: COMMENT ABOUT SSL CONTEXT/INSTALL in order to work with HTTPS
#TODO: Store global var of filename?
#TODO: Store global var of file size?

def download_from_s3(url):
    file_name = url.split('/')[-1]
    # Download the file from `url` and save it locally under `file_name`:

    with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
        print("Bytes: " + response.info().get_all("Content-Length")[0] + "\n")

        bytes = int(response.info().get_all("Content-Length")[0])
        megabytes = 1000000
        #TODO: If file is less than block size, do nothing
        if bytes > megabytes:
            print("Number of blocks: " + (bytes / megabytes))
        #TODO: If file is greater than block size, do something
        print("Block Size: " +  + "\n")

def read_in_chunks(file_object, chunk_size=67108864):
    #67108864 = 64 MB in BLOCKS
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def writeToNameNode():
    #TODO: Get file name
    #TODO: Get file size
    #TODO: Send file name + file size to NN, "data.tsv.gz 698828243"

    #TODO: Receive DN List + block size from NN
    requests.get

def getFromNameNode():
    #TODO: Receive list of DN's
    #TODO: Receive list of Blocks + Sort them?

def writeToDataNode():
    #TODO: Open stream of file
    #TODO: Read from range of

def main():
    print("Welcome to the Seattle University File System (SUFS)!")
    print("Would you like to read or write a file? To read type 'read' to write type 'write")
    #TODO: Accept input for 'read' or 'write'

    #TODO: WRITE
    #TODO: Get URL from user via keyboard
    #TODO: Download the file
    #TODO: Attempt to read the file from local drive and 'divide' into blocks
    #Assuming block size is 64MB
    #Small Sample file
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/sample_us.tsv"
    #667MB File, should test 1GB
    url2 = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz"
    download_from_s3(url2)

if __name__ == "__main__":
    main()