from flask import Flask
from flask_restful import Api, Resource, reqparse

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('file')

ActiveDNs = {}
ListofBlocks = {}

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

# get list of active DataNodes
class Heartbeat(Resource):
    def post(self, DataNode):
        ActiveDNs.add(DataNode)
        return 'received heartbeat'


# check for heartbeat timeout if timeout occurs
# then remove DataNode from ActiveDNs list
class HeartbeatTimeout(Resource):
    def post(self):
        # TODO:
        return ''


# receiving block report from DataNodes
class BlockReport(Resource):
    def post(self, blockreport):
        # TODO:
        return 'block report received'

    def get(self):
        """
        :return:
        """
        pass


class DetermineBlocks(Resource):
    def get(self, filename, filesize, blocksize, replicationfactor):
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
            "file_chunks": [0, 300, 700, 890, 1000]
        }

        :param filename:
        :param filesize:
        :param blocksize:
        :param replicationfactor:
        :return:
        """
        if (filename != ""):
            chunk = filesize / blocksize
            ListofBlocks.add(chunk)
            return ListofBlocks, ActiveDNs
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

api.add_resource(DetermineBlocks, "/determineblocks/<string:filename>/<int:filesize>/<int:blocksize>/<int:replicationfactor>")
api.add_resource(Heartbeat, "/heartbeat/<string:DataNode>")
api.add_resource(SendFileStructure, "/filestructure/<string:filename>")
#api.add_resource(DummyAPI, "/DummyAPI/hello")
api.add_resource(DummyAPI, "/")


if __name__ == '__main__':
     app.run(port='5002')