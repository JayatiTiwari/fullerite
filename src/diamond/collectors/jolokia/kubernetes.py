import json
import urllib2

KUBELET_DEFAULT_PORT = 10255
KUBELET_DEFAULT_HOST = "localhost"


class Kubelet(object):

    def list_pods(self):
        url = "http://{}:{}/pods".format(KUBELET_DEFAULT_HOST, KUBELET_DEFAULT_PORT)
        try:
            response = urllib2.urlopen(url)
            if response['status'] == 200:
                json_str = response.read()
                return json.loads(json_str), None
        except urllib2.URLError as e:
            return None, e
