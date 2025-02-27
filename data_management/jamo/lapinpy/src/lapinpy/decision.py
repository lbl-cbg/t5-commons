### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future.utils import iteritems
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import copy
from .common import checkMongoQuery


class SingleNode(object):
    def __init__(self, field, values, complexOnes):
        self.field = field
        self.values = values
        self.complexOnes = []
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, subscriptions in iteritems(complexOnes):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, subscriptions in complexOnes.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            leftovers = []
            ends = []
            for subscription in subscriptions:
                if len(subscription['filter']) == 0:
                    ends.append(subscription['name'])
                else:
                    leftovers.append(subscription)
            if len(leftovers) == 0:
                node = EndNode(ends)
            else:
                node = createTree(leftovers)
                node.ends = ends
            self.complexOnes.append((subscriptions[0]['__val'], node))

    def __repr__(self):
        return self.field + ',' + str(self.values) + ',' + str(self.complexOnes)

    def getValue(self, data):
        keys = self.field.split('.')
        for key in keys:
            if key not in data:
                return None
            else:
                data = data[key]
        return data

    def test(self, data):
        val = self.getValue(data)
        if val is None:
            return []
        ret = []
        for condition, node in self.complexOnes:
            if checkMongoQuery(data, {self.field: condition}):
                ret.extend(node.test(data))
        if isinstance(val, list):
            for v in val:
                if v in self.values:
                    ret.extend(self.values[v].test(data))
        elif val in self.values:
            ret.extend(self.values[val].test(data))
        return ret


class MultipleNode(object):
    def __init__(self):
        self.nodes, self.ends = [], []

    def add(self, node):
        self.nodes.append(node)

    def __repr__(self):
        return str(self.nodes)

    def test(self, data):
        ret = self.ends[:]
        for node in self.nodes:
            ret.extend(node.test(data))
        return ret


class EndNode(object):
    def __init__(self, ends):
        self.ends = ends

    def test(self, data):
        return self.ends[:]

    def __repr__(self):
        return str(self.ends)


def isSimple(value):
    return not isinstance(value, dict) or '$in' in value


def add(hashMap, key, subscription):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if not isinstance(key, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if not isinstance(key, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        subscription['__val'] = key
        key = str(key)
    if key not in hashMap:
        hashMap[key] = [subscription]
    else:
        hashMap[key].append(subscription)


def getMostInCommon(subscriptions):
    key_map = {}
    most_key = None
    most_value = 0
    for subscription in subscriptions:
        for key in subscription['filter']:
            if key in key_map:
                key_map[key] += 1
            else:
                key_map[key] = 1
            if key_map[key] > most_value:
                most_key = key
                most_value = key_map[key]
    return most_key


def createTree(subscriptions):
    '''Create a tree with a single node at the top that will
       be used to filter out records based on the filters

       :param subscriptions: a list of dictonaries with they keys:
            - filter : a dict that is in the form of a mongodb query
            - name : the name of the filter, must be uniq

    '''
    if len(subscriptions) == 0:
        return EndNode([])
    node, leftovers = createNode(subscriptions)
    if len(leftovers) == 0:
        return node
    ret = MultipleNode()
    ret.add(node)
    while len(leftovers) > 0:
        node, leftovers = createNode(leftovers)
        ret.add(node)
    return ret


def unifyKeys(values):
    ret = {}
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    for value, subscriptions in iteritems(values):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # for value, subscriptions in values.items():  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        leftovers = []
        ends = []
        for subscription in subscriptions:
            if len(subscription['filter']) == 0:
                ends.append(subscription['name'])
            else:
                leftovers.append(subscription)
        if len(leftovers) == 0:
            ret[value] = EndNode(ends)
        else:
            ret[value] = createTree(leftovers)
            ret[value].ends = ends
    return ret


def createNode(subscriptions):
    key = getMostInCommon(subscriptions)
    values = {}
    leftovers = []
    complexOnes = {}
    for subscription in subscriptions:
        if key in subscription['filter']:
            aSubscription = copy.deepcopy(subscription)
            del aSubscription['filter'][key]
            value = subscription['filter'][key]
            if isinstance(value, dict) and '$in' in value:
                for item in value['$in']:
                    add(values, item, aSubscription)
            elif isinstance(value, (dict, list)):
                add(complexOnes, value, aSubscription)
            else:
                add(values, value, aSubscription)
        else:
            leftovers.append(subscription)
    values = unifyKeys(values)
    node = SingleNode(key, values, complexOnes)
    return node, leftovers
