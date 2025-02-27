import copy
from lapinpy.common import checkMongoQuery

NULL = '______NONENULL_________'


class SingleNode:
    def __init__(self, field, values, complexOnes):
        self.field = field
        self.values = values
        self.complexOnes = []
        for key, subscriptions in complexOnes.items():
            leftovers = []
            ends = []
            node = None
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
            # Not ideal, but if the element is a list, then just examine the first one
            if isinstance(data, list):
                data = data[0]
            if key not in data:
                return NULL
            else:
                data = data[key]
        return data

    def test(self, data):
        val = self.getValue(data)
        ret = []
        if val == NULL:
            return []
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


class MultipleNode:
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


class EndNode:
    def __init__(self, ends):
        self.ends = ends

    def test(self, data):
        return self.ends[:]

    def __repr__(self):
        return str(self.ends)


def isSimple(value):
    return not isinstance(value, dict) or '$in' in value


def add(hashMap, key, subscription):
    if not isinstance(key, str):
        subscription['__val'] = key
        if key is not None and not isinstance(key, int):
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
    for value, subscriptions in values.items():
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
