from decimal import Decimal
import json
import traceback
import threading
import urllib2

from six import string_types

from . import common, decision, restful
from .lapinpy_core import encrypt, decrypt

subscription_validator = {
    'filter': {'type': str, 'doc': 'The filter that must be true for this subscription to be triggered'},
    'methods': {'type': dict, 'validator': {
        'new|update|removed|replaced': {'type': dict, 'validator': {
            'url': {'type': str, 'doc': 'The url to call when this event is triggered'},
            'url_method': {'type': str, 'doc': 'The http method to use'},
            'auth': {'type': str, 'doc': 'The auth header to use when making calls', 'required': False},
            'send_keys': {'type': list, 'doc': 'The keys to send in with this request',
                          'validator': {'*': {'type': str}}, 'required': False}
        }}}},
    'name': {'type': str, 'doc': 'The name to give this subscription, this will be used as the key'},
    'description': {'type': str, 'doc': 'A string that describes what this subscription does'}
}


class Subscription(object):
    def __init__(self, collection):
        self.subscription_collection = collection
        self.db = []
        self.db[collection].ensure_index('name', unique=True)
        Subscription.get_subscriptions = restful.pagetable('Subscriptions', collection,
                                                           map=self.subscription_display_fields, sort=('_id', -1),
                                                           allow_empty_query=True)(self.get_subscriptions)
        self.queue_manager = restful.RestServer.Instance().queueManager
        self.subscriptions = {}
        self.task_thread = threading.Thread(target=self.thread)
        self._stop = threading.Event()
        self.task_thread.start()

    '''
    subscription code

    '''

    def handler(self, obj):
        if isinstance(obj, type):
            return obj.__name__
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return "%.2f" % obj
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))

    @restful.onload
    def createTree(self):
        subscriptions = self.getsubscriptions()
        self.subscriptions = {}
        for subscription in subscriptions:
            self.subscriptions[subscription['name']] = subscription
            subscription['filter'] = common.toMongoObj(subscription['filter'])
        self.subscriptionTree = decision.createTree(subscriptions)

    def __del__(self):
        self._stop.set()

    def stop(self):
        self._stop.set()

    def thread(self):
        import time
        while 1:
            if self._stop.isSet():
                return
            task = self.queue_manager.next(['_web'])
            while task is not None:
                if self._stop.isSet():
                    return
                try:
                    self.url_call(task['data']['url'], task['data']['method'], task['data']['data'],
                                  task['data']['auth_header'])
                except Exception:
                    self.queue_manager.fail(task['tid'], traceback.format_exc(3))
                else:
                    self.queue_manager.finished(task['tid'])
                task = self.queue_manager.next(['web'])
            time.sleep(3)

    @restful.doc('Adds or update a subscription')
    @restful.passreq(True)
    @restful.validate(subscription_validator, allowExtra=False)
    def post_subscription(self, args, kwargs):
        name = kwargs['name']
        if len(kwargs['filter']) < 6:
            raise common.HttpException(400, 'You have passed in an invalid filter')
        if name in self.subscriptions:
            if kwargs['group'] != self.subscriptions[name]['group']:
                raise common.ValidationException(403,
                                                 '''You do not have access to overwrite existing subscription with the name '%s' ''' % name)
            kwargs['_id'] = self.subscriptions[name]['_id']
        common.toMongoObj(kwargs['filter'])
        for method, value in kwargs['methods'].iteritems():
            if 'auth' in value:
                value['auth'] = encrypt(value['auth'])
        ret = self.save(self.subscription_collection, kwargs)
        self.createTree()
        return ret

    @restful.validate(argsValidator=[{'name': 'name', 'type': str, 'doc': 'the name of the service to update'}])
    def put_subscription(self, args, kwargs):
        pass

    @restful.permissions('admin')
    @restful.validate(argsValidator=[{'name': 'name', 'type': str, 'doc': 'the name of the service to delete'}])
    def delete_subscription(self, args, kwargs):
        self.remove(self.subscription_collection, {'name': args[0]})
        self.createTree()

    subscription_display_fields = {
        'name': {'order': 0},
        'description': {'order': 1},
        'user': {'order': 2, 'title': 'Owner'},
        'filter': {'order': 3}
    }

    @restful.menu('subscriptions')
    def get_subscriptions(self, args, kwargs):
        pass

    def getsubscriptions(self):
        return self.query(self.subscription_collection)

    @restful.single
    @restful.validate(argsValidator=[{'name': 'name', 'type': str, 'doc': 'the name of the subscription to get'}])
    def get_subscription(self, args, kwargs):
        return self.query(self.subscription_collection, name=args[0])

    def sub_newrecord(self, record):
        ret = []
        subscriptions_to_call = self.subscriptionTree.test(record)
        for subscription_name in subscriptions_to_call:
            subscription = self.subscriptions[subscription_name]
            if 'new' in subscription['methods']:
                self.call_subscription(subscription_name, 'new', record=record)
                ret.append(subscription_name)
        return ret

    def sub_updatedrecord(self, newRecord, oldRecord):
        ret = ([], [])
        old_subscriptions = oldRecord['_subscriptions'] if '_subscriptions' in oldRecord else []
        subscriptions_to_call = self.subscriptionTree.test(newRecord)
        diff = compare(newRecord, oldRecord)
        for subscription_name in subscriptions_to_call:
            subscription = self.subscriptions[subscription_name]
            if subscription_name not in old_subscriptions:
                if 'new' in subscription['methods']:
                    self.call_subscription(subscription_name, 'new', record=newRecord)
                    ret[0].append(subscription_name)
            elif 'update' in subscription['methods'] and len(diff) > 0:
                self.call_subscription(subscription_name, 'update', record=newRecord, diff=diff)
        for old_subscription in old_subscriptions:
            if old_subscription not in subscriptions_to_call:
                ret[1].append(old_subscription)
                subscription = self.subscriptions[subscription_name]
                if 'invalidate' in subscription['methods']:
                    self.call_subscription(old_subscription, 'invalidated', record=newRecord, diff=diff)
        return ret

    def sub_removedrecord(self, record):
        ''' don't need to check for old subscriptions '''
        subscriptions_to_call = self.subscriptionTree.test(record)
        for subscription_name in subscriptions_to_call:
            subscription = self.subscriptions[subscription_name]
            if 'remove' in subscription['methods']:
                self.call_subscription(subscription_name, 'remove', record=record)

    def sub_replacedrecord(self, newRecord, oldRecord):
        '''Call replaced on all old subscriptions
           and new on any new subscription
        '''
        old_subscriptions = oldRecord['_subscriptions'] if '_subscriptions' in oldRecord else []
        subscriptions_to_call = self.subscriptionTree.test(newRecord)
        diff = None
        for old_subscription in old_subscriptions:
            subscription = self.subscriptions[old_subscription]
            if 'replace' in subscription['methods']:
                if diff is None:
                    diff = compare(newRecord, oldRecord)
                self.call_subscription(old_subscription, 'replace', record=newRecord, diff=diff)

        for subscription_name in subscriptions_to_call:
            subscription = self.subscriptions[subscription_name]
            if subscription_name not in old_subscriptions and 'new' in subscription['methods']:
                self.call_subscription(subscription_name, 'new', record=newRecord)

    def call_subscription(self, subscription_name, event, **data):
        subscription = self.subscriptions[subscription_name]
        if event in subscription['methods']:
            call_settings = subscription['methods'][event]
            if 'send_keys' in call_settings and 'record' in data:
                new_rec = {}
                record = common.customtransform(data['record'])
                for key in call_settings['send_keys']:
                    new_rec[key] = record[key]
                data['record'] = new_rec
            auth = decrypt(call_settings['auth']) if 'auth' in call_settings else None
            self.async_url_call(call_settings['url'], call_settings['url_method'], data, auth_header=auth)

    def post_test(self, args, kwargs):
        if args[0] == 'new':
            self.sub_newrecord(kwargs)
        else:
            self.sub_updatedrecord(kwargs['new'], kwargs['old'])

    def async_url_call(self, url, method, data, auth_header=None):
        web_host = url.split('/')[2]
        self.queue_manager.get_queue(self.subscription_collection + '/' + web_host, resources=['web']).add(
            {'url': url, 'method': method, 'data': data, 'auth_header': auth_header})

    def url_call(self, url, method, data, auth_header=None):
        if url.startswith('local://'):
            urls = url[8:].split('/')
            module = urls[0]
            name = method.lower() + '_' + urls[1]
            args = urls[2:]
            if data is None:
                data = {}
            return restful.run_internal(module, name, *args, **data)

        if data is not None and method == 'GET':
            url_values = ''
            for key in data:
                url_values += '%s=%s&' % (key, data[key])
            if url_values != '':
                url += '?' + url_values
        elif data is None:
            data = ''

        req = urllib2.Request(url)
        req.get_method = lambda: method
        if not isinstance(data, string_types):
            data = json.dumps(data, default=self.handler)
            req.add_header('Content-type', 'application/json')
        else:
            data = data.replace('+', '%2B')
        if method != 'GET':
            req.add_data(data)
            req.add_header('Content-length', len(data))
        if auth_header is not None:
            req.add_header('Authorization', auth_header)

        f = urllib2.urlopen(req)
        response = f.read()
        f.close()
        return response


def compare(dic1, dic2, sub_key=None):
    ret = {}
    for key in dic1.keys():
        display_key = sub_key + "." + key if sub_key is not None else key
        if key in dic2:
            value = dic1[key]
            old_value = dic2[key]
            if isinstance(value, dict):
                ret.update(compare(value, old_value, display_key))
            elif value != old_value:
                ret[display_key] = {'new': value, 'old': old_value}
            del dic2[key]
        else:
            ret[display_key] = {'new': dic1[key]}
    for key, value in dic2.iteritems():
        display_key = sub_key + "," + key if sub_key is not None else key
        ret[display_key] = {'old': value}
    return ret
