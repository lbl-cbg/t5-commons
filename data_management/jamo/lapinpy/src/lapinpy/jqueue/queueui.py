from lapinpy import restful
from lapinpy.jqueue.tid import TaskObjectId


class QueueUI(restful.Restful):

    def __init__(self, queue_manager):
        restful.Restful.__init__(self)
        self.qm = queue_manager
        self.auto_reload = True

    def post_clearqueue(self, args, kwargs):
        self.qm.get_queue(kwargs['queue'], False).set_status('Normal')

    def post_pausequeue(self, args, kwargs):
        self.qm.get_queue(kwargs['queue'], False).set_status('Paused')

    queue_map = {
        "queue": {'order': 0},
        'status': {'order': 1},
        'in_progress': {'order': 2},
        'queued': {'order': 3, 'type': 'html', 'value': '<a href="{{module}}/queuetasks/{queue}">{queued}</a>'},
        'failed': {'order': 4, 'type': 'html', 'value': '<a href="{{module}}/queueerrors/{queue}">{failed}</a>'},
        'clear state': {'type': 'html', 'value': restful.Button('Clear', post_clearqueue, 'queue')},
        'pause': {'type': 'html', 'value': restful.Button('Pause', post_pausequeue, 'queue')},
    }

    @restful.table('Queue status', map=queue_map)
    def get_queuestatus(self, args, kwargs):
        return self.qm.status()

    @restful.validate({'status': {'type': str}}, [{'name': 'queue', 'type': str}])
    def put_queuestatus(self, args, kwargs):
        st = kwargs['status']
        queue = self.qm.get_queue(args[0], False)
        queue.status = st

    def post_requeue(self, args, kwargs):
        self.qm.requeue(TaskObjectId(kwargs['tid']))

    @restful.table('Errors', map={'tid': {'order': 0}, 'error': {'order': 1, 'type': 'string', 'value': '{data.message}'}, 'requeue': {'type': 'html', 'value': restful.Button('Requeue', post_requeue, 'tid')}}, onlyshowmap=True)
    @restful.validate(argsValidator=[{'name': 'queue', 'type': str}])
    def get_queueerrors(self, args, kwargs):
        return self.qm.get_queue('/'.join(args), False).get_error_range(50)

    @restful.table('Tasks')
    @restful.validate(argsValidator=[{'name': 'queue', 'type': str}])
    def get_queuetasks(self, args, kwargs):
        return self.qm.get_queue('/'.join(args), False).get_range(50)
