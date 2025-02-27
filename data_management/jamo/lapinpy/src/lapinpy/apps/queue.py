from lapinpy.jqueue import queueui
from lapinpy import restful


@restful.menu('Queue', 1)
class QueueApp(queueui.QueueUI):

    def __init__(self, config):
        self.config = config
        queueui.QueueUI.__init__(self, restful.RestServer.Instance().queueManager)
        self.auto_reload = True

    @restful.onload
    def finishLoading(self):
        self.queueManager = restful.RestServer.Instance().queueManager

    def post_task(self, args, kwargs):
        return self.queueManager.get_queue(args[0]).add(kwargs)
