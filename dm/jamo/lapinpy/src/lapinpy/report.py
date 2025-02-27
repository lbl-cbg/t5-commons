from lapinpy import restful


# TODO: Do we need this? It hasn't been touched in over a year...
@restful.menu('reports')
class Report(restful.Restful):
    def __init__(self, config=None):
        if config is not None:
            self.config = config

    @restful.menu('Overview')
    def get_overview(self, args, kwargs):
        return []
