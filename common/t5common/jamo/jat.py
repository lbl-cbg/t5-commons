import abc
import os


from .connector import JAMOConnector

class JATSubmitter(metaclass=abc.ABCMeta):
    """A class to simplify submitting data to JAT"""

    def __init__(self):
        self.jamo_connector = JAMOConnector()

    @abc.abstractmethod
    def get_template_data(self, directory, *args, **kwargs):
        """A function to aggregate all outputs and metadata into
        a dictionary that can be used for submitting analysis results
        (or any other set of files) to JAMO through the JAT API

        Args:
            directory:  The directory to build the submission from.
            args:       Any positional arguments to pass to the user implementaiton
            kwargs:     Any keyword arguments to pass to the user implementaiton
        """
        pass

    @property
    @abc.abstractmethod
    def template_name(self):
        """The name of the template that will be used for submitting to JAT"""
        pass

    def submit(self, directory, *args, source="nersc", **kwargs):
        """Submit from the given directory to JAT

        Args:
            directory:  The directory to build the submission from. This argument
                        will be passed to get_template_data
            args:       Any positional arguments to pass to the user implementaiton of
                        get_template_data
            source:     The source of the data. See analysis/analysisimport endpoint
                        documentation for more details
            kwargs:     Any keyword arguments to pass to the user implementaiton of
                        get_template_data

        """

        td = self.get_template_data(directory, *args, **kwargs)
        response = self.jamo_connector.create_analysis(directory, self.template_name, td, source=source)
        return td, response

    def analysis_url(self, jat_key, download=False):
        """Get the URL for the at JAT record

        Args:
            jat_key:    The key of the JAT record
        """
        if isinstance(jat_key, dict):
            jat_key = jat_key['jat_key']
        if download:
            return os.path.join(self.jamo_connector.host, 'api/analysis/download', jat_key)
        else:
            return os.path.join(self.jamo_connector.host, 'analysis/analysis', jat_key)

    def file_url(self, oid, download=False):
        """Get the URL for the at JAT record

        Args:
            jat_key:    The key of the JAT record
        """
        if isinstance(oid, dict):
            oid = oid['_id']
        if download:
            return os.path.join(self.jamo_connector.host, 'api/metadata/download', oid)
        else:
            return os.path.join(self.jamo_connector.host, 'metadata/file', oid)
