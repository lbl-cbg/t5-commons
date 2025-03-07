import abc
import os

import requests

from .connector import JAMOConnector

class JATSubmitter(metaclass=abc.ABCMeta):
    """A class to simplify submitting data to JAT"""

    def __init__(self):
        self.jc = JAMOConnector()

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
        payload = {
                'template_name': self.template_name,
                'template_data': td,
                'source': source,
                'location': os.path.abspath(directory)
        }
        response = self.jc.create_analysis(directory, self.template_name, td, source=source)
        return td, response

    def get_url(self, jat_key):
        """Get the URL for the at JAT record

        Args:
            jat_key:    The key of the JAT record
        """
        if isinstance(jat_key, dict):
            jat_key = jat_key['jat_key']
        return os.path.join(self.jc.host, 'analysis/analysis', jat_key)
