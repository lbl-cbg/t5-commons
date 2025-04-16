import json

class MetadataBuilder:
    """A class to help build metadata records for submitting
    analysis outputs to JAT"""

    def __init__(self):
        self.doc = {
                'outputs': list(),
                'metadata': dict(),
                'inputs': list()
                }

    def add_output(self, label, path, **metadata):
        """Add an output to this analysis

        All key-values pairs found in metadata will be
        added as file-level metadata to the output
        """
        self.doc['outputs'].append({
            "file": path,
            "label": label,
            "metadata": metadata
            })

    def add_input(self, metadata_id):
        """Add an input to this analysis"""
        self.doc['inputs'].append(metadata_id)

    def add_metadata(self, **metadata):
        """Add analysis-level metadata

        All key-values pairs found in metadata will be
        added as analysis-level metadata
        """
        for key, value in metadata.items():
            self.doc['metadata'][key] = value

    def write(self, path):
        """Write the submission record to a file"""
        with open(path, 'w') as f:
            json.dump(f, self.doc)
