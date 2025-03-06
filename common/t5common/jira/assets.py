def AssetBuilder:
    """A class to simplify building an asset record

    The most notable feature of this class is the the id_map argument
    to the constructor. This argument should be an dictionary that maps
    a keyword to an asset attribute ID. This serves as a way to
    make code more understandable by providing a way to specify asset values
    with variable names, rather than integers, when buidling the payload to create
    an asset. The keywords found in id_map are expected to be used when calling
    this object.
    """

    def __init__(self, type_id, id_map, unrequired=None):
        """
        Args:
            type_id:    the asset object type ID
            id_map:     a key-value mapping from attribute to attribute id. Keys prvided
                        here will be expected when calling the AssetBuilder object
            unrequired: the attributes that are not required for building the asset payload
        """
        self.type_id = type_id
        self.id_map = id_map
        self.unrequired = unrequired or list()

    def __add_field(self, asset_data, attr_id, value_key, attrs, required=True):
        value = asset_data.get(value_key)
        if value is not None:
            attrs.append({
                    "objectTypeAttributeId": attr_id,
                    "objectAttributeValues": [{'value': value}]
            })
            return True
        elif required:
            raise ValueError(f"Required key {value_key} not provided")
        return False

    def __call__(self, **data):
        """Build the asset record for creating in Jira.

        Args:
            data:   Key-values to specify asset attributes. The keys used here
                    should be the same as the keys specified in id_map in the
                    constructor

        """
        attrs = list()
        for unreq in self.unrequired:
            if self.__add_field(data, self.id_map[unreq], unreq, attrs):
                del data[unreq]
        for req in data:
            self.__add_field(data, self.id_map[req], req, attrs)

        create_data = {
            "objectTypeId": self.type_id,
            "attributes": attrs,
            "hasAvatar": False
        }
        return create_data


