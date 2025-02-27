from argparse import ArgumentParser

from lapinpy.config_util import ConfigManager
from pymongo import MongoClient

def main(argv=None):
    """Initialize MySQL database needed for running tape application

    :param argv: command-line arguments following the subcommand this function
                 is assigned to[, optional | , default: value]
    :raises exception: description
    :return: None
    """
    parser = ArgumentParser(description="Initialize MySQL database needed "
                                        "for running tape application")
    parser.add_argument("lapin_config", help="The lapinpy config file")
    parser.add_argument("analysis_config", help="The metadata config file")
    tape_args = parser.add_argument_group("Tape", "Arguments for initializing tape database")
    tape_args.add_argument("-c", "--clean", action='store_true', default=False,
                           help="drop all existing tables before creating more")
    args = parser.parse_args(argv)

    lconf = ConfigManager(args.lapin_config).settings
    lconf = lconf['shared']

    # Connect to MongoDB with authentication
    lconf['mongoserver'] = '%s:%s' % (lconf['mongoserver'], lconf.get('mongo_port', '27017'))
    client = MongoClient('mongodb://{mongo_user}:{mongo_pass}@{mongoserver}'.format(**lconf))
    print("Connecting to MongoDB as {mongo_user} at {mongoserver}".format(**lconf))

    # Access the database
    db = client[lconf['meta_db']]

    # List of collections to create
    collections = [
        'analysis',
        'analysis_macros',
        'analysis_plocations',
        'analysis_publishingflags',
        'analysis_tag_template',
        'analysis_template',
        'email',
        'file',
    ]

    # Function to create a collection by inserting and then removing a dummy document
    def create_collection(collection_name):
        if args.clean:
            db[collection_name].drop()
        db[collection_name].insert_one({ '_id': 0, 'dummy': 'This is a dummy document' })
        db[collection_name].delete_one({ '_id': 0 })

    # Create each collection
    for collection in collections:
        create_collection(collection)
        print("Created collection '%s'" % collection)


if __name__ == '__main__':
    main()

