from argparse import ArgumentParser
import sys

import pymysql

from .config_util import ConfigManager


def init(argv=None):
    """Initialize core MySQL database needed for running Lapin

    :param argv: command-line arguments following the subcommand this function
                 is assigned to[, optional | , default: value]
    :raises exception: description
    :return: None
    """
    parser = ArgumentParser(description="Initialize core MySQL database needed "
                                        "for running LapinPy")
    parser.add_argument("config", help="The LapinPy config file to use for "
                                       "initializeing the requisite MySQL "
                                       "database")
    args = parser.parse_args(argv)

    conf = ConfigManager(args.config).settings
    db = conf['core_db']
    conf = conf['shared']

    statements = [
        """create table if not exists modules(
            name varchar(32),
            path varchar(256),
            CONSTRAINT name_uniq_idx UNIQUE (name));""",

        """create table if not exists application(
            id INTEGER  not null primary key AUTO_INCREMENT,
            name varchar(100) not null,
            `group` varchar(32) not null,
            division varchar(64) not null default '{default_division}');""",

        """create table if not exists permission(
            id INTEGER  not null primary key AUTO_INCREMENT,
            name varchar(32) not null,
            CONSTRAINT name_idx UNIQUE (name));""",

        """create table if not exists application_permissions(
            id INTEGER  not null primary key AUTO_INCREMENT,
            application INTEGER  not null,
            permission INTEGER  not null,
            foreign key (permission) references permission (id),
            foreign key (application) references application (id));""",

        """create table if not exists chart_conf(
            id INTEGER  not null primary key AUTO_INCREMENT,
            url varchar(126) not null,
            conf longtext not null,
            CONSTRAINT url_idx UNIQUE (url));""",

        """create table if not exists menu(
            id INTEGER  not null primary key AUTO_INCREMENT,
            parent INTEGER  not null,
            place tinyint not null,
            href varchar(126) not null,
            title varchar(126) not null);""",

        """create table if not exists user(
            user_id INTEGER  not null primary key AUTO_INCREMENT,
            email varchar(126) not null,
            name varchar(126) not null,
            `group` varchar(32),
            division varchar(64) not null default '{default_division}',
            CONSTRAINT user_email_idx UNIQUE (email));""",

        """create table if not exists user_tokens(
            id INTEGER  not null primary key AUTO_INCREMENT,
            user_id INTEGER  not null,
            token varchar(32) not null,
            CONSTRAINT user_token_idx UNIQUE (token),
            foreign key (user_id) references `user` (user_id));""",

        """create table if not exists application_tokens(
            id INTEGER  not null primary key AUTO_INCREMENT,
            application_id INTEGER  not null,
            token varchar(32) not null,
            created timestamp default current_timestamp,
            CONSTRAINT application_token_idx UNIQUE (token),
            foreign key (application_id) references `application` (id));""",

        """create table if not exists user_permissions(
            id INTEGER  not null primary key AUTO_INCREMENT,
            user_id INTEGER  not null,
            permission INTEGER  not null,
            foreign key (user_id) references user (user_id),
            foreign key (permission) references permission (id));""",

        """create table if not exists permission_group(
            id INTEGER  not null primary key AUTO_INCREMENT,
            permission INTEGER  not null,
            has_permission INTEGER  not null,
            foreign key(permission) references permission(id),
            foreign key(has_permission) references permission(id));""",

        """create table if not exists setting(
            id INTEGER  not null primary key AUTO_INCREMENT,
            application varchar(32) not null,
            setting varchar(32) not null,
            value varchar(1024) not null);""",

        "insert into setting values (null,'core','db_version','1.0.4');",

        """create table if not exists job(
            job_id INT NOT NULL AUTO_INCREMENT,
            job_path TEXT,
            job_name TEXT,
            sge_id INT,
            submitted_date TEXT,
            started_date TEXT,
            ended_date TEXT,
            status TEXT,
            exit_code INT,
            machine TEXT,
            platform TEXT,
            minutes FLOAT,
            cores INT,
            pipeline TEXT,
            process TEXT,
            record_id TEXT,
            record_id_type TEXT,
            PRIMARY KEY (job_id)
        );
        """,
    ]

    stmt = None
    try:
        connection = pymysql.connect(host=conf['mysql_host'], user=conf['mysql_user'], password=conf['mysql_pass'], port=conf.get('mysql_port'))
        cur = connection.cursor()

        cur.execute("create database if not exists %s" % db)
        cur.execute("use %s" % db)
        for stmt in statements:
            stmt = stmt.format(**conf)
            cur.execute(stmt)

    except pymysql.MySQLError as e:
        print("Error when executing the following statement:\n%s" % stmt, file=sys.stderr)
        print(f"Error: {e}")
        exit(1)

    print("All tables successfully created")
