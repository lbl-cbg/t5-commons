import sdm_logger
import platform
import os
import sys
import subprocess
import re
import argparse
from sdm_curl import Curl


class RemoteSourceConfigurationException(Exception):
    """Exception to be raised if remote source configuration is missing required fields.
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class EgressHandler:

    def __init__(self, curl, remote_data_center_source):
        sdm_logger.config(f'egress_handler-{platform.node()}.log', emailTo='jgi-dm@lbl.gov', curl=curl)
        self.logger = sdm_logger.getLogger('egress_handler')
        self.remote_data_center_source = remote_data_center_source
        self.sdm_curl = curl
        self.dm_archive_root, self.config = self._get_config(self.remote_data_center_source, self.sdm_curl)
        self.cv = self.sdm_curl.toStruct(self.sdm_curl.get('api/tape/cvs'))

    def run(self):
        """Run rsync to copy pending egress requests files locally if file does not already exist.
        """
        egress_requests = self.sdm_curl.get(f'api/tape/registered_egress_requests/{self.remote_data_center_source}')
        for egress_request in egress_requests:
            from_path = os.path.join(egress_request.get('file_path').replace(f'{self.dm_archive_root}/', ''),
                                     egress_request.get('file_name'))
            to_path = f'{self.config.get("dm_archive_root_source")}/{from_path}'
            if os.path.exists(to_path):
                # File exists in remote data center dm_archive, set the egress request status to `COMPLETE`
                self.sdm_curl.put(f'api/tape/egress_request/{egress_request.get("egress_id")}',
                                  egress_status_id=self.cv.queue_status.COMPLETE,
                                  bytes_transferred=0)
            else:
                # File does not exist in remote data center dm_archive
                # Check that there aren't any `IN_PROGRESS` requests to prevent multiple rsync requests for the same
                # file since the rsync may take a while for large files
                if any([request.get('egress_status_id') == self.cv.queue_status.IN_PROGRESS for request in
                        self.sdm_curl.get(
                            f'api/tape/egress_requests/{egress_request.get("source")}/{egress_request.get("file_id")}')]):
                    # Skip this request since there are `IN_PROGRESS` requests for the given file at the remote data
                    # center
                    continue
                # Set egress request status to `IN_PROGRESS`
                self.sdm_curl.put(f'api/tape/egress_request/{egress_request.get("egress_id")}',
                                  egress_status_id=self.cv.queue_status.IN_PROGRESS)
                try:
                    # Create subdirectories for destination path if needed
                    if not os.path.exists(os.path.dirname(to_path)):
                        os.makedirs(os.path.dirname(to_path))
                    rsync_results = self._rsync(from_path, to_path, self.config.get('rsync_uri'),
                                                self.config.get('rsync_password'))
                    copied_file_size = rsync_results.get('total_transferred_file_size')
                    if copied_file_size == 0:
                        raise Exception(f'Copied file size is 0 for {to_path}')
                    # rsync successful, set egress status to `COMPLETE`
                    self.sdm_curl.put(f'api/tape/egress_request/{egress_request.get("egress_id")}',
                                      egress_status_id=self.cv.queue_status.COMPLETE,
                                      bytes_transferred=copied_file_size)
                except Exception as e:
                    # Error with rsync copy, set egress status to `FAILED`
                    self.sdm_curl.put(f'api/tape/egress_request/{egress_request.get("egress_id")}',
                                      egress_status_id=self.cv.queue_status.FAILED)
                    self.logger.warning(
                        f'rsync from {self.dm_archive_root}/{from_path} to {to_path} failed with exception: {e}')

    @staticmethod
    def _get_config(remote_data_center_source, curl):
        """Get the `dm_archive_root` and remote data source configuration from the JAMO server.

        :param str remote_data_center_source: Data center source name (e.g., igb, dori)
        :param Curl curl: Curl to make the configuration request
        """
        config = curl.get('api/core/settings/tape')
        if 'remote_sources' not in config:
            raise RemoteSourceConfigurationException('Config missing `remote_sources` configuration')
        if remote_data_center_source not in config.get('remote_sources'):
            raise RemoteSourceConfigurationException(
                f'Remote data source configuration for {remote_data_center_source} not found')
        remote_config = config.get('remote_sources').get(remote_data_center_source)
        for key in ('rsync_uri', 'rsync_password', 'dm_archive_root_source'):
            if key not in remote_config:
                raise RemoteSourceConfigurationException(
                    f'Missing remote configuration value {key} for {remote_data_center_source}')
        return config.get('dm_archive_root'), remote_config

    @staticmethod
    def _rsync(source, destination, rsync_uri, rsync_password):
        """Rsync data from source to destination.

        :param str source: Path to source
        :param str destination: Path to destination
        :param str rsync_uri: URI to rsync
        :param str rsync_password: Password to rsync
        """
        # To honor the process's `umask` setting, we need to set the `--chmod=ugo=rwX` flag so that all non-masked bits
        # in the source file get enabled (otherwise permission bits are set to the source file's permissions). See
        # `rsync` man page for more information.
        results = subprocess.run(
            ['rsync', '-rtL', '--chmod=ugo=rwX', '--no-h', '--stats', f'{rsync_uri}/{source}', destination],
            env={'RSYNC_PASSWORD': rsync_password}, stdout=subprocess.PIPE, check=True)
        ret = {}
        if results.stdout:
            for line in results.stdout.decode('utf-8').split('\n'):
                match = re.search(r'([\w\s]+): (\d+)', line)
                if match:
                    ret[match.group(1).lower().replace(' ', '_')] = int(match.group(2))
        return ret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str, help="The server address of the restful service")
    parser.add_argument('-s', '--source', type=str, help='Data center source name (e.g., igb, dori)')
    parser.add_argument('-j', '--jamo_token_env_var', type=str, help='The environemnt variable to get the JAMO token from, defaults to JAMO_TOKEN', default='JAMO_TOKEN')
    args = parser.parse_args()

    # We are going to look in the environment variable specified by the -j flag for the JAMO token
    # in case we are wrapping this in tmux as all sessions on the same machine share the same environment
    jamo_token = os.environ.get(args.jamo_token_env_var)
    if jamo_token is None:
        print(f'{args.jamo_token_env_var} environment variable not set')
        sys.exit(1)

    if args.source is None:
        print('Data center source name not specified')
        sys.exit(1)

    os.umask(0o022)

    curl = Curl(args.host, appToken=jamo_token, errorsToRetry=[524])
    egress_handler = EgressHandler(curl, args.source)
    egress_handler.run()


if __name__ == '__main__':
    main()
