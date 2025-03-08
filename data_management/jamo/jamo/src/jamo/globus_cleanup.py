import argparse
import os
import re
import subprocess
import yaml
import sdm_logger
import platform
from sdm_curl import Curl


class BackupServiceConfigurationException(Exception):
    """Exception to be raised if backup service configuration is missing required fields.
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class GlobusCleanup:

    def __init__(self, curl, backup_service_name):
        sdm_logger.config(f'globus_cleanup-{platform.node()}.log', emailTo='jgi-dm@lbl.gov', curl=curl)
        self.logger = sdm_logger.getLogger('globus_cleanup')
        self.backup_service_name = backup_service_name
        self.sdm_curl = curl
        self.config = self._get_config(self.backup_service_name, self.sdm_curl)

    def run(self):
        """Recursively check files in the source path and if they are found remotely on Globus, delete the source file.
        """
        dir_to_files = {}
        directory = self.config.get('source_path')
        destination_endpoint = self.config.get('destination_endpoint')

        for root, dirs, files in os.walk(directory):
            if len(files) == 0:
                continue
            relative_dir = re.sub(f'^{directory}/?', '', root)
            if relative_dir not in dir_to_files:
                dir_to_files[relative_dir] = []
            dir_to_files.get(relative_dir).extend(files)

        for directory_local, files_local in dir_to_files.items():
            globus_ls_cmd = ['jgi_globus_timer.sh', 'ls', '--level', '0', destination_endpoint, directory_local]
            try:
                result = subprocess.run(globus_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                files_remote = set(result.stdout.decode('utf-8').split())
                for file_local in files_local:
                    path = os.path.join(directory_local, file_local)
                    if path in files_remote:
                        os.remove(os.path.join(directory, path))
            except Exception as e:
                if not isinstance(e, subprocess.CalledProcessError) or 'ClientError.NotFound' not in e.stderr.decode(
                        'utf-8'):
                    # Error is not due to directory not being found remotely (which may not have yet been synced), so
                    # reraise the error
                    self.logger.warning(f'Error cleaning up Globus files: {e}')
                    raise

    @staticmethod
    def _get_config(backup_service_name, curl):
        """Get the backup service configuration for the given `backup_service_name` from the JAMO server.

        :param str backup_service_name: Name of the backup service (e.g., emsl)
        :param Curl curl: Curl to make the configuration request
        """
        config = curl.get('api/core/settings/tape')
        if 'backup_services' not in config:
            raise BackupServiceConfigurationException('Config missing `backup_services` configuration')
        if backup_service_name not in config.get('backup_services'):
            raise BackupServiceConfigurationException(
                f'Backup service configuration for {backup_service_name} not found')
        backup_service_config = config.get('backup_services').get(backup_service_name)
        for key in ('source_path', 'destination_endpoint'):
            if key not in backup_service_config:
                raise BackupServiceConfigurationException(
                    f'Missing backup service configuration value `{key}` for `{backup_service_name}`')
        return backup_service_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, help='Path to configuration file')
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        backup_service_name = config.get('backup_service_name')
        jamo_token = config.get('jamo_token')
        host = config.get('host')
    curl = Curl(host, appToken=jamo_token, errorsToRetry=[524])
    globus_cleanup = GlobusCleanup(curl, backup_service_name)
    globus_cleanup.run()


if __name__ == '__main__':
    main()
