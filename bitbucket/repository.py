# -*- coding: utf-8 -*-
import requests

from tempfile import NamedTemporaryFile
from zipfile import ZipFile


URLS = {
    'CREATE_REPO': 'repositories/',
    'GET_REPO': 'repositories/%(username)s/%(repo_slug)s/',
    'UPDATE_REPO': 'repositories/%(username)s/%(repo_slug)s/',
    'DELETE_REPO': 'repositories/%(username)s/%(repo_slug)s/',
    # Get archive
    'GET_ARCHIVE': 'repositories/%(username)s/%(repo_slug)s/%(format)s/master/',
}

CHUNK_SIZE = 512 * 1024  # 512 KB


class ArchiveDownloadException(Exception):
    pass


class Repository(object):
    """ This class provide repository-related methods to Bitbucket objects."""

    def __init__(self, bitbucket):
        self.bitbucket = bitbucket
        self.bitbucket.URLS.update(URLS)

    def _fetch_binary(self, url, destination_file, auth=None, params=None, **kwargs):
        """ Send HTTP request, with given method,
            credentials and data to the given URL,
            and return the success and the result on success.
        """
        resp = requests.get(url, auth=auth, params=params, data=kwargs, stream=True)

        status_class = resp.status_code / 100
        if status_class == 2:        
            for chunk in resp.iter_content(CHUNK_SIZE):
                destination_file.write(chunk)

        elif status_class == 3:
            raise ArchiveDownloadException('Unauthorized access, ' +
                                           'please check your credentials.')
        elif status_class == 4:
            raise ArchiveDownloadException('Service not found')
        elif status_class == 5:
            raise ArchiveDownloadException('Server error.')
        else:
            raise ArchiveDownloadException('Unknown status code: %s' % resp.status_code)

    def url(self, action, **kwargs):
        """ Construct and return the URL for a specific API service. """
        # TODO : should be static method ?
        return self.URLS['BASE'] % self.URLS[action] % kwargs        

    def _get_files_in_dir(self, zipfile, prefix, repo_slug=None, dir='/'):
        repo_slug = repo_slug or self.bitbucket.repo_slug or ''
        dir = dir.lstrip('/')
        url = self.bitbucket.url(
            'GET_ARCHIVE',
            username=self.bitbucket.username,
            repo_slug=repo_slug,
            format='src')
        dir_url = url + dir
        response = self.bitbucket.dispatch('GET', dir_url, auth=self.bitbucket.auth)
        if response[0] and isinstance(response[1], dict):
            repo_tree = response[1]
            url = self.bitbucket.url(
                'GET_ARCHIVE',
                username=self.bitbucket.username,
                repo_slug=repo_slug,
                format='raw')
            # Download all files in dir
            for file in repo_tree['files']:
                file_url = url + '/'.join((file['path'],))

                with NamedTemporaryFile() as zip_entry:
                    self._fetch_binary(file_url, zip_entry, auth=self.bitbucket.auth)
                    zipfile.write(zip_entry.name, prefix + file['path'])

            # recursively download in dirs
            for directory in repo_tree['directories']:
                dir_path = '/'.join((dir, directory))
                self._get_files_in_dir(zipfile, prefix, repo_slug=repo_slug, dir=dir_path)

    def public(self, username=None):
        """ Returns all public repositories from an user.
            If username is not defined, tries to return own public repos.
        """
        username = username or self.bitbucket.username or ''
        url = self.bitbucket.url('GET_USER', username=username)
        response = self.bitbucket.dispatch('GET', url)
        try:
            return (response[0], response[1]['repositories'])
        except TypeError:
            pass
        return response

    def all(self):
        """ Return own repositories."""
        url = self.bitbucket.url('GET_USER', username=self.bitbucket.username)
        response = self.bitbucket.dispatch('GET', url, auth=self.bitbucket.auth)
        try:
            return (response[0], response[1]['repositories'])
        except TypeError:
            pass
        return response

    def get(self, repo_slug=None):
        """ Get a single repository on Bitbucket and return it."""
        repo_slug = repo_slug or self.bitbucket.repo_slug or ''
        url = self.bitbucket.url('GET_REPO', username=self.bitbucket.username, repo_slug=repo_slug)
        return self.bitbucket.dispatch('GET', url, auth=self.bitbucket.auth)

    def create(self, repo_name, scm='git', private=True, **kwargs):
        """ Creates a new repository on own Bitbucket account and return it."""
        url = self.bitbucket.url('CREATE_REPO')
        return self.bitbucket.dispatch('POST', url, auth=self.bitbucket.auth, name=repo_name, scm=scm, is_private=private, **kwargs)

    def update(self, repo_slug=None, **kwargs):
        """ Updates repository on own Bitbucket account and return it."""
        repo_slug = repo_slug or self.bitbucket.repo_slug or ''
        url = self.bitbucket.url('UPDATE_REPO', username=self.bitbucket.username, repo_slug=repo_slug)
        return self.bitbucket.dispatch('PUT', url, auth=self.bitbucket.auth, **kwargs)

    def delete(self, repo_slug=None):
        """ Delete a repository on own Bitbucket account.
            Please use with caution as there is NO confimation and NO undo.
        """
        repo_slug = repo_slug or self.bitbucket.repo_slug or ''
        url = self.bitbucket.url('DELETE_REPO', username=self.bitbucket.username, repo_slug=repo_slug)
        return self.bitbucket.dispatch('DELETE', url, auth=self.bitbucket.auth)

    def archive(self, repo_slug=None, format='zip', prefix=''):
        """ Get one of your repositories and compress it as an archive.
            Return the path of the archive.

            format parameter is curently not supported.
        """
        prefix = '%s'.lstrip('/') % prefix

        with NamedTemporaryFile(delete=False) as archive:
            with ZipFile(archive, 'w') as zip_archive:
                self._get_files_in_dir(zip_archive, prefix, repo_slug=repo_slug, dir='/')
            return (True, archive.name)
        return (False, 'Could not archive your project.')
