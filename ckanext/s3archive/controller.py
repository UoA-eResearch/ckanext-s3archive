import logging

import paste.fileapp
import mimetypes
import os
from pylons import config
import boto.s3.connection as s3connection
from boto.s3.connection import OrdinaryCallingFormat

#from ckan.common import c, response, request, _
import ckan.model as model
import ckan.lib.uploader as uploader

log = logging.getLogger(__name__)

from ckan.controllers.package import PackageController
import ckan.plugins.toolkit as toolkit

NotFound = toolkit.ObjectNotFound
NotAuthorized = toolkit.NotAuthorized
get_action = toolkit.get_action
c = toolkit.c

class S3Downloader(PackageController):

    def resource_download(self, id, resource_id, filename=None):
        """
        Provides a direct download by either redirecting the user to the url stored
         or downloading an uploaded file directly.
        """
        context = {'model': model, 'session': model.Session,
                   'user': c.user or c.author, 'auth_user_obj': c.userobj}

        try:
            rsc = get_action('resource_show')(context, {'id': resource_id})
            pkg = get_action('package_show')(context, {'id': id})
        except NotFound:
            toolkit.abort(404, toolkit._('Resource not found'))
        except NotAuthorized:
            toolkit.abort(401, toolkit._('Unauthorized to read resource %s') % id)

        if rsc.get('url_type') == 'upload':
            upload = uploader.get_resource_uploader(rsc)
            filepath = upload.get_path(rsc['id'])

            #### s3archive new code
            access_key = config.get('ckanext.s3archive.access_key')
            secret_key = config.get('ckanext.s3archive.secret_key')
            bucket_name = config.get('ckanext.s3archive.bucket')

            if not os.path.exists(filepath):
                content_type, content_enc = mimetypes.guess_type(rsc.get('url',''))
                key_name = filepath[len(filepath)-39:]

                conn = s3connection.S3Connection(access_key, secret_key, calling_format=OrdinaryCallingFormat(), host='object.auckland.ac.nz', port=443)
                bucket = conn.get_bucket(bucket_name)

                key = None
                for key in bucket.list(prefix=key_name.lstrip('/')):
                    pass
                if not key:
                    toolkit.abort(404, toolkit._('Resource data not found'))

                headers = {}
                if content_type:
                    headers['response-content-type'] = content_type
                url = key.generate_url(300, method='GET', response_headers=headers)
                toolkit.redirect_to(url)
            #### code finish

            fileapp = paste.fileapp.FileApp(filepath)
            try:
               status, headers, app_iter = toolkit.request.call_application(fileapp)
            except OSError:
               toolkit.abort(404, toolkit._('Resource data not found'))
            toolkit.response.headers.update(dict(headers))
            content_type, content_enc = mimetypes.guess_type(rsc.get('url',''))
            if content_type:
               toolkit.response.headers['Content-Type'] = content_type
            toolkit.response.status = status
            return app_iter
        elif 'url' not in rsc:
            toolkit.abort(404, toolkit._('No download is available'))
        toolkit.redirect_to(rsc['url'])

