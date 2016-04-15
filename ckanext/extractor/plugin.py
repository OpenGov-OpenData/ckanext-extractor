#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals

import collections
import logging

from ckan import plugins
from ckan.plugins import toolkit
from ckan.logic import get_action

from .logic import action, auth
from . import model


log = logging.getLogger(__name__)


def _is_package(obj):
    """
    Check if a dict describes a package.

    This is a very simple, duck-typing style test that only checks
    whether the dict contains an ``owner_org`` entry.
    """
    return 'owner_org' in obj


class ExtractorPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IConfigurable)

    #
    # IConfigurer
    #

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('fanstatic', 'extractor')

    #
    # IConfigurable
    #

    def configure(self, config):
        model.setup()

    #
    # IPackageController / IResourceController
    #

    def after_create(self, context, obj):
        if _is_package(obj):
            log.debug('A package was created: {}'.format(obj['id']))
        else:
            log.debug('A resource was created: {}'.format(obj['id']))
            get_action('ckanext_extractor_metadata_extract')({}, obj)

    def after_update(self, context, obj):
        if _is_package(obj):
            log.debug('A package was updated: {}'.format(obj['id']))
        else:
            log.debug('A resource was updated: {}'.format(obj['id']))
            get_action('ckanext_extractor_metadata_extract')({}, obj)

    def after_delete(self, context, obj):
        # For IPackageController, obj is a dict, but for IResourceController
        # it's a list of dicts. See https://github.com/ckan/ckan/issues/2949.
        if isinstance(obj, collections.Mapping):
            # Package
            log.debug('A package was deleted: {}'.format(obj['id']))
        else:
            # Resource(s)

            # Perhaps we can handle this automatically using cascading deletes
            # in SQLAlchemy (i.e. when the resource is deleted the data in the
            # metadata table is also deleted). However, we have to make sure
            # that the index is properly updated in that case.
            for resource in obj:
                log.debug('A resource was deleted: {}'.format(resource['id']))
                get_action('ckanext_extractor_metadata_delete')({}, resource)

    def before_index(self, pkg_dict):
        log.debug('Package {} will be indexed'.format(pkg_dict['id']))
        # FIXME: Add metadata
        return pkg_dict

    #
    # IActions
    #

    def get_actions(self):
        return {
            'ckanext_extractor_metadata_delete': action.metadata_delete,
            'ckanext_extractor_metadata_extract': action.metadata_extract,
            'ckanext_extractor_metadata_list': action.metadata_list,
            'ckanext_extractor_metadata_show': action.metadata_show,
        }

    #
    # IAuthFunctions
    #

    def get_auth_functions(self):
        return {
            'ckanext_extractor_metadata_delete': auth.metadata_delete,
            'ckanext_extractor_metadata_extract': auth.metadata_extract,
            'ckanext_extractor_metadata_list': auth.metadata_list,
            'ckanext_extractor_metadata_show': auth.metadata_show,
        }


def task_imports():
    """
    Entry point for Celery task list.
    """
    return ['ckanext.extractor.tasks']
