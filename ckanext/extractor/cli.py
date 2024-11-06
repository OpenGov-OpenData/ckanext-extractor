# encoding: utf-8

import click
import ckan.model as model
import ckan.plugins.toolkit as tk
import logging

import ckanext.extractor.model as extractor_model

log = logging.getLogger(__name__)


def get_commands():
    return [
        extractor
    ]

def _get_ids(ids, only_with_metadata=False):
    """
    Get list of resource IDs from command line arguments.

    Returns the specific IDs listed or all IDs if ``all`` was passed.

    If ``only_with_metadata`` is true and ``all`` was passed then only
    IDs of resources which have metadata are returned.
    """

    if len(ids) < 1:
        tk.error_shout('Missing argument. Specify one or more resource IDs '
                + 'or "all".')
    if len(ids) == 1 and ids[0].lower() == 'all':
        if only_with_metadata:
            return sorted(tk.get_action('extractor_list')({}, {}))
        else:
            return sorted(r.id for r in model.Resource.active())
    else:
        return ids[:]


def _compress(s, n=50):
    s = str(s)
    if len(s) < n:
        return s
    else:
        return s[:n/2] + ' ... ' + s[-n/2:]


@click.group(short_help="Extractor CLI Commands")
def extractor():
    pass;

@extractor.command()
def delete():
    log.info("Deleting extractions")
    for id in _get_ids(True):
        print(id)
        tk.get_action('extractor_delete',  {'id': id})


@extractor.command()
@click.option(u'-f', u'--force', help=u'Force extraction', is_flag=True)
@click.option(u'-a', u'--all', help=u'Extract all', is_flag=True)
@click.argument(u'resource_ids', required=False, nargs=-1)
@click.pass_context
def extract(ctx,force, all, resource_ids):
    log.info("Extraction started ...")
    ids = []
    if all:
        ids.append('all')
    else:
        for id in resource_ids:
            ids.append(id)
    site_user = tk.get_action(u'get_site_user')({u'ignore_auth': True}, {})
    context = {u'user': site_user[u'name'], u'ignore_auth': True}
    extract = tk.get_action('extractor_extract')
    for id in _get_ids(ids):
        print(id + ': ', end='')
        result = extract(context, {'id': id, 'force': force})
        status = result['status']
        if result['task_id']:
            status += ' (task {})'.format(result['task_id'])
        print(status)


@extractor.command()
def init():
    log.info("Creating tables")
    extractor_model.create_tables()


@extractor.command()
def list():
    result = tk.get_action('extractor_list')({}, {})
    print('\n'.join(sorted(result)))


@extractor.command()
@click.argument(u'extract_id', required=False, nargs=-1)
def show(extract_id):
    show = tk.get_action('extractor_show')
    ids =_get_ids(extract_id)
    for i, id in enumerate(ids):
        try:
            result = show({}, {'id': id})
        except tk.NotFound as e:
            tk.error_shout(e)
            continue
        print('{}:'.format(id))
        for key in sorted(result):
            if key in ('resource_id', 'meta'):
                continue
            print('  {}: {!r}'.format(key, result[key]))
        print('  meta:')
        meta = result['meta']
        for key in sorted(meta):
            print('    {}: {!r}'.format(key, _compress(meta[key])))
        if i < len(ids) - 1:
            print('')
