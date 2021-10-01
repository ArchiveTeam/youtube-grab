# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

WGET_AT = find_executable(
    'Wget+AT',
    ['GNU Wget 1.20.3-at.20211001.01'],
    [
        './wget-at',
        '/home/warrior/data/wget-at'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20211001.01'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
TRACKER_ID = 'youtube'
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 1 # DO NOT CHANGE

###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])

        open('%(item_dir)s/%(warc_file_base)s.warc.gz' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.gz' % item,
              '%(data_dir)s/%(warc_file_base)s.warc.gz' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
              '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)


class SetBadUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetBadUrls')

    def process(self, item):
        item['item_name_original'] = item['item_name']
        items = item['item_name'].split('\0')
        items_lower = [s.lower() for s in items]
        with open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'r') as f:
            for aborted_item in f:
                aborted_item = aborted_item.strip().lower()
                index = items_lower.index(aborted_item)
                item.log_output('Item {} is aborted.'.format(aborted_item))
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join(items)


class MaybeUploadWithTracker(UploadWithTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0 and not KEEP_WARC_ON_ABORT:
            item.log_output('Skipping UploadWithTracker.')
            return self.complete_item(item)
        return super(UploadWithTracker, self).enqueue(item)


class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            item.log_output('Skipping SendDoneToTracker.')
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'youtube.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_AT,
            '-U', USER_AGENT,
            '-nv',
            '--content-on-error',
            '--load-cookies', 'cookies.txt',
            '--lua-script', 'youtube.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--rotate-dns',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '30',
            '--tries', 'inf',
            '--domains', 'youtube.com',
            '--span-hosts',
            '--waitretry', '30',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic',
            '--header', 'Accept-Language: en-US;q=0.9, en;q=0.8'
        ]

        v_items = [[], []]

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://'+item_name)
            item_type, item_value = item_name.split(':', 1)
            if item_type in ('v', 'v1', 'v2'):
                wget_args.extend(['--warc-header', 'youtube-video: '+item_value])
                wget_args.append('https://www.youtube.com/watch?v='+item_value)
                if item_type == 'v1':
                    v_items[0].append(item_value)
                elif item_type == 'v2':
                    v_items[1].append(item_value)
            else:
                raise ValueError('item_type not supported.')

        item['v1_items'] = ';'.join(v_items[0])
        item['v2_items'] = ';'.join(v_items[1])

        item['item_name_newline'] = item['item_name'].replace('\0', '\n')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title = 'YouTube',
    project_html = '''
    <img class="project-logo" alt="logo" src="https://wiki.archiveteam.org/images/4/4d/YouTube_logo_2017.png" height="50px"/>
    <h2>youtube.com <span class="links"><a href="https://youtube.com/">Website</a> &middot; <a href="http://tracker.archiveteam.org/youtube/">Leaderboard</a></span></h2>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('http://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    PrepareDirectories(warc_prefix='youtube'),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'warc_file_base': ItemValue('warc_file_base'),
            'v1_items': ItemValue('v1_items'),
            'v2_items': ItemValue('v2_items')
        }
    ),
    SetBadUrls(),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.gz')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='2',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        MaybeUploadWithTracker(
            'http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--partial',
                '--partial-dir', '.rsync-tmp',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
