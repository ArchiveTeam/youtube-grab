# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os
import random
import re
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
import threading
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

class HigherVersion:
    def __init__(self, expression, min_version):
        self._expression = re.compile(expression)
        self._min_version = min_version

    def search(self, text):
        for result in self._expression.findall(text):
            if result >= self._min_version:
                print('Found version {}.'.format(result))
                return True

WGET_AT = find_executable(
    'Wget+AT',
    HigherVersion(
        r'(GNU Wget 1\.[0-9]{2}\.[0-9]{1}-at\.[0-9]{8}\.[0-9]{2})[^0-9a-zA-Z\.-_]',
        'GNU Wget 1.21.3-at.20260319.01'
    ),
    [
        './wget-at',
        '/home/warrior/data/wget-at-nss'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20260717.02'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0'
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

            command = [
                WGET_AT,
                '--host-lookups', 'dns',
                '--hosts-file', '/dev/null',
                '--resolvconf-file', '/dev/null',
                '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
                '--no-hsts',
                '--output-document', '-',
                '--max-redirect', '0',
                '--save-headers'
            ]
            kwargs = {
                'timeout': 60,
                'capture_output': True
            }

            url = 'https://youtube.com/'
            returned = subprocess.run(
                command+[
                    '--max-redirect', '1',
                    url
                ],
                **kwargs
            )
            youtube_ips = set(re.findall(
                br'([0-9]{1,3}(?:\.[0-9]{1,3}){3})',
                returned.stdout + returned.stderr
            ))
            assert youtube_ips, 'No IP addresses found.'
            assert all(ip not in youtube_ips for ip in [
                b'216.239.38.119',
                b'216.239.38.120'
            ]), 'Got restricted IP address in {}.'.format(youtube_ips)
            assert returned.returncode == 0, 'Invalid return code {} on {}.'.format(returned.returncode, url)
            assert re.match(
                b'^HTTP/1\\.1 200 OK\r\n'
                b'Content-Type: text/html; charset=utf-8\r\n',
                returned.stdout
            ), 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))
            assert (
                b'Location: https://www.youtube.com/ [following]\n'
            ) in returned.stderr, 'Bad stderr on {}, got {}.'.format(url, repr(returned.stderr))
            for b in (
                b'<title>YouTube',
                b'ytInitialData',
                b'INNERTUBE_API_KEY',
                b'ytcfg'
            ):
                assert b in returned.stdout, 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))

            url = 'https://legacy-api.arpa.li/now'
            returned = subprocess.run(
                command+[url],
                **kwargs
            )
            assert returned.returncode == 0, 'Invalid return code {} on {}.'.format(returned.returncode, url)
            assert re.match(
                b'^HTTP/1\\.1 200 OK\r\n'
                b'Server: openresty\r\n'
                b'Date: [A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} 202[0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} GMT\r\n'
                b'Content-Type: text/plain\r\n'
                b'Connection: keep-alive\r\n'
                b'Content-Length: 1[0-9]\r\n'
                b'Cache-Control: no-store\r\n'
                b'\r\n'
                b'[0-9]{10}\\.[0-9]{1,3}$',
                returned.stdout
            ), 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))

            actual_time = float(returned.stdout.rsplit(b'\n', 1)[1])
            local_time = time.time()
            max_diff = 180
            diff = abs(actual_time-local_time)
            assert diff < max_diff, 'Your time {} is more than {} seconds off of {}.'.format(local_time, max_diff, actual_time)

            for url in (
                'http://domain.invalid/',
                'http://example.test/',
                'http://www/',
                'http://example.test/example',
                'http://nxdomain.archiveteam.org/'
            ):
                returned = subprocess.run(
                    command+[url],
                    **kwargs
                )
                assert len(returned.stdout) == 0, 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))
                assert (
                    b'failed: No IPv4/IPv6 addresses for host.\n'
                    + bytes(WGET_AT.split('/')[-1], 'utf8')
                    + b': unable to resolve host address'
                ) in returned.stderr, 'Bad stderr on {}, got {}.'.format(url, repr(returned.stderr))
                assert returned.returncode == 4, 'Invalid return code {} on {}.'.format(returned.returncode, url)

            url = 'https://on.quad9.net/'
            returned = subprocess.run(
                command+[url],
                **kwargs
            )
            assert returned.returncode == 0, 'Invalid return code {} on {}.'.format(returned.returncode, url)
            assert re.match(
                b'^HTTP/1\\.1 200 OK\r\n'
                b'Server: nginx/1\\.22\\.1\r\n'
                b'Date: [A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} 202[0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} GMT\r\n'
                b'Content-Type: text/html\r\n'
                b'Content-Length: [23][0-9]{3}\r\n'
                b'Last-Modified: [A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} 202[0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} GMT\r\n'
                b'Connection: keep-alive\r\n'
                b'Keep-Alive: timeout=5\r\n'
                b'ETag: "[^"]+"\r\n'
                b'Strict-Transport-Security: max-age=63072000; includeSubdomains; preload\r\n'
                b'Accept-Ranges: bytes\r\n'
                b'\r\n',
                returned.stdout
            ), 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))
            for b in (
                b'<title>Yes, you ARE using quad9. | Quad9</title>',
                b'<h1 id="banner">YES</h1>',
                b'data-result="yes"'
            ):
                assert b in returned.stdout, 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 100
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
        items_lower = [
            (
                ':'.join(s.split(':', 2)[:2])
                if re.match(r'^v[0-9]?:', s)
                else s
            ).lower()
            for s in items
        ]
        with open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'r') as f:
            for aborted_item in f:
                aborted_item = aborted_item.strip().lower()
                index = items_lower.index(aborted_item)
                item.log_output('Item {} is aborted.'.format(aborted_item))
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join(items)


def get_concurrency():
    if '--concurrent' in sys.argv:
        concurrency = int(sys.argv[sys.argv.index('--concurrent')+1])
    else:
        concurrency = os.getenv('CONCURRENT_ITEMS')
        if concurrency is None:
            concurrency = 2
        else:
            concurrency = int(concurrency)
    return concurrency


class SetCookies(SimpleTask):
    LOCK = threading.Lock()
    COOKIE_INDEX = 0
    COOKIE_FILES = []
    COUNTRY = None

    def __init__(self):
        SimpleTask.__init__(self, 'SetCookies')

    def process(self, item):
        cookies_dir = os.path.join(item['data_dir'].rsplit('/', 1)[0], 'cookies')
        if not os.path.isdir(cookies_dir):
            os.makedirs(cookies_dir)
        with self.LOCK:
            item['cookie_file'] = self.get_cookie(cookies_dir)
            if not os.path.isfile(item['cookie_file']) or SetCookies.COUNTRY is None:
                prefer_family = []
                if 'PREFER_IPV4' in os.environ:
                    prefer_family = ['--prefer-family', 'IPv4']
                elif 'PREFER_IPV6' in os.environ:
                    prefer_family = ['--prefer-family', 'IPv6']
                returned = subprocess.run(
                    [
                        WGET_AT,
                        '--host-lookups', 'dns',
                        '--hosts-file', '/dev/null',
                        '--resolvconf-file', '/dev/null',
                        '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
                        '--no-hsts',
                        '--reject-reserved-subnets',
                        '--output-document', '-',
                        '--timeout', '30',
                        '--tries', '1',
                        '--save-cookies', item['cookie_file'],
                        '--keep-session-cookies',
                        '--impersonate', 'firefox148-h1',
                        '--header', 'Accept-Encoding: identity',
                        *prefer_family,
                        'https://www.youtube.com/'
                    ],
                    timeout=60,
                    capture_output=True
                )
                if returned.returncode != 0 and os.path.isfile(item['cookie_file']):
                    os.remove(item['cookie_file'])
                assert returned.returncode == 0, \
                    'Invalid return code {} while preparing cookies: {}.'.format(returned.returncode, returned.stderr)
                match = re.search(
                    br'"countryCode"\s*:\s*"([A-Z]{2})"',
                    returned.stdout
                )
                assert match, 'Could not find country code.'
                SetCookies.COUNTRY = str(match.group(1), 'utf-8')
                item.log_output('Detected country {}.'.format(SetCookies.COUNTRY))
                match = re.search(
                    br'"saveConsentAction"\s*:\s*\{[^\{\}]*"socsCookie"\s*:\s*"([^"]+)"',
                    returned.stdout
                )
                if match:
                    with open(item['cookie_file'], 'a') as f:
                        f.write('.youtube.com\tTRUE\t/\tTRUE\t{}\tSOCS\t{}\n'.format(
                            int(time.time()) + 400 * 24 * 60 * 60,
                            match.group(1).decode('utf-8')
                        ))
                time.sleep(random.randint(20, 30))
            item['country'] = SetCookies.COUNTRY

    @classmethod
    def get_cookie(cls, cookies_dir):
        new_cookie = lambda: ''.join(random.choices(string.ascii_letters, k=8)) + '.txt'
        concurrency = get_concurrency()
        while len(cls.COOKIE_FILES) < concurrency:
            cls.COOKIE_FILES.append(new_cookie())
        while True:
            cookie_index = cls.COOKIE_INDEX % concurrency
            result = os.path.join(cookies_dir, cls.COOKIE_FILES[cookie_index])
            cls.COOKIE_INDEX += 1
            if os.path.isfile(result + '.bad'):
                os.remove(result + '.bad')
                os.remove(result)
                cls.COOKIE_FILES[cookie_index] = new_cookie()
            else:
                return result


def item_filter(items):
    result = []
    for item in items:
        item_name = item['item']
        accept_item = True
        if item_name.count(':') == 2 and re.match(r'^v[0-9]?:', item_name):
            if SetCookies.COUNTRY is None:
                raise ValueError('Country is not set.')
            accept_item = SetCookies.COUNTRY in item_name.rsplit(':', 1)[-1].split(',')
        result.append(accept_item)
    return result


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
            #'-U', USER_AGENT,
            '-nv',
            '--host-lookups', 'dns',
            '--hosts-file', '/dev/null',
            '--resolvconf-file', '/dev/null',
            '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
            '--reject-reserved-subnets',
            '--content-on-error',
            '--lua-script', 'youtube.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
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
            #'--warc-tempdir', ItemInterpolation('%(item_dir)s'),
            #'--header', 'Accept-Language: en-US;q=0.9, en;q=0.8',
            '--load-cookies', ItemValue('cookie_file'),
            '--save-cookies', ItemValue('cookie_file'),
            '--keep-session-cookies',
            '--impersonate', 'firefox148-h1',
            '--header', 'Accept-Encoding: identity'
        ]

        if 'PREFER_IPV4' in os.environ:
            wget_args.extend(['--prefer-family', 'IPv4'])
        elif 'PREFER_IPV6' in os.environ:
            wget_args.extend(['--prefer-family', 'IPv6'])

        v_items = [[], []]
        item['item_has_countries'] = 'false'

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            if re.match(r'^v[0-9]?:', item_name):
                if item_name.count(':') == 2:
                    item['item_has_countries'] = 'true'
                    item_name = item_name.rsplit(':', 1)[0]
            wget_args.append('item-name://'+item_name)
            item_type, item_value = item_name.split(':', 1)
            if item_type in ('v', 'v1', 'v2'):
                wget_args.extend(['--warc-header', 'youtube-video: '+item_value])
                wget_args.append('https://www.youtube.com/watch?v='+item_value)
                if item_type == 'v1':
                    v_items[0].append(item_value)
                elif item_type == 'v2':
                    v_items[1].append(item_value)
            elif item_type == 'post':
                wget_args.extend(['--warc-header', 'youtube-post: '+item_value])
                wget_args.append('https://www.youtube.com/post/'+item_value)
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
    <h2>youtube.com <span class="links"><a href="https://youtube.com/">Website</a> &middot; <a href="https://tracker.archiveteam.org/youtube/">Leaderboard</a></span></h2>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    SetCookies(),
    GetItemFromTracker('https://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION, item_filter=item_filter),
    PrepareDirectories(warc_prefix='youtube'),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'warc_file_base': ItemValue('warc_file_base'),
            'v1_items': ItemValue('v1_items'),
            'v2_items': ItemValue('v2_items'),
            'cookie_file': ItemValue('cookie_file'),
            'country': ItemValue('country'),
            'item_has_countries': ItemValue('item_has_countries')
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
            'https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
