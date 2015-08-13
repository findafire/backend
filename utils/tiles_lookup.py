import urlparse
import datetime
import urllib2
import re
from lxml import etree
from io import StringIO


def read_url(url):
    connection = None
    try:
        connection = urllib2.urlopen(url)
        return unicode(connection.read(), 'utf8')
    except:
        raise Exception("Can't process url %s properly." % url, )
    finally:
        if connection:
            connection.close()


def get_date(date_str):
    m = re.search('(\d+)\.(\d+)\.(\d+)', date_str)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None


def get_tiles_directories_urls(base_directory_url, date_filter=lambda _: True):
    parser = etree.HTMLParser()
    parsed_directory = etree.parse(StringIO(read_url(base_directory_url)), parser)
    tiles_directories = {}
    for a in parsed_directory.getiterator(tag='a'):
        date = get_date(a.text.rstrip('/'))
        if date and date_filter(date):
            tiles_directories[date] = urlparse.urljoin(base_directory_url, a.get('href'))
    return tiles_directories


def get_tiles_files_urls(tiles_directory_url, tile_suffix='hdf'):
    parser = etree.HTMLParser()
    parsed_directory = etree.parse(StringIO(read_url(tiles_directory_url)), parser)
    tiles_files = {}
    for a in parsed_directory.getiterator(tag='a'):
        if a.text.endswith('.' + tile_suffix):
            tiles_files[a.text] = urlparse.urljoin(tiles_directory_url, a.get('href'))
    return tiles_files
