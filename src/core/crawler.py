# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import logging
import random
import re
import threading
import time
import urllib.parse
from bs4 import BeautifulSoup
import pymongo.errors

from . import errors
from . import common
from . import engine

# Technically I've read that many operations are thread-safe on Python's
# list implementation, so this may not be necessary, but I think I'd rather
# err on the side of caution at least for now
class SharedList(object):
    def __init__(self, lst):
        self.mutex = threading.Lock()
        self.lst = lst
        return

    def __contains__(self, val):
        return val in self.lst

    def __iter__(self):
        return self.lst.__iter__()

    def pop(self):
        self.mutex.acquire()
        try:
            val = self.lst.pop()
            self.mutex.release()
            return val
        except:
            if self.mutex.locked():
                self.mutex.release()
            return None

    def append(self, val):
        self.mutex.acquire()
        try:
            self.lst.append(val)
            self.mutex.release()
            return True
        except:
            if self.mutex.locked():
                self.mutex.release()
            return None

    def __len__(self):
        return len(self.lst)

    def extend(self, lst):
        self.mutex.acquire()
        try:
            self.lst.extend(lst)
            self.mutex.release()
            return True
        except:
            if self.mutex.locked():
                self.mutex.release()
            return True

class EngineWrapper(threading.Thread):
    def __init__(self, parent, group = None, name = None,
            args = (), kwargs = None):
        super(EngineWrapper, self).__init__(group = group, name = name,
                args = args, kwargs = kwargs)
        self.parent = parent
        self.eng = parent.eng.clone()
        self.to_visit = parent.to_visit
        self.stop = parent.stop
        self.delay = parent.delay

    def run(self):
        while self.to_visit or not self.stop.is_set():
            # There are more sites to visit
            if self.to_visit:
                url = self.to_visit.pop()
                site = self.eng.get_page_source(url)
                if url and site:
                    try:
                        self.parent.notify(site)
                    # give the dbs a sec to catch up
                    except (pymongo.errors.AutoReconnect, pymongo.errors.NotMasterError):
                        time.sleep(self.delay)
            # The parent needs more time to generate more sites.
            # Wait the set delay
                time.sleep(self.delay)
            else:
                time.sleep(self.delay)
        return

class SearchCrawler(threading.Thread):
    def __init__(self, kwds = [], dbhandler = None, eng = engine.DefaultEngine(),
            max_threads = 10, delay = 1, group = None, name = None,
            args = (), kwargs = None):
        super(SearchCrawler, self).__init__(group = group, name = name,
                args = args, kwargs = kwargs)
        self.max_threads = max_threads
        self.eng = eng
        self.dbhandler = dbhandler
        self.stop = threading.Event()
        self.to_visit = SharedList([])
        self.delay = delay
        self.kwds = kwds
        self.children = []
        return

    def next_page(self, soup):
        raise MasterError("next_page has not been implemented for this class")

    def get_listings(self, soup):
        raise MasterError("get_listings has not been implemented for this class")

    def notify(self, message):
        if isinstance(message, common.Website):
            logging.info("Dumping %s" % str(message.url))
            threading.Thread(target=self.dbhandler.dump(message))
            return True
        else:
            return False

    def start_threads(self):
        for x in range(0, self.max_threads):
            t = EngineWrapper(self)
            self.children.append(t)
            t.start()
        logging.info("Started %d threads" % self.max_threads)

    def run(self):
        raise MasterError("get_listings has not been implemented for this class")

class BackpageCrawler(SearchCrawler):
    def __init__(self, site, kwds = [], dbhandler = None, area = "atlanta",
            eng = engine.DefaultEngine(), max_threads = 10, delay = 1):
        self.baseurl = "".join(["http://", area, ".backpage.com/", site, "/"])
        if kwds:
            keywords = " ".join(kwds)
            self.url = "?".join([self.baseurl, keywords])
        else:
            self.url = self.baseurl
        super(BackpageCrawler, self).__init__(kwds, dbhandler, eng, max_threads, delay)

    def next_page(self, soup):
        links = soup.find_all("a", href=True)
        for link in links:
            innerHTML = link.decode_contents(formatter = "html")
            if innerHTML == "Next":
                return link["href"]
        return None

    def get_listings(self, soup):
        links = soup.find_all("a", href=True)
        valid = []
        for link in links:
            # remove some non-ad links
            if link.has_attr("class"):
                continue

            href = str(urllib.parse.urljoin(self.baseurl, link["href"]))
            # remove urls that are not on the same site
            if not re.search(self.baseurl, href):
                continue

            try:
                cur = self.dbhandler.find_by_id(href).limit(1)
                if not href in self.to_visit and not cur.count():
                    valid.append(href)
            except (pymongo.errors.AutoReconnect, pymongo.errors.NotMasterError):
                # try again
                self.get_listings(soup)

        self.to_visit.extend(valid)
        return

    def run(self):
        self.start_threads()
        time.sleep(self.delay)
        url = self.url

        while url:
            site = self.eng.get_page_source(url)
            if site:
                soup = BeautifulSoup(site.source, "lxml")
                self.get_listings(soup)
                url = self.next_page(soup)
            else:
                url = None

        self.stop.set()
        for t in self.children:
            t.join()

class BackpageContinuousCrawler(BackpageCrawler):

    """Continously running version of BackpageCrawler class"""

    def __init__(self, site, kwds = None, dbhandler = None, area =
                 "atlanta", eng = engine.DefaultEngine(), max_threads = 2,
                 delay = 5):
        """TODO: to be defined1.

        :site: TODO
        :kwds: TODO
        :dbhandler: TODO
        :area: TODO
        :eng: TODO

        """
        BackpageCrawler.__init__(self, site, kwds, dbhandler, area, eng, max_threads,
                                 delay)
        self._avg_delay = delay

    @property
    def delay(self):
        return 0.5 * self._avg_delay + random.random() * self._avg_delay

    @delay.setter
    def delay(self, value):
        self._avg_delay = value

    def get_listings(self, soup):
        links = soup.find_all("a", href=True)
        valid = []
        for link in links:
            # remove some non-ad links
            if link.has_attr("class"):
                continue

            href = str(urllib.parse.urljoin(self.baseurl, link["href"]))
            # remove urls that are not on the same site
            if not re.search(self.baseurl, href):
                continue

            cur = self.dbhandler.find_by_id(href).limit(1)
            if not href in self.to_visit and not cur.count():
                valid.append(href)

        self.to_visit.extend(valid)
        return valid

    def run(self):
        self.start_threads()
        time.sleep(self.delay)
        url = self.url
        new_listing_cnt = 0

        # TODO: These should be configurable, not hard coded
        self.max_retry = 3
        retry = 0

        old_listing_cnt = -1
        while url and new_listing_cnt != old_listing_cnt:
            site = self.eng.get_page_source(url)
            if site:
                soup = BeautifulSoup(site.source, "lxml")
                valid_listings = self.get_listings(soup)
                new_listing_cnt += len(valid_listings)

                url = self.next_page(soup)
            else:
                if old_listing_cnt == new_listing_cnt:
                    retry += 1
                else:
                    retry = 0

                old_listing_cnt = new_listing_cnt
                if retry <= self.max_retry:
                    url = self.url
                    retry_delay = 10 * self.delay
                    logging.info("Waiting %d seconds to retry" % retry_delay)
                    time.sleep(retry_delay)
                else:
                    logging.info("Tried %d times without new results" % retry)
                    url = None

        self.stop.set()
        for t in self.children:
            t.join()

