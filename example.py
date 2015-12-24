# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import threading
from pymongo import MongoClient
import time

from src.core import engine
from src.core import crawler
from src.core import datahandler
from palantiri import util

areas = [
        "albanyga",
        "athensga",
        "atlanta",
        "augusta",
        "brunswick",
        "columbusga",
        "macon",
        "nwga",
        "savannah",
        "statesboro",
        "valdosta",
        "birmingham",
        "nashville",
        "panamacity",
        "myrtlebeach",
        "memphis",
        "miami",
        "tampa"
        ]

sites = [
        "FemaleEscorts",
        "BodyRubs",
        "Strippers",
        "Domination",
        "TranssexualEscorts",
        "MaleEscorts",
        "Datelines",
        "AdultJobs",
        ]

data_handler = datahandler.MongoDBDump("127.0.0.1", "27017", "crawler",
        "search")

eng = engine.TorEngine()
sighandle = util.ShutdownHandler(2)

def first_finished(threads):
    for i in range(0, len(threads)):
        if not threads[i].isAlive():
            return i
    return None

for area in areas:
    threads = []
    for site in sites:
        if len(threads) > 4:
            idx = first_finished(threads)
            if idx:
                del threads[idx]
            else:
                time.sleep(1)
                continue

        master = crawler.BackpageCrawler(site, [], data_handler, area,
                                         eng, 1, 4, sighandle)
        threads.append(master)
        master.start()

    for t in threads:
        t.join()
