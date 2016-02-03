# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import getpass
import re

from pymongo import MongoClient
import pymongo.errors
from pymongo import ReadPreference

import psycopg2

_version = 1

class ContactFilter(object):
    def __init__(self, parent = None):
        self.parent = parent
        return

    # re.findall("([^2-90]|\b|^)(\d{3})\W*(\d{3})\W*(\d{4})([^\d]|$)", "157145192411")
    def process(self, message):
        # contact info may be in the email or the url
        phonestr = "1?(\d\s*\d\s*\d)\W*(\d\s*\d\s*\d)\W*(\d\s*\d\s*\d\s*\d)"
        phones = re.findall(phonestr, message.source)
        phones.extend(re.findall(phonestr, message.url))
        emails = re.findall("[\w._-]+\@[\w_-]+\.\w+", message.source)
        emails.extend(re.findall("[\w._-]+\@[\w_-]+\.\w+", message.url))
        today = datetime.datetime.now()
        res = {
                "_id": message.url,
                "v": _version,
                "source": message.source,
                "contact": {
                    # store only unique emails and phones
                    "emails": list(set(emails)),
                    "phones": list(set(["-".join(x) for x in phones]))
                    },
                "dateRange": {
                    "first": today,
                    "last": today,
                }
                }
        if self.parent:
            res = self.parent.process(message)
        contact = {}
        contact["emails"] = emails
        contact["phones"] = phones
        res["contact"] = contact
        return res

class BackPageUrlParser(object):
    def __init__(self, parent = None):
        self.parent = parent
        return

    def process(self, message):
        parsed = re.findall("http://(\w+)\.(\w+)\.com/", message.url)
        today = datetime.datetime.now()
        res = {
                "_id": message.url,
                "v": _version,
                "source": message.source,
                "dateRange": {
                    "first": today,
                    "last": today,
                }
                }
        if self.parent:
            res = self.parent.process(message)
        location = {}
        location["area"] = "" if len(parsed) < 1 else parsed[0][0]
        location["site"] = "" if len(parsed) < 1 else parsed[0][1]
        res["siteInfo"] = location
        return res

class PostgreSQLDump(object):
    def __init__(self, host, dbname,
            processor = None, user = None, pwd = None):
        self.db = dbname
        self.host = host
        if user:
            self.user = user
        else:
            self.user = raw_input("PostgreSQL User: ")
        if pwd:
            self.pwd = pwd
        else:
            self.pwd = getpass.getpass("PostgreSQL Password: ")

        self.conn = psycopg2.connect(
                "dbname='{}' user='{}' host='{}' password='{}'".format(
                    self.db, self.user, self.host, self.pwd
                    )
                )
        self.processor = processor

    def __repr__(self):
        return "PostgreSQLDump({}, {})".format(host, db)

    def set_insert_table(self, table):
        return """INSERT INTO {}(%s) VALUES (%s);""".format(table)

    def run_cmd(self, cmd):
        cur = self.conn.cursor()
        cur.execute(cmd)
        return cur

    def dump(self, message):
        try:
            href = message.url
            href = href.replace("'", "")
            cur = self.find_by_id(href)
            if not cur:
                source = message.source.replace("\\n", "\n")
                source = source.replace("\\r", "")
                source = source.replace("&nbsp", " ")
                insstr = self.set_insert_table("page") % (
                        "Url, Content, DateScraped, CrawlerId",
                        "'{}', '{}', NOW(), {}".format(
                            href,
                            source.replace("\'", "\""),
                            _version)
                        )
                cur = self.conn.cursor()
                cur.execute(insstr)
                cur.close()
                self.conn.commit()
        except psycopg2.IntegrityError:
            # Another thread beat us to the insert
            pass
        except psycopg2.InternalError:
            self.conn = psycopg2.connect(
                    "dbname='{}' user='{}' host='{}' password='{}'".format(
                        self.db, self.user, self.host, self.pwd
                        )
                    )

    def find_by_id(self, _id, attempt = 0):
        try:
            cur = self.conn.cursor()
            cur.execute(
            """SELECT id FROM page WHERE url = '{}';""".format(_id)
                )
            return cur.fetchone() is not None
        except (psycopg2.IntegrityError, psycopg2.InternalError):
            if attempt < 5:
                return self.find_by_id(_id, attempt + 1)
            return None

class MongoDBDump(object):
    def __init__(self, host, port, dbname, colname,
            processor = ContactFilter(BackPageUrlParser()),
            replset = None, user = None, pwd = None):
        url = "".join([
            "mongodb://",
            host,
            ":",
            port,
            "/"
            ])
        if replset:
            self.conn = MongoClient(url, replicaSet = replset,
                    read_preference = ReadPreference.PRIMARY_PREFERRED)
        else:
            self.conn = MongoClient(url)
        if user and pwd:
            res = self.conn["crawler"].authenticate(user, pwd)
            #TODO: add err handling for else

        self.col = self.conn[dbname][colname]
        self.processor = processor

    def find_by_id(self, _id):
        return self.col.find({"_id": {"$eq": _id}}).limit(1)

    def dump(self, message):
        try:
            curr = self.col.find({"_id": {"$eq": message.url}}).limit(1)
            # document already exists
            if curr.count() > 0:
                # update the last day the page was indexed
                cur = self.col.update_one(
                        { "_id": message.url },
                        { "$set": { "dateRange.last": datetime.datetime.now() } },
                        upsert = True
                        )
            # if we don't already have the website insert the website
            else:
                cur = self.col.insert_one(self.processor.process(message))
        except pymongo.errors.DuplicateKeyError:
            pass
