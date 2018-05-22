#!/usr/bin/env python
"""Export data from helpscout to files (and S3)"""

import base64
import json
import os.path
import sqlite3
import sys
import time
import requests
import boto3

HELPSCOUT_API_KEY = os.environ['HELPSCOUT_API_KEY']
MAILBOX_LIST = [8430] # GeekOUT # List of mailboxes to process

DB_FILE = 'data.db'
S3_BUCKET_NAME = 'helpscout-export'

# Configure behaviour
FETCH = False # Fetch conversation lists from mailboxes and save in state database (work queue)
PROCESS = True # Process conversations listed in state database
STORE_JSON = True

def db_create_schema(conn):
    """Create database schema"""
    conn.cursor().execute('''CREATE TABLE conversations (c_id integer primary key,
                          mailbox_name text,
                          done integer)''')
    conn.commit()

def db_add_conversation(conn, val):
    """Add a new conversation to work queue"""
    sql = ("INSERT INTO conversations (c_id, mailbox_name, done) VALUES (?, ?, 0)")
    conn.cursor().execute(sql, val)
    conn.commit()

def db_mark_conversation_as_done(conn, cid):
    """Mark conversation as done"""
    sql = ("update conversations set done=1 where c_id=?")
    conn.cursor().execute(sql, (cid,))
    conn.commit()

def db_fetch_conversation(conn):
    """Fetch conversation from queue"""
    sql = ("select mailbox_name, c_id  from conversations where done=0 limit 1")
    for row in conn.cursor().execute(sql):
        yield row

def hs_list_mailboxes():
    """Fetch mailbox list from Helpscout"""
    time.sleep(1) # throttle API requests
    response = requests.get("https://api.helpscout.net/v1/mailboxes.json",
                            auth=(HELPSCOUT_API_KEY, 'dummy'))
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["page"] == 1 # lazy..
    assert response_json["pages"] == 1 # lazy.. but works for now

    for i in response_json["items"]:
        yield (i['id'], i['name'])

def hs_list_conversations(mailbox_id):
    """Fetch conversation list from Helpscout mailbox"""
    page = 1
    while True:
        time.sleep(0.4) # throttle API requests
        url = "https://api.helpscout.net/v1/mailboxes/%d/conversations.json?page=%d" % (mailbox_id,
                                                                                        page)
        response = requests.get(url, auth=(HELPSCOUT_API_KEY, 'dummy'))
        assert response.status_code == 200

        response_json = response.json()
        assert response_json["page"] == page
        pages = response_json["pages"]

        print("Page %d of %d" % (page, pages))

        for i in response_json["items"]:
            #           assert i['type'] == 'email', ("type is %s, id" %  i['type'])
            yield i['id']

        if page >= pages:
            break
        else:
            page = page +1

def hs_fetch_attachment_data(attachment_id):
    """Fetch attachment data from Helpscout"""
    time.sleep(1) # throttle API requests
    url = "https://api.helpscout.net/v1/attachments/%d/data.json" % (attachment_id)
    print(url)
    response = requests.get(url, auth=(HELPSCOUT_API_KEY, 'dummy'))

    if response.status_code == 404:
        return False

    assert response.status_code == 200
    response_json = response.json()
    r_item = response_json['item']
    print(r_item['id'])
    return base64.b64decode(r_item['data'])

def hs_process_conversation(conversation_id, store_path):
    """Process single Helpscout conversation"""
    time.sleep(0.4) # throttle API requests
    url = "https://api.helpscout.net/v1/conversations/%d.json" % (conversation_id)
    print(url)
    response = requests.get(url, auth=(HELPSCOUT_API_KEY, 'dummy'))
    response_json = response.json()
    conversation = response_json["item"]
    assert conversation['type'] == 'email'

    if STORE_JSON:
        store_file("%s/conversation.json" % store_path, json.dumps(conversation),
                   "application/json")

    if 'threads' in conversation:
        for thread in conversation['threads']:
            print("Thread id: %d" % thread['id'])
            if STORE_JSON:
                store_file("%s/thread_%d.json" % (store_path, thread['id']), json.dumps(thread),
                           "application/json")

            if ('attachments' in thread) and (thread['attachments'] is not None):
                print("Found attachments")
                for att in thread['attachments']:
                    assert 'fileName' in att

                    a_key = "%s/attachment_%d_%s" % (store_path, att['id'], att['fileName'])
                    a_data = hs_fetch_attachment_data(att['id'])

                    if a_data is not False:
                        store_file(a_key, a_data, att['mimeType'])
                    else:
                        store_file(("%s_%d" % (a_key, 404)), "", att['mimeType'])

                    if STORE_JSON:
                        store_file("%s/attachment_%d.json" % (store_path, att['id']),
                                   json.dumps(att), 'application/json')

        return True # done

def store_file(key, data, content_type):
    """Store file data"""
    # local file
    f_key = "data/%s" % key
    print("Storing to local file: %s" % f_key)
    dirname = os.path.dirname(f_key)
    if not os.path.exists(dirname):
        print("mkdir: %s" % dirname)
        os.makedirs(dirname)

    assert not os.path.exists(f_key) # check for duplicate files
    with open(f_key, 'w') as file:
        file.write(data)

    # S3
    print("Storing to S3: %s" % key)
    bucket = boto3.resource('s3').Bucket(S3_BUCKET_NAME)

    bucket.put_object(
        ACL='public-read',
        ContentType=content_type,
        Key=key,
        Body=data,
    )

#============================================================
# BEGIN

# Create database if not already present
if os.path.isfile(DB_FILE):
    DB = sqlite3.connect(DB_FILE)
else:
    DB = sqlite3.connect(DB_FILE)
    db_create_schema(DB)

# PART 1: Walk through mailboxes, fetch list of conversations and save it in sqlite DB


if FETCH:
    print("Fetching conversation lists")
    for mb_id, mb_name in hs_list_mailboxes():
        if mb_id in MAILBOX_LIST:
            print("[X] Mailbox '%s': %d" % (mb_name, mb_id))
            for c_id in hs_list_conversations(mb_id):
                db_add_conversation(DB, (c_id, mb_name))
        else:
            print("[ ] Mailbox '%s': %d" % (mb_name, mb_id))
            DB.close()
            sys.exit()

# PART 2: Process conversations, also keeping track of state

if PROCESS:
    print("Processing conversations")

    WORK = True
    while WORK:
        WORK = False
        for (mailbox, c_id) in db_fetch_conversation(DB):
            WORK = True
            print("Conversation: %d" % c_id)
            path = "%s/%d" % (mailbox, c_id) # or... (mailbox.replace(" ", "_")
            if hs_process_conversation(c_id, path) is True:
                db_mark_conversation_as_done(DB, c_id)
                print("Done")

    DB.close()
    sys.exit()

# END
