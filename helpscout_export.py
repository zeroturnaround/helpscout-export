#!/usr/bin/env python

import base64
import boto3
import datetime
import json
import os.path
import requests
import sqlite3
import sys
import time

API_KEY = "fill me in"
db_file = "data.db"

store_json=True

def db_create(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE conversations (c_id integer primary key, c_number, mailbox_name text, done integer)''')
    conn.commit()

def db_insert_conversation(conn, val):
    c = conn.cursor()
    sql=("INSERT INTO conversations (c_id, c_number, mailbox_name, done) VALUES (?, ?, ?, 0)")
    c.execute(sql, val)
    conn.commit()

def db_mark_conversation_as_done(conn, c_id):
    c = conn.cursor()
    sql=("update conversations set done=1 where c_id=?")
    c.execute(sql, (c_id,))
    conn.commit()

def db_fetch_new_conversation(conn):
    c = conn.cursor()
    sql=("select c_id, mailbox_name from conversations where done=0 limit 1")
    for row in c.execute(sql):
        yield row

#============================================================
def list_mailboxes():
        time.sleep(1) # throttle API requests
        response = requests.get("https://api.helpscout.net/v1/mailboxes.json", auth=(API_KEY, 'dummy'))
        assert response.status_code == 200
        response_json = response.json()

        assert response_json["page"] == 1 # lazy..
        assert response_json["pages"] == 1 # lazy.. but works for now

        for i in response_json["items"]:
            yield (i['id'], i['name'])

def list_conversations(mailbox_id):
    page=1
    while True:
        time.sleep(0.4) # throttle API requests
        url="https://api.helpscout.net/v1/mailboxes/%d/conversations.json?page=%d" % (mailbox_id, page)
        print url
        response = requests.get( url , auth=(API_KEY,  'dummy'))
        assert response.status_code == 200

        response_json = response.json()
        assert response_json["page"] == page
        pages=response_json["pages"]

        print ("Page %d of %d" % (page, pages))

        for i in response_json["items"]:
            assert i['type'] == 'email'
            yield (i['id'], i['number'])


        if page >= pages:
            break
        else:
            page = page +1

def fetch_attachment_data(attachment_id):
        time.sleep(1) # throttle API requests
        url="https://api.helpscout.net/v1/attachments/%d/data.json" % (attachment_id)
        print url
        response = requests.get( url , auth=(API_KEY,  'dummy'))

        if response.status_code == 404:
            return False

        assert response.status_code == 200
        response_json = response.json()
        r = response_json['item']
        print r['id']
        return base64.b64decode(r['data'])

def store_file(key, data, type):

    # local file
    f_key="data/%s" % key
    print ("Storing to local file: %s" % f_key)
    d=os.path.dirname(f_key)
    if not os.path.exists(d):
        print("mkdir: %s" % d)
        os.makedirs(d)

    assert not os.path.exists(f_key) # check for duplicate files
    with open(f_key, 'w') as file:
        file.write(data)

    # S3
    print ("Storing to S3: %s" % key)
    AWS_BUCKET_NAME = 'helpscout-export'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)

    bucket.put_object(
        ACL='public-read',
        ContentType=type,
        Key=key,
        Body=data,
    )

#============================================================
def process_conversation(conversation_id, path):
        time.sleep(0.4) # throttle API requests
        url="https://api.helpscout.net/v1/conversations/%d.json" % (conversation_id)
        print url
        response = requests.get( url , auth=(API_KEY,  'dummy'))

#       print "response.status_code: %d" %  response.status_code
#        try:
#            assert response.status_code == 200
#        except AssertionError:
#            print "response.status_code: %d" %  response.status_code

        response_json = response.json()
        conversation = response_json["item"]
        assert conversation['type'] == 'email'

        if store_json:
            store_file("%s/conversation.json" % path, json.dumps(conversation), "application/json")

        #print ("Conversation %d\nSubject: %s" % (conversation['number'], conversation['subject']))

        if 'threads' in conversation:
            for t in conversation['threads']:
                print ( "Thread id: %d" % t['id'])
                if store_json:
                    store_file("%s/thread_%d.json" % (path,t['id']), json.dumps(t), "application/json")

                if ('attachments' in t) and (t['attachments'] is not None):
                    print "Found attachments"
                    for a in t['attachments']:
                            assert 'fileName' in a
                            #print ( "attachment: %s" % a['fileName'])
                            #print json.dumps( a )
                            if store_json:
                                store_file("%s/attachment_%d.json" % (path, a['id']), json.dumps(a), 'application/json')

                            a_key = "%s/attachment_%d_%s" % (path, a['id'], a['fileName'])
                            a_data = fetch_attachment_data(a['id'])

                            if a_data is not False:
                                store_file( a_key, a_data, a['mimeType'])
                            else:
                                store_file( ("%s_%d" % (a_key, 404)) , "", a['mimeType'])

        return True # done
#============================================================
# BEGIN

# Create database if not already present
if os.path.isfile(db_file):
    db = sqlite3.connect(db_file)
else:
    db = sqlite3.connect(db_file)
    db_create(db)

# PART 1: Walk through mailboxes, fetch list of conversations and save it in sqlite DB

#mailbox_list=[28847] # test
# Mailbox 'Java Fundamentals': 28847 # 1068 items

mailbox_list=[8037,11634,8428] # real
# Mailbox 'Sales Operations':   8037 # 25575 items
# Mailbox 'ZT EE Accounting':  11634 # 26730 items
# Mailbox 'ZT Sales':           8428 # 23209 items

if not True:
    for mb_id, mb_name in list_mailboxes():
        print ("Mailbox '%s': %d" % (mb_name, mb_id))
        if mb_id in mailbox_list :
            for c_id, c_number in list_conversations(mb_id):
                db_insert_conversation(db, (c_id, c_number, mb_name))
    db.close()
    sys.exit()

# PART 2: Process conversations, also keeping track of state

if True:
    print "Processing conversations"

    work=True
    while work:
        work=False
        for (c_id, mailbox) in db_fetch_new_conversation(db):
            work=True
            print "Conversation: %d" % c_id
            path="%s/%d" % (mailbox, c_id) # or... (mailbox.replace(" ", "_")
            if process_conversation(c_id, path) == True:
                db_mark_conversation_as_done(db, c_id)
    print "Done"

    db.close()
    sys.exit()

