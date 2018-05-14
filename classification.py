import redis
import random
import names
import sqlite3
import json
import datetime as dt

############## SQLite3 ##############

n_users = 10000000

def init_sqlite():
    conn = sqlite3.connect('scores.db')
    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS classification;""")
    cursor.execute("""   
    CREATE TABLE IF NOT EXISTS classification (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            score INTEGER
    );
    """)
    return conn

def add_score_sqlite(scores_list, conn):
    cursor = conn.cursor()
    cursor.executemany("""
    INSERT INTO classification (name, score)
    VALUES (?, ?)
    """, scores_list)
    conn.commit()

def get_classification_sqlite(conn):
    cursor = conn.cursor()
    cursor.execute("""
    select name, score from classification
    order by score desc limit 10
    """)
    return cursor.fetchall()


############## Redis ##############

def init_redis():
    r = redis.Redis(host='localhost', port=6379)
    r.zremrangebyrank('classification', 0, n_users)
    return r

def add_score_redis(scores_list, r):
    i = 0
    while i < len(scores_list):
        r.zadd('classification', **{name:score for name, score in scores_list[i:i+1000]})
        i += 1000

def get_classification_redis(r):
    return r.zrevrange('classification', 0, 10, withscores=True)


if __name__ == '__main__':
    scores_list = None
    try:
        scores_list = json.load(open('scores.json', 'r'))
    except FileNotFoundError:
        scores_list = []
        for i in range(n_users):
            scores_list.append(('user'+ str(i), random.randint(0,n_users * 100)))
        print ('file dumped')
        json.dump(scores_list, open('scores.json', 'w'))

    conn = init_sqlite()
    r = init_redis()

    add_score_redis(scores_list, r)
    add_score_sqlite(scores_list, conn)

    start = dt.datetime.now()
    print (get_classification_sqlite(conn))
    print ('SQLite: ', dt.datetime.now() - start)

    start = dt.datetime.now()
    print (get_classification_redis(r))
    print ('Redis: ',dt.datetime.now() - start)

    conn.close()
