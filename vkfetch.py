import requests
import psycopg2
import warnings

from psycopg2.sql import SQL, Identifier

access_token = 'token'
version = 5.92
default_count = 100
max_posts_count = 5000

psql_db_name = 'text_classification_db'
psql_table_prefix = 'vk_wall_posts'
psql_user = 'postgres'
psql_pass = '1'
psql_host = '/var/run/postgresql/'


def psql_connect_and_write_data(items, entity_id):
    psql_table_name = psql_table_prefix + '_' + str(entity_id)
    conn = psycopg2.connect(dbname=psql_db_name, user=psql_user, password=psql_pass)
    cur = conn.cursor()
    # cur.execute(SQL("DROP TABLE IF EXISTS {};").format(Identifier(psql_table_name)))
    if not table_exists(psql_table_name):
        cur.execute(SQL("CREATE TABLE {} (id int PRIMARY KEY, text varchar);").format(Identifier(psql_table_name)))
        for item in items:
            cur.execute(SQL("INSERT INTO {} (id, text) VALUES (%s, %s) "
                        "ON CONFLICT (id) DO NOTHING;").format(Identifier(psql_table_name)),
                        (item['id'], item['text']))
        print_psql_rows_count(cur, psql_table_name)
    conn.commit()
    cur.close()
    conn.close()


def table_exists(psql_table_name):
    conn = psycopg2.connect(dbname=psql_db_name, user=psql_user, password=psql_pass)
    cur = conn.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (psql_table_name, ))
    res = cur.fetchone()[0]
    cur.close()
    conn.close()
    return res


def print_psql_rows_count(cursor, table_name):
    cursor.execute(SQL("SELECT COUNT(*) FROM {};").format(Identifier(table_name)))
    print(cursor.fetchone()[0])


def psql_get_rows_count(table_name):
    conn = psycopg2.connect(dbname=psql_db_name, user=psql_user, host=psql_host)
    cur = conn.cursor()
    print_psql_rows_count(cur, table_name)
    cur.close()
    conn.close()


def vk_api_get_wall_posts(offset, count, owner_id):
    response = requests.get('https://api.vk.com/method/wall.get',
                            {'access_token': access_token, 'v': version, 'owner_id': owner_id,
                             'offset': offset, 'count': count})
    if response.status_code != 200:
        warnings.warn('wall.get {}'.format(response.status_code), Warning)
        return 0, []
    response_dict = response.json()
    if 'error' in response_dict:
        warnings.warn('wall.get error: ' + str(response_dict), Warning)
        return 0, []
    else:
        resp_val = response_dict['response']
        return resp_val['count'], resp_val['items']


def vk_api_get_all_wall_posts(owner_ids):
    for owner_id in owner_ids:
        psql_table_name = psql_table_prefix + '_' + str(owner_id)
        if table_exists(psql_table_name):
            continue
        total_items = []
        total_items_count, items = vk_api_get_wall_posts(0, default_count, '-' + str(owner_id))
        total_items += items
        total_items_count = min(total_items_count, max_posts_count)
        for i in range(default_count, total_items_count, default_count):
            total_count, items = vk_api_get_wall_posts(i, default_count, '-' + str(owner_id))
            total_items += items
        if len(total_items) > 0:
            psql_connect_and_write_data(total_items, owner_id)


#get_data_limit([128033123, 69671264])

#r"\b[а-яА-Яa-zA-Z]{3,}|\B#[а-яА-Яa-zA-Z]{3,}\b"

vk_api_get_all_wall_posts([51045049, 47951388, 55045888, 115357087, 71785575, 53388683, 47118092, 123632634, 51068271,
                           48303580, 121344058, 32894860, 160506183, 126967384, 92876084])
