import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



def check_user(connection, sber_id):
    if sber_id is None:
        return None
    if len(sber_id) == 0:
        return None
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = "select * from users where sber_id = %s"
        cursor.execute(select, (sber_id,))
        if cursor.rowcount == 0:
            return None
        else:
            data = cursor.fetchall()[0]
            return {'user_id': data['user_id'], 'login': data['login']}
    except Exception as e:
        print(e)
        return None
    finally:
        cursor.close()


def check_login(connection, sber_id, login):
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = "select * from users where sber_id = %s or login = %s"
        cursor.execute(select, (sber_id, login))
        if cursor.rowcount == 0:
            return None
        else:
            return cursor.fetchall()[0]['user_id']
    except Exception as e:
        print(e)
        return None
    finally:
        cursor.close()


def new_user(connection, sber_id, login):
    if login is None or sber_id is None:
        print("корявые данные")
        print("корявые данные по длинне",login,sber_id)
        return False
    if len(login) == 0 or len(sber_id) == 0:
        print("корявые данные по длинне",login,sber_id,len(login),len(sber_id))
        return False
    user_id = check_login(connection, sber_id, login)
    if user_id != None:
        print("юзер такой есть")
        return False
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select max(user_id) as n from users'
        cursor.execute(select)
        n = 1
        try:
            n = cursor.fetchall()[0]['n'] + 1
        except:
            pass
        select = 'INSERT INTO users VALUES (%s, %s, %s)'
        cursor.execute(select, (n, sber_id, login))
        return n
    except Exception as e:
        print(e)
        return False
    finally:
        cursor.close()


def new_friends(connection, sber_id, login):
    user_id = check_user(connection, sber_id)
    if user_id is None:
        return {'result': False, 'status': "Вас нет в списке пользователей!"}
    user_id = user_id['user_id']
    user_friend_id = check_login(connection, None, login)
    if user_friend_id is None:
        return {'result': False, 'status': "Пользователь с логином " + str(login) + " не найден!"}
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select * from friends where user_id = %s and friend_user_id = %s'
        cursor.execute(select, (user_id, user_friend_id))
        if cursor.rowcount != 0:
            return {'result': cursor.fetchall()[0]['friends_id'], 'status': 'ok'}
        select = 'select max(friends_id) as n from friends'
        cursor.execute(select)
        n = 1
        try:
            n = cursor.fetchall()[0]['n'] + 1
        except:
            pass
        select = 'INSERT INTO friends VALUES (%s, %s, %s, %s)'
        cursor.execute(select, (n, user_id, user_friend_id, None))
        return {'result': n, 'status': 'ok'}
    except Exception as e:
        print(e)
        return {'result': False, 'status': "Техническая ошибка!"}
    finally:
        cursor.close()


def message(connection, sber_id, login, message):
    if message is None or len(message) == 0:
        return {'result': False, 'status': "Нет сообщения!"}
    user_id = check_user(connection, sber_id)
    if user_id is None:
        return {'result': False, 'status': "Вас нет в списке пользователей!"}
    user_id = user_id['user_id']
    user_friend_id = check_login(connection, None, login)
    if user_friend_id is None:
        return {'result': False, 'status': "Пользователь с логином " + str(login) + " не найден!"}
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select * from friends where user_id = %s and friend_user_id = %s'
        cursor.execute(select, (user_friend_id, user_id))
        if cursor.rowcount == 0:
            return {'result': False, 'status': 'Вы должны быть добавлены в друзья у пользователя ' + login}
        friends_id = cursor.fetchall()[0]['friends_id']
        select = 'update friends set message = %s where friends_id = %s'
        cursor.execute(select, (message, friends_id))
        if cursor.rowcount == 1:
            return {'result': True, 'status': 'Сообщение сохранено!'}
    except Exception as e:
        print(e)
        return {'result': False, 'status': "Техническая ошибка!"}
    finally:
        cursor.close()


def set_standart_message(connection, message):
    select = 'select max(message_id) as n from messages'
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        cursor.execute(select)
        n = 1
        try:
            n = cursor.fetchall()[0]['n'] + 1
        except:
            pass
        select = 'INSERT INTO messages VALUES (%s, %s)'
        cursor.execute(select, (n, message))
    except Exception as e:
        print(e)
    finally:
        cursor.close()


def get_message_from_user(connection, user_id):
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select * from friends where user_id = %s and message is not null limit 1'
        cursor.execute(select, (user_id,))
        if cursor.rowcount == 0:
            return None
        data = cursor.fetchall()[0]
        select = 'update friends set message = null where friends_id = %s'
        cursor.execute(select, (data['friends_id'],))
        return data['message']
    except Exception as e:
        print(e)
        return None
    finally:
        cursor.close()


def get_standart_message(connection, user_id):
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select m.message_id, m.message, case when x.n is not null then n + 1 else 1 end as n_times '
        select += 'from messages m '
        select += 'left join user_x_message x on x.user_id = %s and x.message_id = m.message_id '
        select += 'order by n_times limit 1'
        cursor.execute(select, (user_id,))
        data = cursor.fetchall()[0]
        if data['n_times'] == 1:
            select = 'select max(id) as n from user_x_message'
            cursor.execute(select)
            id = 1
            try:
                id = cursor.fetchall()[0]['n'] + 1
            except:
                pass
            select = 'select min(n) as n from user_x_message where user_id = %s'
            cursor.execute(select, (user_id,))
            n = 1
            try:
                n = cursor.fetchall()[0]['n'] + 1
            except:
                pass
            select = 'INSERT INTO user_x_message VALUES (%s, %s, %s, %s)'
            cursor.execute(select, (id, user_id, data['message_id'], n))
        else:
            select = 'update user_x_message set n = %s where user_id = %s and message_id = %s'
            cursor.execute(select, (data['n_times'], user_id, data['message_id']))
        return data['message']
    except Exception as e:
        print(e)
        return None
    finally:
        cursor.close()


def get_some_message(connection, sber_id):
    user_id = check_user(connection, sber_id)
    if user_id is None:
        return None
    user_id = user_id['user_id']
    message = get_message_from_user(connection, user_id)
    if message is not None:
        return message
    return get_standart_message(connection, user_id)


def get_address_book(connection, sber_id):
    user_id = check_user(connection, sber_id)
    if user_id is None:
        return None
    user_id = user_id['user_id']
    cursor = connection.cursor(cursor_factory=DictCursor)
    try:
        select = 'select u.login from users u '
        select += 'inner join friends f on u.user_id = f.user_id and f.friend_user_id = %s '
        select += 'limit 10'
        cursor.execute(select, (user_id, ))
        if cursor.rowcount == 0:
            return []
        numbers = []
        for line in cursor.fetchall():
            numbers.append(line['login'])
        return numbers
    except Exception as e:
        print(e)
        return []
    finally:
        cursor.close()
