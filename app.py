from flask import Flask, jsonify, request, abort
import bd_app
import psycopg2

connection = psycopg2.connect(user="postgres", host='localhost', port="5432", password="test1", database="test_db")
connection.autocommit = True

app = Flask(__name__)

#проверка на зарегестрированность пользователя
@app.route('/check_user', methods=['POST'])
def check_user():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    user_id = bd_app.check_user(connection,str(sber_id))
    status = 1
    login = "нет логина"
    if user_id is None:
        status = 0
    else:
        login = user_id['login']
    return jsonify({'status': status, 'login': login})


#заведение нового пользователя
@app.route('/new_user', methods=['POST'])
def new_user():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    if not 'login' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    login = request.json['login']
    user_id = bd_app.new_user(connection, str(sber_id), str(login))
    status = 1
    if user_id is None or user_id == False:
        status = 0
    print(user_id, login)
    return jsonify({'status': status})


#добавление в друзья
@app.route('/new_friends', methods=['POST'])
def new_friends():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    if not 'login' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    login = request.json['login']
    result = bd_app.new_friends(connection, str(sber_id), str(login))
    return jsonify({'result': result['result'], 'status': result['status']})


#отправка сообщения другому пользователю
@app.route('/message', methods=['POST'])
def message():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    if not 'login' in request.json:
        abort(400)
    if not 'message' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    login = request.json['login']
    message = request.json['message']
    result = bd_app.message(connection, str(sber_id), str(login), str(message))
    return jsonify({'result': result['result'], 'status': result['status']})


#выдать сообщение
@app.route('/get_some_message', methods=['POST'])
def get_some_message():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    result = bd_app.get_some_message(connection, str(sber_id))
    if result == None:
        abort(400)
    return jsonify({'message': result})


#выдать список тех, кому можно отправить сообщение
@app.route('/get_address_book', methods=['POST'])
def get_address_book():
    if not request.json or not 'sber_id' in request.json:
        abort(400)
    sber_id = request.json['sber_id']
    result = bd_app.get_address_book(connection, str(sber_id))
    if result == None:
        abort(400)
    return jsonify({'address_book': result})


#админский метод, добавитьт новый варинт с добрым утром
@app.route('/set_standart_message', methods=['POST'])
def set_standart_message():
    if not request.json or not 'message' in request.json:
        abort(400)
    message = request.json['message']
    bd_app.set_standart_message(connection, str(message))
    return jsonify({'save': message})


@app.route('/')
def hello():
    return "Hello World!"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, debug=False)
