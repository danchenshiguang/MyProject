import sys

sys.path.insert(0, "D:\pythonProject\pydata")

from flask import Flask, session, redirect, url_for, request, render_template

app = Flask(__name__)

app.secret_key = 'fkdjsafjdkfdlkjfadskjfadskljdsfklj'


@app.route('/', methods=['POST', 'GET'])
def index():
    df = {'name': {0: 'alice', 1: 'bob', 2: 'coc'}, 'age': {0: 3, 1: 5, 2: 7}}
    offset = 0
    if request.method == 'POST':
        if request.form.get('offset') != None:
            offset = request.form['offset']
            return render_template('test.html', table_html=df, offset=int(offset))
        else:
            offset = 0
            render_template('test.html', table_html=df, offset=offset)

    return render_template('test.html', table_html=df, offset=offset)


@app.route('/test_2', methods=['POST', 'GET'])
def test_2():
    df = {'name': {0: 'alice', 1: 'bob', 2: 'coc'}, 'age': {0: 3, 1: 5, 2: 7}}
    offset = 0
    if request.method == 'POST':
        offset = request.form['offset']
        return render_template('test.html', table_html=df, offset=int(offset))

    return render_template('test2.html', table_html=df, offset=offset)


if __name__ == '__main__':
    app.run(debug=True)
