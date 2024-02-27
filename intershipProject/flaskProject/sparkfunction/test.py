from flask import Flask, render_template, request,redirect,url_for
import pandas as pd

app = Flask(__name__)


@app.route('/',methods=['POST','GET'])
def index():
    # 生成一个 Pandas DataFrame
    df = {'name': {0: 'Alice', 1: 'Bob', 2: 'Charlie'}, 'age': {0: 25, 1: 30, 2: 35}}
    offset = 0
    if request.method == 'POST':
        offset = request.form['offset']
        return render_template('test.html', table_html= df, offset= int(offset))
    # # 将 DataFrame 转换成 HTML 表格字符串
    # table_html = df.to_html(index=False)
    # 渲染模板并传入 HTML 表格字符串和 DataFrame 列名列表
    return render_template('test.html', table_html=df, offset=offset)
# render_template('test.html', table_html=df, offset=offset)
# @app.route('/test', methods=['POST', 'GET'])
# def test():
#     if request.method == 'POST':
#         offset = request.form['offset']
#         return render_template('test.html',  offset=offset)
#
