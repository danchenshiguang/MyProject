from flask import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, date_format
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
# from sparkfunction.loyal_user import loyal_user_find
# from sparkfunction.loyal_user import df

from pyecharts.charts import Bar
from pyecharts import options as opts

bar = Bar()
bar.add_xaxis(["衬衫", "毛衣", "领带", "裤子", "风衣", "高跟鞋", "袜子"])
bar.add_yaxis("商家A", [114, 55, 27, 101, 125, 27, 105])
bar.add_yaxis("商家B", [57, 134, 137, 129, 145, 60, 49])
bar.set_global_opts(title_opts=opts.TitleOpts(title="某商场销售情况"))
bar = bar.render_embed()
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin_day') \
    .getOrCreate()
checkin_schema = StructType([StructField("business_id", StringType(), True),
                             StructField("col", TimestampType(), True)])
df = spark.read.json('yelp_academic_dataset_tip.json')
app = Flask(__name__)

app.secret_key = 'alskdjflkj'


# from sparkfunction import loyal_user


# from loyal_user import loyal_user_find,df
# from bs4 import BeautifulSoup
@app.route('/', methods=['POST', 'GET'])
def index():
    return render_template('index.html')


# def hello_world():  # put application's code here
#     # 往模板中传入的数据
#     # loyal_user = loyal_user_find(df, "XQfwVwDr-v0ZS3_CbbE5Xw")
#     # loyal_user_pd =loyal_user.toPandas()
#     # # html_table = loyal_user_pd.to_html(index=False,  classes = './static/css/my_style.css')
#     # loyal_user_dict=loyal_user_pd.to_dict()
#     # if  request.method == 'POST':
#     #     return redirect(url_for('user_recommend'))
#
#     return render_template("user_recommend.html")


@app.route("/session_user")
def user_session():
    return render_template("user_session.html")


@app.route("/business_session")
def business_session():
    return render_template("business_session.html")


'''
商户的界面
'''


@app.route('/business_page', methods=['POST', 'GET'])
def business_page():
    if request.method == 'POST':
        if request.form.get('business_id') != None:
            session['username'] = request.form['business_id']
            return render_template("business_page.html", result=session['username'])
    return render_template("business_page.html", result="please sign in ")


@app.route('/Portrait_of_the_business', methods=['POST', 'GET'])
def Portrait_of_the_business():
    # if request.method == 'POST':
    # session['username'] = request.form['user_id']
    return render_template("Portrait_of_the_business.html", result=session['username'])


@app.route('/Business_location_analysis', methods=['POST', 'GET'])
def Business_location_analysis():
    # if request.method == 'POST':
    # session['username'] = request.form['business_id']
    return render_template("Business location analysis.html", result=session['username'])


@app.route('/Loyal_user_division', methods=['POST', 'GET'])
def Loyal_user_division():
    # if request.method == 'POST':
    # session['username'] = request.form['user_id']
    return render_template("Loyal user division.html", result=session['username'])


@app.route('/Analysis_of_trends', methods=['POST', 'GET'])
def Analysis_of_trends():
    # if request.method == 'POST':
    # session['username'] = request.form['user_id']
    return render_template("Analysis of trends.html", result=session['username'])


@app.route('/Business_information_view', methods=['POST', 'GET'])
def Business_information_view():
    # if request.method == 'POST':
    # session['username'] = request.form['user_id']
    return render_template("Business information view.html", result=session['username'])


@app.route('/Business_annual_summary', methods=['POST', 'GET'])
def Business_annual_summary():
    # if request.method == 'POST':
    #     session['username'] = request.form['user_id']
    return render_template("Business annual summary.html", result=session['username'])


''''

用户的页面
'''


@app.route('/user_recommend', methods=['POST', 'GET'])
def user_recommend():  # put application's code here
    test = df.select('user_id', 'date').limit(10)
    test_pd = test.toPandas()
    testdict = test_pd.to_dict()
    # 往模板中传入的数据
    # loyal_user = loyal_user_find(df, "XQfwVwDr-v0ZS3_CbbE5Xw")
    # loyal_user_pd = loyal_user.toPandas()
    # html_table = loyal_user_pd.to_html(index=False,  classes = './static/css/my_style.css')
    # loyal_user_dict = loyal_user_pd.to_dict()
    indexs = [0, 1, 2]
    if request.method == 'POST':
        if request.form.get('offset') != None:
            offset = request.form['offset']
            indexs = [i + int(offset) * len(indexs) for i in indexs]
            return render_template("user_recommend.html", result=session['username'], data=testdict,
                                   offset=indexs)
        else:
            session['username'] = request.form.get('username')
            return render_template("user_recommend.html", result=session['username'], data=testdict,
                                   offset=indexs,figure=bar)
    return render_template("user_recommend.html", result='please sign in', data=testdict, offset=indexs)


@app.route('/user_recommend_1', methods=['POST', 'GET'])
def user_recommend_1():  # put application's code here
    return render_template("user_recommend_1.html")


@app.route('/user_recommend_2', methods=['POST', 'GET'])
def user_recommend_2():  # put application's code here
    return render_template("user_recommend_2.html")


@app.route('/user_recommend_3', methods=['POST', 'GET'])
def user_recommend_3():  # put application's code here
    return render_template("user_recommend_3.html")


@app.route('/user_recommend_4', methods=['POST', 'GET'])
def user_recommend_4():  # put application's code here
    return render_template("user_recommend_4.html")


@app.route('/user_recommend_5', methods=['POST', 'GET'])
def user_recommend_5():  # put application's code here
    return render_template("user_recommend_5.html")


if __name__ == '__main__':
    app.run()
