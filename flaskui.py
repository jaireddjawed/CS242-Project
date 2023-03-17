import flask
from flask import Flask, render_template, request
from pylucene import retrieve
from search_engine import retrieve_documents
from flask_bootstrap import Bootstrap

#create_index('sample_lucene_index/')

app = Flask(__name__)
bootstrap = Bootstrap(app)

@app.route('/')
def home():
    return render_template('home_b.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        query = request.form['query']
        index_type = request.form['index_type']
        if index_type == 'lucene':
            results = retrieve(query)
        elif index_type == 'bert':
            results = retrieve_documents(query)
        return render_template('results.html', query=query, results=results)
    else:
        return render_template('home_b.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888, debug=True)

