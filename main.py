from flask import Flask, send_from_directory
from vivafarm import parse_vivafarm
from extractum import parse_extractum
import asyncio

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def test():
    return 'Hello, world'


@app.route('/vivafarm', methods=['GET'])
def vivafarm():
    asyncio.run(parse_vivafarm())
    return send_from_directory('./', 'vivafarm.py.csv')


@app.route('/extractum', methods=['GET'])
def extractum():
    asyncio.run(parse_extractum())
    return send_from_directory('./', 'extractum.py.csv')


app.run()
