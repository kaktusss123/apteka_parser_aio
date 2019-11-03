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
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(parse_vivafarm(loop))
    except Exception as e:
        return '{}: {}'.format(e.__class__.__name__, e)
    return send_from_directory('./', 'vivafarm.py.csv')


@app.route('/extractum', methods=['GET'])
def extractum():
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(parse_extractum(loop))
    except Exception as e:
        return '{}: {}'.format(e.__class__.__name__, e)
    return send_from_directory('./', 'extractum.py.csv')


# app.run()
app.run('45.147.198.134', 9512)
