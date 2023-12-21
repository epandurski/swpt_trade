#!/usr/bin/env python

from swpt_trade import create_app

app = create_app()

if __name__ == '__main__':
    import os

    app.run(host='0.0.0.0', port=os.environ.get('WEBSERVER_PORT', 5000), debug=True, use_reloader=False)
