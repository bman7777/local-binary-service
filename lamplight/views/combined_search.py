
import json
from urllib.parse import unquote

from __init__ import app
from flask import request, Response
from utilities import criteria_helper


@app.route('/search', methods=['POST'])
def combined_search():
    return Combination.search(
        request.get_json(),
        int(unquote(request.args.get("page", "0"))),
        int(unquote(request.args.get("pageSize", "15"))))


class Combination(object):

    @staticmethod
    def search(criteria, page, page_size):
        resp = None
        if criteria:
            data, highlight, stats = criteria_helper.from_criteria(
                criteria, page, page_size)

            if data:
                resp = Response(json.dumps(
                    {
                        "data": data,
                        "highlight_verse": highlight,
                        "stats": stats
                    } ))

        # we found nothing, so return nothing
        if not resp:
            resp = Response()
            resp.status_code = 204
        else:
            # all actual responses should be json
            resp.headers["Content-Type"] = "application/json"

        return resp
