#!/usr/bin/env python

import sys; sys.path.insert(0, 'lib') # this line is necessary for the rest
import os                             # of the imports to work!

import web
import sqlitedb
from jinja2 import Environment, FileSystemLoader

###########################################################################################
##########################DO NOT CHANGE ANYTHING ABOVE THIS LINE!##########################
###########################################################################################

######################BEGIN HELPER METHODS######################

# helper method to render a template in the templates/ directory
#
# `template_name': name of template file to render
#
# `**context': a dictionary of variable names mapped to values
# that is passed to Jinja2's templating engine
#
# See curr_time's `GET' method for sample usage
#
# WARNING: DO NOT CHANGE THIS METHOD
def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(autoescape=True,
            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
            extensions=extensions,
            )
    jinja_env.globals.update(globals)

    web.header('Content-Type','text/html; charset=utf-8', unique=True)

    return jinja_env.get_template(template_name).render(context)

#####################END HELPER METHODS#####################

urls = (
    '/currtime', 'curr_time',
    '/selecttime', 'select_time',
    '/search', 'search',
    '/add_bid', 'add_bid',
    '/auction', 'auction',
)


class curr_time:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        current_time = sqlitedb.getTime()
        return render_template('curr_time.html', time = current_time)


class select_time:
    # Another GET request, this time to the URL '/selecttime'
    def GET(self):
        return render_template('select_time.html')

    # A POST request
    #
    # You can fetch the parameters passed to the URL
    # by calling `web.input()' for **both** POST requests
    # and GET requests
    def POST(self):
        post_params = web.input()
        MM = post_params['MM']
        dd = post_params['dd']
        yyyy = post_params['yyyy']
        HH = post_params['HH']
        mm = post_params['mm']
        ss = post_params['ss'];
        enter_name = post_params['entername']


        selected_time = '%s-%s-%s %s:%s:%s' % (yyyy, MM, dd, HH, mm, ss)
        update_message = '(Hello, %s. Previously selected time was: %s.)' % (enter_name, selected_time)
        # TODO: save the selected time as the current time in the database

        try:
            sqlitedb.updateTime(selected_time)
        except Exception as e:
            return render_template('select_time.html', message = str(e))

        # Here, we assign `update_message' to `message', which means
        # we'll refer to it in our template as `message'
        return render_template('select_time.html', message = update_message)


class auction:
    def GET(self):
        get_params = web.input()
        itemID = get_params['itemID']

        # get item tuple from item ID
        item = sqlitedb.getItemById(itemID)

        # get bids
        bids = sqlitedb.getBids(itemID)

        # get categories
        categories = sqlitedb.getCategories(itemID)

        # get auction status
        auction_status = sqlitedb.getAuctionStatus(itemID)

        # get winner if auction is closed
        winner = None
        if auction_status == sqlitedb.AuctionStatus.CLOSED:
            winner = sqlitedb.getWinner(itemID)

        return render_template(
            'auction.html',
            item=item,
            bids=bids,
            categories=categories,
            auction_status=auction_status,
            winner=winner
        )


class add_bid:
    def GET(self):
        return render_template('add_bid.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        userID = post_params['userID']
        price = post_params['price']

        try:
            sqlitedb.addBid(itemID, userID, price)
        except Exception as e:
             return render_template('add_bid.html', add_result = False, error_message = str(e))

        return render_template('add_bid.html', add_result = True)


class search:
    def GET(self):
        return render_template('search.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        minPrice = post_params['minPrice']
        maxPrice = post_params['maxPrice']
        category = post_params['category']
        description = post_params['description']
        status = post_params['status']

        search_results = sqlitedb.find_auctions_results_from_db(
            itemID,
            minPrice,
            maxPrice,
            category,
            description,
            status
        )

        return render_template('search.html', search_results=search_results)


###########################################################################################
##########################DO NOT CHANGE ANYTHING BELOW THIS LINE!##########################
###########################################################################################

if __name__ == '__main__':
    web.internalerror = web.debugerror
    app = web.application(urls, globals())
    app.add_processor(web.loadhook(sqlitedb.enforceForeignKey))
    app.run()
