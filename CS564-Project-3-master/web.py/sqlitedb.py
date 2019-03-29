import web

from datetime import datetime

class AuctionStatus:
    NOTSTARTED = 1
    OPEN = 2
    CLOSED = 3

db = web.database(dbn='sqlite',
        db='AuctionBase' #TODO: add your SQLite database filename
    )

######################BEGIN HELPER METHODS######################

# helper method to convert times from database (which will return a string)
# into datetime objects. This will allow you to compare times correctly (using
# ==, !=, <, >, etc.) instead of lexicographically as strings.

# Sample use:
# current_time = string_to_time(sqlitedb.getTime())
def string_to_time(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()

# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples


def find_auctions_results_from_db(itemID, minPrice, maxPrice, category, description, status):
    '''
        returns the auction details based on the input parameter
    '''

    query_string = "SELECT * FROM Items"
    select_conditions = []

    if category:
        query_string += " INNER JOIN Categories ON (Items.ItemID = Categories.ItemID)"
        select_conditions.append("Categories.Category='%s'" % (category))

    if itemID:
        select_conditions.append("Items.ItemID='%s'" % (itemID))

    if description:
        select_conditions.append("DESCRIPTION LIKE '%" + description + "%'")

    if status:
        curr_time = getTime()
        if status == 'open':
            select_conditions.append("Started <= '%s' AND Ends >= '%s' AND Currently < Buy_Price" % (curr_time, curr_time))
        elif status == 'close':
            select_conditions.append("Ends <= '%s' OR Currently >= Buy_Price" % (curr_time))
        elif status == 'notStarted':
            select_conditions.append("Started > '%s'" % (curr_time))

    if minPrice:
        select_conditions.append("Currently >= %s" % (minPrice))

    if maxPrice:
        select_conditions.append("Currently <= %s" % (maxPrice))

    # check if we need to insert a WHERE clause
    if select_conditions:
        # start with a WHERE clause
        query_string += ' WHERE '

        # add brackets around every individual condition
        select_conditions = ['(' + condition + ')' for condition in select_conditions]

        # AND all the conditions
        query_string += ' AND '.join(select_conditions)
    return db.query(query_string)


# returns the current time from your database
def getTime():
    query_string = 'SELECT time FROM CurrentTime'
    results = query(query_string)

    return results[0].Time


# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # TODO: rewrite this method to catch the Exception in case `result' is empty
    query_string = 'SELECT * FROM Items WHERE itemID = $itemID'
    result = query(query_string, {'itemID': item_id})

    try:
        return result[0]
    except IndexError:
        return None


# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

def getAuctionStatus(itemID):
    # get item object from database
    item = getItemById(itemID)

    if not item:
        return 

    # get current time and time attributes from item
    current_time = string_to_time(getTime())
    item_start_time = string_to_time(item["Started"])
    item_end_time = string_to_time(item["Ends"])

    # auction is closed if we're not inside start and end time
    if current_time < item_start_time:
        return AuctionStatus.NOTSTARTED

    if current_time >= item_end_time:
        return AuctionStatus.CLOSED

    # get current price and buy price
    # also convert them to floats
    item_current_price = None
    if item["Currently"]:
        item_current_price = float(item["Currently"])

    item_buy_price = None
    if item["Buy_Price"]:
        item_buy_price = float(item["Buy_Price"])

    # if buy price is reached, then we close the auction
    if item_current_price and item_buy_price and item_current_price >= item_buy_price:
        return AuctionStatus.CLOSED

    return AuctionStatus.OPEN


def updateTime(new_time):
    query_string = "UPDATE CurrentTime SET Time = '%s'" % (new_time)

    t = transaction()
    try:
        result = db.query(query_string)
    except Exception as e:
        t.rollback()
        raise e
    else:
        t.commit()


def getBids(itemID):
    query_string = "SELECT * from Bids WHERE ItemID='%s'" % (itemID)
    return db.query(query_string)


def getCategories(itemID):
    query_string = "SELECT Category from Categories WHERE ItemID='%s'" % (itemID)
    return db.query(query_string)


def getWinner(itemID):
    # get buy price from item
    item_query_string = " SELECT Buy_Price from Items where Items.ItemID = '%s' " % (itemID)
    buy_price = db.query(item_query_string)[0]["Buy_Price"]

    # if buy price exists then get the bid greater than buy price
    if buy_price:
        query_string = """ SELECT Bids.UserID from Bids
                           INNER JOIN Items ON (Items.ItemID = Bids.ItemID)
                           WHERE Items.ItemID='%s'
                           AND Bids.Amount >= Items.Buy_Price
                           ORDER BY Bids.Amount Desc LIMIT 1 """\
                           % (itemID)
    else:
        # if buy price doesn't exist, then get the highest bid
        query_string = """ SELECT Bids.UserID from Bids
                           INNER JOIN Items ON (Items.ItemID = Bids.ItemID)
                           WHERE Items.ItemID='%s'
                           ORDER BY Bids.Amount Desc LIMIT 1 """\
                           % (itemID)

    try:
        winner = query(query_string)[0]["UserID"]
        return winner
    except IndexError:
        return None


def addBid(itemID, userID, price):
    t = transaction()
    try:
        # get current time
        current_time = getTime()

        query_string = "INSERT INTO Bids VALUES ('%s', '%s', '%s', '%s')" % (itemID,
        userID, price, current_time)

        # check if auction is open
        # if not, raise an exception
        auction_status = getAuctionStatus(itemID)
        if auction_status == AuctionStatus.NOTSTARTED:
            raise Exception("Auction has not started yet")

        if auction_status == AuctionStatus.CLOSED:
            raise Exception("Auction is closed")

        result = db.query(query_string)
    except Exception as e:
        t.rollback()
        raise e
    else:
        t.commit()
