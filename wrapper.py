from flask import Flask
from flask import jsonify
from flask import request

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, query, Q, DocType, utils
from elasticsearch_dsl.exceptions import IllegalOperation

from flasgger import Swagger
from sortedcontainers import SortedList
from pprint import pprint
import json
from concurrent.futures import ThreadPoolExecutor

es = Elasticsearch( ["https://BitShares:Infrastructure@eu.elasticsearch.bitshares.ws:443"], timeout=60 )
#es = Elasticsearch( timeout=60 )


app = Flask(__name__)
from flask_cors import CORS, cross_origin
CORS(app)

app.config['SWAGGER'] = {
    'title': 'Bitshares ES API',
    'uiversion': 2
}
Swagger(app, template_file='wrapper.yaml')

# maybe write to files not to RAM
class ResponseCache:
    #pragma region
    # storage struct
    # {
    #    "account_name":
    #    {
    #        "datapoints": X,
    #        "from_date": t1,
    #        "to_date": t2,
    #        "data": [
    #           {respdata1},
    #           {respdata2}
    #        ]
    #    },
    #    "account_name2":
    #    {
    #        ...
    #    }
    # }

    def __init__(self, store_max):
        self.store_max = store_max
        self.storage = {}

    def add( self, account, datapoints, from_date, to_date, data ):
        self.storage[account] = {}
        stored = self.storage[account]
        stored["datapoints"] = datapoints
        stored["from_date"] = from_date
        stored["to_date"] = to_date
        stored["data"] = data

    def get( self, account, datapoints, from_date, to_date ):
        stored = {}
        try:
            stored = self.storage[account]
        except:
            return None

        if stored["from_date"] != from_date \
            or stored["to_date"] != to_date \
            or stored["datapoints"] != datapoints:
                return None

        return stored["data"]

account_data_self_total_cache = ResponseCache(100)
account_data_self_proxy_cache = ResponseCache(100)
voteable_votes_cache  = ResponseCache(100)

@app.route('/get_account_history')
def get_account_history():

    account_id = request.args.get('account_id', False)
    operation_type = request.args.get('operation_type', False)
    from_ = request.args.get('from_', 0)
    size = request.args.get('size', 10)
    from_date = request.args.get('from_date', "2015-10-10")
    to_date = request.args.get('to_date', "now")
    sort_by = request.args.get('sort_by', "-block_data.block_time")
    type = request.args.get('type', "data")
    agg_field = request.args.get('agg_field', "operation_type")

    if type != "data":
        s = Search(using=es, index="bitshares-*")
    else:
        s = Search(using=es, index="bitshares-*", extra={"size": size, "from": from_})

    q = Q()
    if account_id and operation_type:
        q = Q("match", account_history__account=account_id) & Q("match", operation_type=operation_type)
    elif account_id and not operation_type:
        q = Q("match", account_history__account=account_id)
    elif not account_id and operation_type:
        q = Q("match", operation_type=operation_type)

    range_query = Q("range", block_data__block_time={'gte': from_date, 'lte': to_date})
    s.query = q & range_query

    if type != "data":
        s.aggs.bucket('per_field', 'terms', field=agg_field, size=size)

    s = s.sort(sort_by)

    #print s.to_dict()

    response = s.execute()
    #print response
    results = []
    #print s.count()
    if type == "data":
        for hit in response:
            results.append(hit.to_dict())
    else:
        for field in response.aggregations.per_field.buckets:
            results.append(field.to_dict())

    return jsonify(results)


@app.route('/get_single_operation')
def get_single_operation():

    operation_id = request.args.get('operation_id', "1.11.0")

    s = Search(using=es, index="bitshares-*", extra={"size": 1})

    q = Q("match", account_history__operation_id=operation_id)

    s.query = q
    response = s.execute()
    results = []
    for hit in response:
        results.append(hit.to_dict())

    return jsonify(results)

@app.route('/is_alive')
def is_alive():
    find_string = datetime.utcnow().strftime("%Y-%m")
    from_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    s = Search(using=es, index="bitshares-" + find_string)
    s.query = Q("range", block_data__block_time={'gte': from_date, 'lte': "now"})
    s.aggs.metric("max_block_time", "max", field="block_data.block_time")

    json_response = {
        "server_time": datetime.utcnow(),
        "head_block_timestamp": None,
        "head_block_time": None
    }

    try:
        response = s.execute()
        if response.aggregations.max_block_time.value is not None:
            json_response["head_block_time"] = str(response.aggregations.max_block_time.value_as_string)
            json_response["head_block_timestamp"] = response.aggregations.max_block_time.value
            json_response["deltatime"] = abs((datetime.utcfromtimestamp(json_response["head_block_timestamp"] / 1000) - json_response["server_time"]).total_seconds())
            if json_response["deltatime"] < 30:
                json_response["status"] = "ok"
            else:
                json_response["status"] = "out_of_sync"
                json_response["error"] = "last_block_too_old"
        else:
            json_response["status"] = "out_of_sync"
            json_response["deltatime"] = "Infinite"
            json_response["query_index"] = find_string
            json_response["query_from_date"] = from_date
            json_response["error"] = "no_blocks_last_24_hours"
    except NotFoundError:
        json_response["status"] = "out_of_sync"
        json_response["deltatime"] = "Infinite"
        json_response["error"] = "index_not_found"
        json_response["query_index"] = find_string

    return jsonify(json_response)

@app.route('/get_trx')
def get_trx():

    trx = request.args.get('trx', "738be2bd22e2da31d587d281ea7ee9bd02b9dbf0")
    from_ = request.args.get('from_', 0)
    size = request.args.get('size', 10)

    s = Search(using=es, index="bitshares-*", extra={"size": size, "from": from_})

    q = Q("match", block_data__trx_id=trx)

    s.query = q
    response = s.execute()
    results = []
    for hit in response:
        results.append(hit.to_dict())

    return jsonify(results)


def make_equal_intervals( from_date, to_date, datapoints ):

    datetime_format = "%Y-%m-%dT%H:%M:%S"
    datetime_from   = datetime.strptime( from_date, datetime_format )
    datetime_to     = datetime.strptime( to_date, datetime_format )
    time_window_days = ( datetime_to - datetime_from ).days / datapoints
    delta_between_points = timedelta( days=time_window_days )

    datetime_intervals = []
    # extend the points by one to include a point around 'from_date' (if present)
    datetime_val = datetime_from - 2 * delta_between_points
    for i in range( datapoints + 1 ):
        datetime_val += delta_between_points
        datetime_val_str = datetime_val.strftime( datetime_format )
        datetime_intervals.append( datetime_val_str )

    return datetime_intervals

@app.route('/get_account_power_self_total')
def get_account_power_self_total(): # return time to power to proxied power
    print( "REQUEST RECEIVED" )
    global account_data_self_total_cache

    from_date = request.args.get( "from", "2018-01-01T00:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-15T00:00:00" ) # date after past date
    account   = request.args.get( "account", "1.2.285" ) # account_id
    datapoints = int( request.args.get( "datapoints", 100 ) )
    if datapoints > 700:
        datapoints = 700

    cached_data = account_data_self_total_cache.get( account, datapoints, from_date, to_date )
    if cached_data != None:
        return cached_data

    blocks = []
    block_time = []
    self_powers = []
    total_powers = []

    time_intervals = make_equal_intervals( from_date, to_date, datapoints )
    for i in range( 1, len(time_intervals) ):

        from_date_to_search = time_intervals[i-1]
        to_date_to_search = time_intervals[i]

        print( "Searching from: ", from_date_to_search, " to: ", to_date_to_search )

        req = Search( using=es, index="objects-voting-statistics", extra={"size": 1} ) # return size 1k
        req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
        req = req.sort("-block_number") # sort by blocknumber => last_block is first hit


        qaccount = Q( "match", account=account ) # match account
        qrange = Q( "range", block_time={ "gte": from_date_to_search, "lte": to_date_to_search} ) # match date range
        req.query = qaccount & qrange # combine queries

        response = req.execute()

        for hit in response:
            hit = hit.to_dict()

            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )

            self_power  = 0
            if hit["proxy"] == "1.2.5":
                self_power = int( hit["stake"] )
            self_powers.append( int( self_power / 100000 ) )

            total_power = 0
            for proxee in hit["proxy_for"]:
                total_power += int( proxee[1] )
            total_powers.append( int( total_power / 100000 ) )

    ret = {
        "account": account,
        "blocks": blocks,
        "block_time": block_time,
        "self_powers": self_powers,
        "total_powers": total_powers
    }

    ret = jsonify( ret )
    account_data_self_total_cache.add( account, datapoints, from_date, to_date, ret )

    return ret

@app.route('/get_account_power_self_proxies')
def get_account_power_self_proxies(): # return all proxies with given
    print( "REQUEST RECEIVED" )
    global account_data_self_proxy_cache

    from_date = request.args.get( "from", "2018-01-01T00:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-15T00:00:00" )   # date after past date
    account   = request.args.get( "account", "1.2.285" )
    datapoints = int( request.args.get( "datapoints", 100 ) )
    if datapoints > 700:
        datapoints = 700

    cached_data = account_data_self_proxy_cache.get( account, datapoints, from_date, to_date )
    if cached_data != None:
        return cached_data

    blocks = []
    block_time = []
    self_powers = []
    proxy_powers = {}
    block_counter = 0


    time_intervals = make_equal_intervals( from_date, to_date, datapoints )
    for i in range( 1, len(time_intervals) ):

        from_date_to_search = time_intervals[i-1]
        to_date_to_search = time_intervals[i]

        print( "Searching from: ", from_date_to_search, " to: ", to_date_to_search )

        req = Search( using=es, index="objects-voting-statistics", extra={ "size": 1 } ) # size: max return size
        req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
        req = req.sort("-block_number") # sort by blocknumber => newest block is first hit

        qaccount = Q( "match", account=account )
        qrange = Q( "range", block_time={ "gte": from_date_to_search, "lte": to_date_to_search} )
        req.query = qaccount & qrange
        response = req.execute()

        # generate json in form of
        # {
        #    blocks: [ 1, 2, 3, 4, 5 ]
        #    self_power: [ 0, 2, 0, 4, 5 ]
        #    proxies: // proxies should be sorted by last item in arr
        #    [
        #        [
        #           "1.2.15", [ 1, 2, 3, 4, 5 ]
        #        ],
        #        [
        #           "1.2.30", [ 6, 7, 8, 9, 10 ]
        #        ]
        #    ]
        # }

        for hit in response:
            hit = hit.to_dict()

            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )

            self_power = 0
            if hit["proxy"] == "1.2.5":
                self_power = int( int( hit["stake"] ) / 100000 )
            self_powers.append( self_power )

            for proxee in hit["proxy_for"]:
                proxy_name = proxee[0]
                proxy_power = int( int( proxee[1] ) / 100000 )
                if proxy_name in proxy_powers:
                    # proxy_name = key => ret arr of powers => [block_counter] == power at that block
                    proxy_powers[proxy_name][block_counter] = proxy_power
                else:
                    proxy_powers[proxy_name] = [0] * datapoints
                    proxy_powers[proxy_name][block_counter] = proxy_power

            block_counter += 1



    size_successful_queries = len(self_powers)
    size_not_found_elements = datapoints - size_successful_queries
    index_to_delete_begin   = datapoints - size_not_found_elements
    index_to_delete_end     = datapoints

    del self_powers[index_to_delete_begin:index_to_delete_end]
    for proxy_name, proxy_power in proxy_powers.items():
        del proxy_power[index_to_delete_begin:index_to_delete_end]


    if size_successful_queries == 0:
        return {
            "account":      account,
            "blocks":       [],
            "block_time":   [],
            "self_powers":  [],
            "proxy_powers": []
        }


    last_block = size_successful_queries - 1
    total_power_last_block = self_powers[last_block]
    for proxy_power in proxy_powers.values():
        total_power_last_block += proxy_power[last_block]

    merge_below_perc = 0.05
    merge_below = merge_below_perc * total_power_last_block

    merged_proxies = [0] * size_successful_queries
    proxies_to_delete = []
    for proxy_name, proxy_power in proxy_powers.items():
        if proxy_power[last_block] < merge_below:
            for i in range( size_successful_queries ):
                merged_proxies[i] += proxy_power[i]
            proxies_to_delete.append( proxy_name )

    proxy_powers["< 5%"] = merged_proxies

    for proxy_name in proxies_to_delete:
        del proxy_powers[proxy_name]

    # sort all proxies by last item size
    def sort_by_last_item_asc( proxied_powers ):
        return proxied_powers[1][-1]

    # convert to list to be sortable
    proxy_powers_list = [ [k,v] for k, v in proxy_powers.items() ]
    proxy_powers_list.sort( key = sort_by_last_item_asc )

    ret = {
        "account":      account,
        "blocks":       blocks,
        "block_time":   block_time,
        "self_powers":  self_powers,
        "proxy_powers": proxy_powers_list
    }

    ret = jsonify( ret )
    account_data_self_proxy_cache.add( account, datapoints, from_date, to_date, ret )

    return ret

@app.route('/get_voteable_votes_total')
def get_voteable_powers(): # returns the voting power of a worker over time with each account voted for him
    print( "REQUEST RECEIVED" )
    global voteable_votes_cache

    from_date = request.args.get( "from", "2019-01-01T00:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-15T00:00:00" )   # date after past date
    vote_id   = request.args.get( "vote_id", "2:614" )
    datapoints = int( request.args.get( "datapoints", 100 ) )
    if datapoints > 700:
        datapoints = 700

    cached_data = voteable_votes_cache.get( vote_id, datapoints, from_date, to_date )
    if cached_data != None:
        return cached_data

    blocks      = []
    block_time  = []
    total_votes = []


    time_intervals = make_equal_intervals( from_date, to_date, datapoints )
    for i in range( 1, len(time_intervals) ):

        from_date_to_search = time_intervals[i-1]
        to_date_to_search = time_intervals[i]

        print( "Searching from: ", from_date_to_search, " to: ", to_date_to_search )

        req = Search( using=es, index="objects-voteable-statistics", extra={ "size": 1 } ) # size: max return size
        req = req.source( ["vote_id", "block_time", "block_number", "voted_by"] ) # retrive only this attributes
        req = req.sort("-block_number") # sort by blocknumber => newest block is first hit

        qvote_id = Q( "match_phrase", vote_id=vote_id )
        qrange = Q( "range", block_time={ "gte": from_date_to_search, "lte": to_date_to_search} )
        req.query = qvote_id & qrange
        #pprint( req.to_dict() )
        response = req.execute()

        # generate json in form of
        # {
        #    blocks: [ 1, 2, 3, 4, 5 ]
        #    voteable: [ 0, 2, 0, 4, 5 ]
        #    proxies: // proxies should be sorted by last item in arr
        #    [
        #        [
        #           "1.2.15", [ 1, 2, 3, 4, 5 ]
        #        ],
        #        [
        #           "1.2.30", [ 6, 7, 8, 9, 10 ]
        #        ]
        #    ]
        # }

        for hit in response:
            hit = hit.to_dict()
            #pprint( hit )
            blocks.append( hit["block_number"] )
            block_time.append( hit["block_time"] )

            total = 0
            for account_to_proxeed in hit["voted_by"]:
                total += int(account_to_proxeed[1])
            total_votes.append( int(total / 100000) )
            #block_counter += 1

    #size_successful_queries = len(blocks)
    #size_not_found_elements = datapoints - size_successful_queries
    #index_to_delete_begin = datapoints - size_not_found_elements
    #index_to_delete_end = datapoints

    #del self_powers[index_to_delete_begin:index_to_delete_end]
    #for proxy_name, proxy_power in proxy_powers.items():
    #    del proxy_power[index_to_delete_begin:index_to_delete_end]


    #last_block = size_successful_queries - 1
    #total_power_last_block = self_powers[last_block]
    #for proxy_power in proxy_powers.values():
    #    total_power_last_block += proxy_power[last_block]

    #merge_below_perc = 0.05
    #merge_below = merge_below_perc * total_power_last_block

    #merged_proxies = [0] * size_successful_queries
    #proxies_to_delete = []
    #for proxy_name, proxy_power in proxy_powers.items():
    #    if proxy_power[last_block] < merge_below:
    #        for i in range( size_successful_queries ):
    #            merged_proxies[i] += proxy_power[i]
    #        proxies_to_delete.append( proxy_name )


    #proxy_powers["< 5%"] = merged_proxies


    #for proxy_name in proxies_to_delete:
    #    del proxy_powers[proxy_name]

    # sort all proxies by last item size
    #def sort_by_last_item_asc( proxied_powers ):
    #    return proxied_powers[1][-1]

    # convert to list to be sortable
    #proxy_powers_list = [ [k,v] for k, v in proxy_powers.items() ]
    #proxy_powers_list.sort( key = sort_by_last_item_asc )

    ret = {
        "vote_id":      vote_id,
        "blocks":       blocks,
        "block_time":   block_time,
        "total_votes":  total_votes,
        #"proxy_powers": proxy_powers_list
    }

    ret = jsonify( ret )
    account_data_self_proxy_cache.add( vote_id, datapoints, from_date, to_date, ret )

    return ret

@app.route('/get_voted_workers_over_time')
def get_voted_workers_over_time(): # returns worker voted by this account over time

    #from_      = request.args.get('from', ) # current_time
    #to_        = request.args.get('to', ) # 2 years back
    account = request.args.get( "account", "1.2.18" ) # account_id

    s = Search( using=es, index="objects-voting-statistics" )
    s.query = Q( "match", account=account )

    response = s.execute()
    json = SortedList(key=lambda json: json["block_number"])
    for hit in response:
        hit = hit.to_dict()
        del hit["object_id"]
        del hit["id"]
        stake = 0
        if hit["proxy"] == "1.2.5":
            stake += int( hit["stake"] )
        for proxeed in hit["proxy_for"]:
            stake += int( proxeed[1] )
        hit["sum_stake"] = stake
        del hit["proxy"]
        del hit["stake"]
        del hit["proxy_for"]
        json.add(hit)

    return jsonify( list(json) )



if __name__ == '__main__':
    app.run( debug=True, port=5000 )

