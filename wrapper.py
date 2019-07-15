from flask import Flask
from flask import jsonify
from flask import request

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, query, Q, DocType, utils
from elasticsearch_dsl.exceptions import IllegalOperation

from flasgger import Swagger
from sortedcontainers import SortedList

import json

es = Elasticsearch( ["https://BitShares:Infrastructure@elasticsearch.bitshares.ws:443"], timeout=60 )
#es = Elasticsearch( timeout=60 )


app = Flask(__name__)
from flask_cors import CORS, cross_origin
CORS(app)

app.config['SWAGGER'] = {
    'title': 'Bitshares ES API',
    'uiversion': 2
}
Swagger(app, template_file='wrapper.yaml')

class QueryCache:
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
        stored = self.storage[account]
        stored["datapoints"] = datapoints
        stored["from_date"] = from_date
        stored["to_date"] = to_date
        stored["data"] = data

    def get( self, account ):
        return self.storage[account]["data"]

    def contains( self, account, datapoints, from_date, to_date):
        if account not in self.storage:
            return False
        
        stored = self.storage[account]
        if stored["from_date"] != from_date \
            or stored["to_date"] != to_date \
            or stored["datapoints"] != datapoints:
                return False

        return True


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

@app.route('/get_account_power_self_total')
def get_account_power_self_total(): # return time to power to proxied power
    print( "REQUEST RECEIVED" )

    print( "GLOBAL_CACHE", global_cache )

    from_date = request.args.get( "from", "2018-01-01T00:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-15T00:00:00" ) # date after past date
    account   = request.args.get( "account", "1.2.285" ) # account_id
    datapoints = int( request.args.get( "datapoints", 100 ) )
    if datapoints > 700: 
        datapoints = 700

    global query_cache
    write_to_cache = True
    if query_cache.contains( account, datapoints, from_date, to_date ):
        write_to_cache = False

    datetime_format = "%Y-%m-%dT%H:%M:%S"
    datetime_from   = datetime.strptime( from_date, datetime_format )
    datetime_to     = datetime.strptime( to_date, datetime_format )
    
    from_to_delta_days = ( datetime_to - datetime_from ).days    
    time_window_days = from_to_delta_days / datapoints

    blocks = []
    self_powers = []
    total_powers = []

    # dec once to have inc at begin
    datetime_from -= timedelta( days=time_window_days )
    for i in range( datapoints ):
        
        datetime_from += timedelta( days=time_window_days )
        datetime_to = datetime_from + timedelta( days=time_window_days )
        
        from_date = datetime_from.strftime( datetime_format )
        to_date = datetime_to.strftime( datetime_format )

        print( "Searching from: ", from_date, " to: ", to_date )

        req = Search( using=es, index="objects-voting-statistics", extra={"size": 1} ) # return size 1k
        req = req.sort("-block_number") # sort by blocknumber => last_block is first hit
        req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
        
        qaccount = Q( "match", account=account ) # match account
        qrange = Q( "range", block_time={ "gte": from_date, "lte": to_date} ) # match date range
        req.query = qaccount & qrange # combine queries

        response = req.execute()

        # generate json in form of 
        # {
        #    blocks: [ 1, 2, 3, 4, 5 ]
        #    self_power: [ 0, 2, 0, 4, 5 ]
        #    total_power: [100, 200, 300, 400, 500 ]
        # }

    
        for hit in response:
            hit = hit.to_dict()

            blocks.append( hit["block_number"] )

            self_power  = 0
            if hit["proxy"] == "1.2.5":
                self_power = int( hit["stake"] )
            self_powers.append( self_power )

            total_power = 0     
            for proxee in hit["proxy_for"]:
                total_power += int( proxee[1] )
            total_powers.append( total_power )

    ret = {
        "account": account,
        "blocks": blocks,
        "self_powers": self_powers,
        "total_powers": total_powers
    }

    return jsonify( ret )


@app.route('/get_account_power_self_proxies')
def get_account_power_self_proxies(): # return all proxies with given 
    print( "REQUEST RECEIVED" )

    from_date = request.args.get( "from", "2018-01-01T00:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-15T00:00:00" ) # date after past date
    account   = request.args.get( "account", "1.2.285" ) # account_id
    datapoints = int( request.args.get( "datapoints", 100 ) )
    if datapoints > 700: 
        datapoints = 700

    datetime_format = "%Y-%m-%dT%H:%M:%S"
    datetime_from   = datetime.strptime( from_date, datetime_format )
    datetime_to     = datetime.strptime( to_date, datetime_format )
    
    from_to_delta_days = ( datetime_to - datetime_from ).days
    
    time_window_days = from_to_delta_days / datapoints

    blocks = []
    self_powers = []
    proxy_powers = {}
    block_counter = 0

    # dec once to have inc at begin
    datetime_from -= timedelta( days=time_window_days )
    for i in range( datapoints ):
        
        datetime_from += timedelta( days=time_window_days )
        datetime_to = datetime_from + timedelta( days=time_window_days )
        
        from_date = datetime_from.strftime( datetime_format )
        to_date = datetime_to.strftime( datetime_format )

        print( "Searching from: ", from_date, " to: ", to_date )

        req = Search( using=es, index="objects-voting-statistics", extra={ "size": 1 } ) # size: max return size
        req = req.sort("-block_number") # sort by blocknumber => newest block is first hit 
        req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
        
        qaccount = Q( "match", account=account ) 
        qrange = Q( "range", block_time={ "gte": from_date, "lte": to_date} ) 
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

            self_power = 0
            if hit["proxy"] == "1.2.5":
                self_power = int( int( hit["stake"] ) / 1000000 )
            self_powers.append( self_power )

            for proxee in hit["proxy_for"]:
                proxy_name = proxee[0]
                proxy_power = int( int( proxee[1] ) / 1000000 )
                if proxy_name in proxy_powers:
                    # proxy_name = key => ret arr of powers => [block_counter] == power at that block
                    proxy_powers[proxy_name][block_counter] = proxy_power
                else:
                    proxy_powers[proxy_name] = [0] * datapoints
                    proxy_powers[proxy_name][block_counter] = proxy_power

            block_counter += 1

    size_successful_queries = len(self_powers)
    size_not_found_elements = datapoints - size_successful_queries
    index_to_delete_begin = datapoints - size_not_found_elements
    index_to_delete_end = datapoints
    
    del self_powers[index_to_delete_begin:index_to_delete_end]
    for proxy_name, proxy_power in proxy_powers.items():
        del proxy_power[index_to_delete_begin:index_to_delete_end]


    last_block = size_successful_queries - 1
    total_power_last_block = self_powers[last_block]
    for proxy_name, proxy_power in proxy_powers.items():
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
    proxy_powers_list = []
    for k, v in proxy_powers.items():
        proxy_powers_list.append( [k, v] )
    
    proxy_powers_list.sort( key = sort_by_last_item_asc )

    ret = {
        "account": account,
        "blocks": blocks,
        "self_powers": self_powers,
        "proxy_powers": proxy_powers_list
    }
    return jsonify( ret )


@app.route('/get_account_power_self_proxies_COPY')
def get_account_power_self_proxies_COPY(): # SAFETY COPY

    from_date = request.args.get( "from", "2018-01-06T15:00:00" ) # past date
    to_date   = request.args.get( "to", "2019-07-13T06:00:00" ) # date after past date
    account   = request.args.get( "account", "1.2.285" ) # account_id

    req = Search( using=es, index="objects-voting-statistics", extra={ "size": 300 } ) # size: max return size
    req = req.sort("-block_number") # sort by blocknumber => newest block is first hit
    req = req.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
    
    qaccount = Q( "match", account=account ) 
    qrange = Q( "range", block_time={ "gte": from_date, "lte": to_date} ) 
    req.query = qaccount & qrange 

    response = req.execute()
    
    new_ret = []
    for hit in response:
        new_ret.append( hit.to_dict() )
    return jsonify( new_ret )

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

    blocks = []
    self_powers = []
    proxy_powers = {}
    
    block_counter = 0
    num_blocks = len(response)
    for hit in reversed(response):
        hit = hit.to_dict()

        blocks.append( hit["block_number"] )

        self_power = 0
        if hit["proxy"] == "1.2.5":
            self_power = int( hit["stake"] )
        self_powers.append( self_power )

        for proxee in hit["proxy_for"]:
            proxy_name = proxee[0]
            proxy_power = int( proxee[1] )
            if proxy_name in proxy_powers:
                # proxy_name = key => ret arr of powers => [block_counter] == power at that block
                proxy_powers[proxy_name][block_counter] = proxy_power
            else:
                proxy_powers[proxy_name] = [0] * num_blocks
                proxy_powers[proxy_name][block_counter] = proxy_power
        
        block_counter += 1

    last_block = num_blocks - 1
    
    total_power_last_block = self_powers[last_block]
    for proxy_name, proxy_power in proxy_powers.items():
        total_power_last_block += proxy_power[last_block]

    merge_percentage = 0.05
    merge_below = merge_percentage * total_power_last_block

    merged_proxies = [0] * num_blocks
    proxies_to_delete = []
    for proxy_name, proxy_power in proxy_powers.items():
        if proxy_power[last_block] < merge_below:
            for i in range(num_blocks):
                merged_proxies[i] += proxy_power[i]
            proxies_to_delete.append( proxy_name )

    proxy_powers["< 5%"] = merged_proxies

    for proxy_name in proxies_to_delete:
        del proxy_powers[proxy_name]

    # sort all proxies by last item size
    def sort_by_last_item_asc( proxied_powers ):
        return proxied_powers[1][-1]

    # convert to list to be sortable
    proxy_powers_list = []
    for k, v in proxy_powers.items():
        proxy_powers_list.append( [k, v] )
    
    proxy_powers_list.sort( key = sort_by_last_item_asc )

    ret = {
        "account": account,
        "blocks": blocks,
        "self_powers": self_powers,
        "proxy_powers": proxy_powers_list
    }
    return jsonify( ret )

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

@app.route('/get_worker_power_over_time')
def get_worker_power_over_time(): # returns the voting power of a worker over time with each account voted for him
    #from_      = request.args.get('from', ) # current_time
    #to_        = request.args.get('to', ) # 2 years back
    vote_id = request.args.get('vote_id', '1:0') # vote_id

    s = Search( using=es, index="objects-voting-statistics", extra={"size": 1000} )
    s.query = Q( "match_all" ) #, vote_id=vote_id )

    response = s.execute()
    json = SortedList(key=lambda k: k["block_number"])

    for hit in response: # traverse all hits
        hit = hit.to_dict()
        if vote_id not in hit["votes"]: # ignore htis which not contain our vote_id
            continue 
        
        stake = 0
        for filtered in json:
            if filtered["block_number"] == hit["block_number"]: # when we already have the object extend it
                if hit["proxy"] == "1.2.5": # 1.2.5 == GRAPHENE_PROXY_TO_SELF means no proxy set, hence add the stake
                    stake += hit["stake"]
                for proxied in hit["proxy_for"]: # proxied is account to stake datastructure
                    stake += int(proxied[1])
                
                if stake == 0: # when there is no stake no need to add the proxied stake
                    break
                
                filtered["proxied"].append( {"proxee": hit["account"], "stake": stake} )
                break

        if stake != 0: # can only occur when the vote_id was found in json_filtered hence it was already added so start looking in new hit
            continue

        if hit["proxy"] == "1.2.5":
            stake += hit["stake"]
        for proxied in hit["proxy_for"]:
            stake += int(proxied[1])
        
        if stake == 0: 
            continue
        
        # create a new filtered_obj and add it the list of filtered objects
        filtered_obj =  { 
            "block_number" : hit["block_number"], 
            "block_time": hit["block_time"],
            "proxied" : [ {"proxee": hit["account"], "stake": stake} ] 
        }
        json.add( filtered_obj )

    return jsonify( list(json) )


if __name__ == '__main__':
    app.run( debug=True, port=5000 )

