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
import array

es = Elasticsearch( ["https://BitShares:Infrastructure@elasticsearch.bitshares.ws:443"], timeout=60 )
#es = Elasticsearch( timeout=60 )

def get_account_power_over_time():

   from_date = "2019-04-06T15:00:00" # past date
   to_date   = "2019-07-13T06:00:00" # date after past date
   account   = "1.2.285" # account_id

   request = Search( using=es, index="objects-voting-statistics", extra={"size": 10000} ) # return size 1k
   request = request.sort("-block_number") # sort by blocknumber => last_block is first hit
   request = request.source( ["account", "stake", "proxy", "proxy_for", "block_time", "block_number"] ) # retrive only this attributes
   
   qaccount = Q( "match", account=account ) # match account
   qrange = Q( "range", block_time={ "gte": from_date, "lte": to_date} ) # match date range
   request.query = qaccount & qrange # combine queries

   response = request.execute()
   print( type(response) )
   print( len(response) )
   
   while(True): pass
   result = []
   for hit in reversed(response):
      hit = hit.to_dict()
      total_stake = 0 
      if hit["proxy"] == "1.2.5":
         total_stake += int( hit["stake"] )
      for proxeed in hit["proxy_for"]:
         total_stake += int( proxeed[1] )
      hit["total_stake"] = total_stake
      result.append( hit )
   
   print( json.dumps(result) )
   return json.dumps(result)

def get_account_power_over_time_proxies():

    from_date = "2019-01-06T15:00:00"  # past date
    to_date   = "2019-07-13T06:00:00"  # date after past date
    account   = "1.2.285"  # account_id

    req = Search( using=es, index="objects-voting-statistics", extra={"size": 100} ) # return size 1k
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
    #    proxies: {
    #        "1.2.15": [],
    #        "1.2.30": []
    #    }
    # }

    blocks = []
    self_powers = []
    proxy_powers = {}
    
    block_counter = 0
    num_blocks = len(response)
    for hit in reversed(response):

        hit = hit.to_dict()

        blocks.append( hit["block_number"] )

        self_power  = 0
        if hit["proxy"] == "1.2.5":
            self_power = int( hit["stake"] )
        self_powers.append( self_power )

        for proxee in hit["proxy_for"]:
            proxy_name = proxee[0]
            proxy_power = int( proxee[1] )
            if proxy_name in proxy_powers:
                # proxy_name = key => ret arr of powers => [block_counter] == power at that block else will be null
                proxy_powers[proxy_name][block_counter] = proxy_power
            else:
                proxy_powers[proxy_name] = [0] * num_blocks
                proxy_powers[proxy_name][block_counter] = proxy_power
        
        block_counter += 1

    ret = {
        "account": account,
        "blocks": blocks,
        "self_powers": self_powers,
        "proxy_powers": proxy_powers
    }
    print( json.dumps( ret ) )

def get_account_power_over_time_proxies_time_split():

    from_date = "2018-01-06T15:00:00"
    to_date   = "2019-07-13T06:00:00"
    account   = "1.2.285" 

    conv_from = datetime.strptime( from_date, "%Y-%m-%dT%H:%M:%S" )
    conv_to = datetime.strptime( to_date, "%Y-%m-%dT%H:%M:%S" )
    print( conv_from )
    print( conv_to )
    
    from_to_delta_days = (conv_to - conv_from).days
    num_queries =  5
    time_window_days = from_to_delta_days / num_queries

    print( "From: " + str(conv_from) )
    for i in range( num_queries ):
        conv_from += timedelta( days=time_window_days )
        print( "From: " + conv_from.strftime("%Y-%m-%dT%H:%M:%S") )




    return

    req = Search( using=es, index="objects-voting-statistics", extra={ "size": 300 } ) # size: max return size
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

if __name__ == '__main__':
   #get_account_power_over_time()
   get_account_power_over_time_proxies_time_split()
