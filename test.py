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


if __name__ == '__main__':
   #get_account_power_over_time()
   get_account_power_over_time_proxies()
