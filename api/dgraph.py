#!/usr/bin/env python3
import os
import json
import socket
import requests
import pydgraph

# Set the hostname of the Dgraph alpha service
host = "localhost"

error_events = ["StartFailed", "NegotiateFailed", "TakerFeeSendFailed",
                "MakerPaymentValidateFailed", "MakerPaymentWaitConfirmFailed",
                "TakerPaymentTransactionFailed", "TakerPaymentWaitConfirmFailed",
                "TakerPaymentDataSendFailed", "TakerPaymentWaitForSpendFailed",
                "MakerPaymentSpendFailed", "TakerPaymentWaitRefundStarted",
                "TakerPaymentRefunded", "TakerPaymentRefundFailed",
                "TakerFeeValidateFailed", "MakerPaymentTransactionFailed",
                "MakerPaymentDataSendFailed",  "TakerPaymentValidateFailed",
                "TakerPaymentSpendFailed", "TakerPaymentSpendConfirmFailed",
                "MakerPaymentWaitRefundStarted", "MakerPaymentRefunded",
                "MakerPaymentRefundFailed"
]

class DgraphClient:
    def __init__(self, admin: bool = False):
        self.admin = admin
        self.http_port = 8080
        self.grpc_port = 9080
        self.zero_port = 5080

    def client_stub(self):
        """
        Returns a pydgraph client stub
        """
        return pydgraph.DgraphClientStub(
            addr=f"{host}:{self.grpc_port}",
            options=[('grpc.max_receive_message_length', 1024*1024*1024)]
        )

    def create_schema(self, schema: str):
        """
        # Deploy the GraphQL Schema to Dgraph
        """
        if not self.admin:
            return
        with open(schema, "r") as f:
            schema = f.read()
        stub = self.client_stub()
        client = pydgraph.DgraphClient(stub)
        response = client.alter(pydgraph.Operation(schema=schema))
        stub.close()
        return response

    def check_port(self, url, port):
        """
        check_port returns true if the port at the url is accepting connections
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)  # Set a timeout value for the connection attempt
            result = sock.connect_ex((url, port))
            sock.close()
            if result == 0:
                return True
            else:
                return False
        except socket.error:
            return False

    def healthcheck(self):
        """
        healthcheck returns true if the Dgraph cluster is healthy
        """
        if not self.check_port(host, self.http_port):
            raise Exception(f"Port {self.http_port} at {host} not responding, is the server running?")
        if not self.check_port(host, self.grpc_port):
            raise Exception(f"Port {self.grpc_port} at {host} not responding, is the server running?")
        if not self.check_port(host, self.zero_port):
            raise Exception(f"Port {self.zero_port} at {host} not responding, is the server running?")

    def drop(self):
        stub = self.client_stub()
        client = pydgraph.DgraphClient(stub)
        response = client.alter(pydgraph.Operation(drop_all=True))
        stub.close()
        return response
    
    def set_coin(self, ticker: str, testnet: bool = False):
        return {
            "uid": "_:coin",
            "dgraph.type": "Coin",
            "ticker": ticker,
            "testnet": testnet
        }
    
    def set_swap(
        self, swap_uuid: str, events=list, transactions=list,
        result=str, started_at=int, ended_at=int, maker=None,
        taker=None
    ):
        data = {
            "dgraph.type": "Swap",
            "uid": "uid(v)",
            "uuid": swap_uuid,
            "events": events,
            "transactions": transactions,
            "result": result,
            "started_at": started_at,
            "ended_at": ended_at
        }
        if maker is not None:
            data.update({"maker": maker})
        if taker is not None:
            data.update({"taker": taker})
        return data
    
    def set_maker(self, pubkey: dict, gui: str, sdk: str, coin: str):
        return {
            "uid": "_:maker",
            "swap": "uid(v)",
            "dgraph.type": "Maker",
            "pubkey": pubkey,
            "coin": coin,
            "gui": gui,
            "sdk": sdk
        }
    
    def set_taker(self, pubkey: dict, gui: str, sdk: str, coin: str):
        return { 
            "uid": "_:taker",
            "swap": "uid(v)",
            "dgraph.type": "Taker",
            "pubkey": pubkey,
            "coin": coin,
            "gui": gui,
            "sdk": sdk
        }
    
    def set_pubkey(self, pubkey: str):
        return {
            "uid": "_:pubkey",
            "dgraph.type": "Pubkey",
            "name": pubkey
        }

    def set_sdk(self, version: str):
        return {
            "uid": "_:sdk",
            "dgraph.type": "Sdk",
            "version": version
        }
        
    def set_price(self, timestamp: int, price: float):
        return {
            "uid": "_:spotprice",
            "dgraph.type": "SpotPrice",
            "price_timestamp": timestamp,
            "price": price
        }

    def set_gui(self, gui: str):
        return {
            "uid": "_:gui",
            "dgraph.type": "Gui",
            "gui_name": gui
        }

    def set_swap_event(self, event: str, timestamp: int):
        return {
            "uid": "_:swapevent",
            "swap": "uid(v)",
            "dgraph.type": "SwapEvent",
            "event_name": event,
            "timestamp": timestamp
        }
    
    def set_transaction(self, txid: str, tx_type: str, amount: str, usd_price: dict, coin: str):
        return {
            "uid": "_:transaction",
            "swap": "uid(v)",
            "dgraph.type": "Transaction",
            "usd_price": usd_price,
            "txid": txid,
            "coin": coin,
            "tx_type": tx_type,
            "amount": amount
        }

    def set_fee(self, coin: str, amount: str, paid_from_trading_vol: bool, usd_price: dict, pubkey:dict, fee_type: str):
        return {
            "uid": "_:fee",
            "swap": "uid(v)",
            "dgraph.type": "Fee",
            "fee_type": fee_type,
            "coin": coin,
            "usd_price": usd_price,
            "pubkey": pubkey,
            "amount": amount,
            "paid_from_trading_vol": paid_from_trading_vol
        }

    def is_paid_from_trading_vol(self, fee: dict):
        if "paid_from_trading_vol" not in fee:
            return False
        elif fee["paid_from_trading_vol"] is None:
            return False
        else:
            return fee["paid_from_trading_vol"]
    
    def get_swap_type(self, swap_data: dict, swap_type):
        if "type" in swap_data:
            return swap_data['type']
        else:            
            return swap_type.title()
    
    def get_usd_price(self, swap_data, key):
        if key in swap_data:
            return swap_data[key]
        else:
            return 0
    
    def get_result(self, events):
        result = "Incomplete"
        for i in events:
            event_type = i['event']['type']            
            if event_type in ["TakerPaymentSpendConfirmed", "MakerPaymentSpent"]:
                result = "Success"
            elif event_type in ["TakerPaymentRefunded", "MakerPaymentRefunded"]:
                result = "Refunded"
            elif event_type in ["TakerPaymentWaitRefundStarted", "MakerPaymentWaitRefundStarted"]:
                result = "Awaiting Refund"
            elif event_type in error_events:
                result = "Failed"
        return result
        
    def get_tx_data(self, event_data, taker_coin, maker_coin, taker_amount=0, maker_amount=0):
        event_type = event_data['type']
        if 'data' in event_data:
            if event_data['data']:
                if 'tx_hash' in event_data['data']:
                    # These are duplicated txids
                    if event_type not in [
                        "MakerPaymentReceived",
                        "TakerFeeValidated",
                        "TakerPaymentReceived"
                    ]:
                        print(event_data['data']['tx_hash'])
                        if event_type in ["MakerPaymentSent", "MakerPaymentRefunded", "MakerPaymentSpent"]:
                            amount = maker_amount
                        elif event_type in ["TakerPaymentSpent", "TakerPaymentSent", "TakerPaymentRefunded"]:
                            amount = taker_amount
                        elif event_type == "TakerFeeSent":
                            amount = 0
                        else:
                            input(event_type)
                            amount = 0
                        tx = {
                            "uid": "_:transaction",
                            "swap": "uid(v)",
                            "dgraph.type": "Transaction",
                            "txid": event_data['data']['tx_hash'],
                            "tx_type": event_type,
                            "amount": amount,
                        }
                        if event_type in [
                            "TakerFeeSent",
                            "TakerPaymentSent",
                            "TakerPaymentSpent"
                        ]:
                            tx.update({
                                "coin": taker_coin,
                            })
                        elif event_type in [
                            "MakerPaymentSent",
                            "MakerPaymentSpent"
                        ]:
                            tx.update({
                                "coin": maker_coin,
                            })
                        print(tx)
                        return tx
    
    def update_swap(self, swap_data: dict, swap_type):
        swap_uuid = swap_data['uuid']
        swap_type = swap_type.title()
        maker_coin = self.set_coin(swap_data['maker_coin'])
        taker_coin = self.set_coin(swap_data['taker_coin'])
        maker_coin_volume = swap_data['maker_amount']
        taker_coin_volume = swap_data['taker_amount']
        result = self.get_result(swap_data['events'])
        # Some older json files don't have these keys
        receive_fee = None
        send_fee = None
        dex_fee = None
        maker_payment_lock = None

        # Events loop
        ended_at = 0
        events = []
        transactions = []
        for i in swap_data['events']:
            event_type = i['event']['type']
            event = self.set_swap_event(event_type, i['timestamp'])
            events.append(event)
            ended_at = i['timestamp'] if i['timestamp'] > ended_at else ended_at
            if ended_at > 123581321345:
                ended_at = int(ended_at / 1000)
            if event_type == "StartFailed":
                started_at = i['timestamp']
                
            if event_type == "Started":
                started_at = i['event']['data']['started_at']
                maker_coin_usd = self.set_price(
                    started_at,
                    self.get_usd_price(swap_data, "maker_coin_usd_price")
                )
                taker_coin_usd = self.set_price(
                    started_at,
                    self.get_usd_price(swap_data, "taker_coin_usd_price")
                )
                if swap_type == 'Maker':
                    maker_pubkey = self.set_pubkey(i['event']['data']['my_persistent_pub'])
                    maker_payment_lock = i['event']['data']['maker_payment_lock']
                    maker_confs = i['event']['data']['maker_payment_confirmations']
                    maker_nota = i['event']['data']['maker_payment_requires_nota']
                    
                    # Fees
                    if 'maker_payment_trade_fee' in i['event']['data']:
                        maker_send_fee = i['event']['data']['maker_payment_trade_fee']
                        if maker_send_fee is not None:
                            if maker_send_fee["coin"] == maker_coin:
                                usd_price = maker_coin_usd
                            else:
                                usd_price = None
                            send_fee = self.set_fee(
                                coin=self.set_coin(maker_send_fee["coin"]),
                                amount=maker_send_fee["amount"],
                                paid_from_trading_vol=self.is_paid_from_trading_vol(maker_send_fee),
                                usd_price=self.set_price(i['timestamp'], usd_price),
                                pubkey=maker_pubkey,
                                fee_type="MakerSendFee"
                            )
                        if 'taker_payment_spend_trade_fee' in i['event']['data']:
                            maker_receive_fee = i['event']['data']['taker_payment_spend_trade_fee']
                            if maker_receive_fee is not None:
                                if maker_receive_fee["coin"] == maker_coin:
                                    usd_price = maker_coin_usd
                                else:
                                    usd_price = None
                                receive_fee = self.set_fee(
                                    coin=self.set_coin(maker_receive_fee["coin"]),
                                    amount=maker_receive_fee["amount"],
                                    paid_from_trading_vol=self.is_paid_from_trading_vol(maker_receive_fee),
                                    usd_price=self.set_price(i['timestamp'], usd_price),
                                    pubkey=maker_pubkey,
                                    fee_type="MakerReceiveFee"
                                )

                elif swap_type == 'Taker':
                    taker_pubkey = self.set_pubkey(i['event']['data']['my_persistent_pub'])
                    taker_payment_lock = i['event']['data']['taker_payment_lock']
                    taker_confs = i['event']['data']['taker_payment_confirmations']
                    taker_nota = i['event']['data']['taker_payment_requires_nota']
                    
                    # Fees
                    if 'fee_to_send_taker_fee' in i['event']['data']:
                        taker_dex_fee = i['event']['data']['fee_to_send_taker_fee']
                        if taker_dex_fee is not None:
                            if taker_dex_fee["coin"] == taker_coin:
                                usd_price = taker_coin_usd
                            else:
                                usd_price = None
                            dex_fee = self.set_fee(
                                coin=self.set_coin(taker_dex_fee["coin"]),
                                amount=taker_dex_fee["amount"],
                                paid_from_trading_vol=self.is_paid_from_trading_vol(taker_dex_fee),
                                usd_price=self.set_price(i['timestamp'], usd_price),
                                pubkey=taker_pubkey,
                                fee_type="TakerDexFee"
                            )

                    if 'taker_payment_trade_fee' in i['event']['data']:
                        taker_send_fee = i['event']['data']['taker_payment_trade_fee']
                        if taker_send_fee is not None:
                            if taker_send_fee["coin"] == taker_coin:
                                usd_price = taker_coin_usd
                            else:
                                usd_price = None
                            send_fee = self.set_fee(
                                coin=self.set_coin(taker_send_fee["coin"]),
                                amount=taker_send_fee["amount"],
                                paid_from_trading_vol=self.is_paid_from_trading_vol(taker_send_fee),
                                usd_price=self.set_price(i['timestamp'], usd_price),
                                pubkey=taker_pubkey,
                                fee_type="TakerSendFee"
                            )

                    if 'maker_payment_spend_trade_fee' in i['event']['data']:
                        taker_receive_fee = i['event']['data']['maker_payment_spend_trade_fee']
                        if taker_receive_fee is not None:
                            if taker_receive_fee["coin"] == taker_coin:
                                usd_price = taker_coin_usd
                            else:
                                usd_price = None
                            receive_fee = self.set_fee(
                                coin=self.set_coin(taker_receive_fee["coin"]),
                                amount=taker_receive_fee["amount"],
                                paid_from_trading_vol=self.is_paid_from_trading_vol(taker_receive_fee),
                                usd_price=self.set_price(i['timestamp'], usd_price),
                                pubkey=taker_pubkey,
                                fee_type="TakerReceiveFee"
                            )
            # Transactions
            tx = self.get_tx_data(i['event'], taker_coin, maker_coin, taker_coin_volume, maker_coin_volume)
            if tx is not None:
                transactions.append(tx)
        
        maker = None
        taker = None
        # todo add success_event and fail_event
        if result == "Failed":
            maker_pubkey = None
            taker_pubkey = None
            maker_payment_lock = None
            taker_payment_lock = None
            maker_coin_usd = None
            taker_coin_usd = None
            maker_confs = None
            maker_nota = None
            receive_fee = None
            send_fee = None
            dex_fee = None
            
        if swap_type == 'Maker':
            maker = {
                "swap": "uid(v)",
                "uid": "_:maker",
                "dgraph.type": "Maker",
                "payment_lock": maker_payment_lock,
                "confs": maker_confs,
                "nota": maker_nota,
                "pubkey": maker_pubkey,
                "coin": maker_coin,
                "volume": maker_coin_volume,
                "usd_price": maker_coin_usd,
                "send_fee": send_fee,
                "receive_fee": receive_fee,
                "gui": swap_data['gui'],
                "version": swap_data['mm_version'],
            }

        elif swap_type == 'Taker':
            taker = {
                "swap": "uid(v)",
                "dgraph.type": "Taker",
                "uid": "_:taker",
                "payment_lock": taker_payment_lock,
                "confs": taker_confs,
                "nota": taker_nota,
                "pubkey": taker_pubkey,
                "coin": taker_coin,
                "volume": taker_coin_volume,
                "dex_fee": dex_fee,
                "usd_price": taker_coin_usd,
                "send_fee": send_fee,
                "receive_fee": receive_fee,
                "gui": swap_data['gui'],
                "version": swap_data['mm_version']
            }

        data = self.set_swap(swap_uuid,
            events, transactions,
            result, started_at, ended_at,
            maker, taker
        )
        print(json.dumps(data, indent=4))
        query = f'{{ q(func: eq(uuid, "{swap_uuid}")) {{ \
                v as uid \
            }} \
        }}'
        print(query)
        mutations = [ {"set": [data]} ]
        return self.upsert_request(query=query, mutations=mutations)
            
    def upsert(self, set_data: dict):
        stub = self.client_stub()
        client = pydgraph.DgraphClient(stub)
        txn = client.txn()
        try:
            response = txn.mutate(set_obj=set_data)
            txn.commit()
        finally:
            txn.discard()
        stub.close()
        return response

    def upsert_request(self, query: str, mutations: dict):
        upsert = {'query': query, "mutations": mutations}
        endpoint = "http://localhost:8080/mutate?commitNow=true"
        return requests.post(endpoint, json=upsert).json()
        

    def query(self):
        q = """{
            showallnodes(func: has(dgraph.type)){
                dgraph.type
                expand(_all_)
            }
        }"""

        v = {'$a': 'KMD'}

        stub = self.client_stub()
        client = pydgraph.DgraphClient(stub)
        txn = client.txn()
        try:
            response = client.txn(read_only=True).query(q, variables=v)
        finally:
            txn.discard()
        stub.close()
        return response

if __name__ == "__main__":
    d = DgraphClient()
    d.healthcheck()
    d_admin = DgraphClient(admin=True)
    d_admin.drop()
    d_admin.create_schema("schema.dgraphql")
    for i in ["MAKER", "TAKER"]:
        path = f"/home/smk762/.atomic_qt/mm2/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/SWAPS/STATS/{i}"
        for file in os.listdir(path):
            if file.endswith(".json"):
                    print(f"Inputing {i}/{file}")
                    data = json.load(open(f"{path}/{file}", "r"))
                    print(d_admin.update_swap(data, i))
    #print(d.query())
    
    
'''{
  foo(func:has(PREDICATE_NAME_HERE)) {
    uid,
    expand(_all_) { 
      expand(_all_)
    } 
  }
}
'''