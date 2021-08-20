import json
from Processing.Template import pgtracker


def pg_get_url():
    _pg_api_key = ["ef53798e3676ea190464e4e8aa099e1f83a005dfd68ea69c46dcb516a6bdc93a",
                   "caf120d0abba96864facdbdeea0f37264202eee384f83832d495102b8cfdd05e"]

    _pg_symbol_per_call = 20
    _pg_price_base_url = "https://min-api.cryptocompare.com/data/pricemulti?"
    # _pg_price_symbol_ticket = "fsym=BTC"
    _pg_price_symbol_ticket = "fsyms="
    # _pg_price_currency = "tsyms=USD,JPY,EUR"
    _pg_price_currency = "tsyms=USD"
    _pg_price_api = f"pi_key={_pg_api_key[0]}"
    _list = []

    with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_tickets.json", "r") as json_file:
        _pg_crypto_ticket = json.load(json_file)

        for _index, _item in enumerate(_pg_crypto_ticket.items(), start=1):
            if _index % _pg_symbol_per_call == 0:
                _list.append(_pg_price_symbol_ticket)
                _pg_price_symbol_ticket = "fsyms="
            else:
                _pg_price_symbol_ticket = f"{_pg_price_symbol_ticket}{_item[0]}" if _index % _pg_symbol_per_call == 1 else f"{_pg_price_symbol_ticket},{_item[0]}"

        return [f"{_pg_price_base_url}{x}&{_pg_price_currency}&{_pg_price_api}" for x in _list]


def crypto_compare_formatter(data):
    return [{"crypto_ticker": _key, "crypto_price": format(_val["USD"], '.10f')} for _key, _val in data.items()]


if __name__ == "__main__":
    pgtracker.PGTracker().run("test2", pg_get_url()[:10], crypto_compare_formatter)
