import inspect
from Meta import pggenericfunc
from Data.Connect import pgdatabase


_SQL1 = '''select * from
    (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) as pg_time_n LEFT JOIN
    (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n_minus_1 where date_rank = (select count(1) from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) + 1) as pg_time_n_minus_1 
    on pg_time_n.pg_crypto_ticker = pg_time_n_minus_1.pg_crypto_ticker where cast(pg_time_n.pg_crypto_price as float) > 1 * cast(pg_time_n_minus_1.pg_crypto_price as float)
    '''

@pgdatabase.db_session('mysql')
def run(logger=None, db_session=None, ):
    _db_query = _SQL1
    try:
        return db_session.simple_query(_db_query)
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
    return None


def main():
    run()

if __name__ == "__main__":
    print(main())