import itertools
import sqlparse

from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if item.is_group:
            for x in extract_from_part(item):
                yield x
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING']:
                from_seen = False
                StopIteration
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                yield value
        elif isinstance(item, Identifier):
            value = item.value.replace('"', '').lower()
            yield value


def extract_tables(sql):
    # let's handle multiple statements in one sql string
    extracted_tables = []
    statements = list(sqlparse.parse(sql))
    for statement in statements:
        if statement.get_type() != 'UNKNOWN':
            stream = extract_from_part(statement)
            extracted_tables.append(set(list(extract_table_identifiers(stream))))
    return list(itertools.chain(*extracted_tables))


string = "Select a.col1, b.col2 from tb1 as a inner join tb2 as b on tb1.col7 = tb2.col8;"
string1= """SELECT /* Elecena\Models\Product::newFromIds */ prod_id AS id,code,products.name AS name,products.description AS description,producers.producer_id AS producer_id,producers.name AS producer,extra,parameters,url,price AS price_original,price *  currencies.value / (currencies.multiply * 100) AS price,last_price,price_history,products.multiply AS multiply,prices,min_amount,currency,currencies.name AS currency_name,shops.shop_id AS shop_id,shops.internal_code AS shop_code,shops.name AS shop_name,img,img_width,img_height,img_src,added,products.updated AS updated,last_seen FROM products LEFT JOIN shops ON shop_id = shop LEFT JOIN producers ON producer_id = producer LEFT JOIN currencies ON currencies.symbol = currency WHERE prod_id IN ("43672","1234827","752637","2568089","2966413","367950","5014492","3814059","1671966","5123458") ORDER BY prod_id LIMIT 10"""

sqlstring = """
SELECT *
FROM Orders
LEFT JOIN Customers
"""

print(extract_tables(string))
