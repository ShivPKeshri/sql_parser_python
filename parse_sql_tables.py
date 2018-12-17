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
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING', 'UNION', 'ALL', 'UNION ALL', 'INTERSECT', 'MINUS']:
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
            # print([x for x in stream])
            extracted_tables.append(set(list(extract_table_identifiers(stream))))
    sub_table_list= list(itertools.chain(*extracted_tables))
    # print(sub_table_list)
    tables_list = list(subquery_as_table(sub_table_list))

    return tables_list



def subquery_as_table(tab_list):

    table_set=set() 
    for index in range(len(tab_list)):        
        if 'select' not in tab_list[index]:
            table_set.add(tab_list[index])
            
    return table_set


string = "Select a.col1, b.col2 from tb1 as a inner join tb2 as b on tb1.col7 = tb2.col8;"
string1= """SELECT /* Elecena\Models\Product::newFromIds */ prod_id AS id,code,products.name AS name,products.description AS description,producers.producer_id AS producer_id,producers.name AS producer,extra,parameters,url,price AS price_original,price *  currencies.value / (currencies.multiply * 100) AS price,last_price,price_history,products.multiply AS multiply,prices,min_amount,currency,currencies.name AS currency_name,shops.shop_id AS shop_id,shops.internal_code AS shop_code,shops.name AS shop_name,img,img_width,img_height,img_src,added,products.updated AS updated,last_seen FROM products LEFT JOIN shops ON shop_id = shop LEFT JOIN producers ON producer_id = producer LEFT JOIN currencies ON currencies.symbol = currency WHERE prod_id IN ("43672","1234827","752637","2568089","2966413","367950","5014492","3814059","1671966","5123458") ORDER BY prod_id LIMIT 10"""

sqlstring = """
SELECT *
FROM Orders
LEFT JOIN Customers
"""

sqlsubstring = """
Select * from s.A as a where a.id in (select id from s.B)
"""
sqlEx1="""
SELECT suppliers.name, subquery1.total_amt
FROM suppliers,
 (SELECT supplier_id, SUM(orders.amount) AS total_amt
  FROM orders
  GROUP BY supplier_id) subquery1
WHERE subquery1.supplier_id = suppliers.supplier_id;
"""

sqlquery1 = """
    select 
    content,
    nick,
    name
    FROM table1 a
    JOIN table2 b ON a.sender_id = b.user_id
    JOIN table3 c ON a.channel_id = c.channel_id
    JOIN table4 d ON c.store_id = d.store_id
    WHERE sender_id NOT IN
      (select user_id
       FROM table5
       WHERE store_id IN ('agent_store:1',
                                         'ask:1'))
       AND to_timestamp(a.time_updated_server/1000)::date >= '2014-05-01'
       GROUP BY 1,2,3,4
       HAVING sum(1) > 500
       ORDER BY 1 ASC
    """


sqltab1="""select * from tab1  where t1.id in select id from tab1 """


sqlDemo="""
SELECT ( ALLOC_ESTIMATED_SHPMENT.ESTD_SHIPMN_QTY+ALLOC_ESTIMATED_SHPMENT.CNTLD_ALLOC_QTY+ALLOC_ESTIMATED_SHPMENT.TEMPRY_ALLOC_QTY ) as EstShip,   ALLOC_ESTIMATED_SHPMENT.ALLOC_GRP_CD,   BRAND_ALLOC_GRP_HIERARCHY.BRAND,   ALLOC_ESTIMATED_SHPMENT.BUSNS_ASCT_CD,   ALLOC_ESTIMATED_SHPMENT.CONSNS_PRD_CD,   ALLOC_ESTIMATED_SHPMENT.SELLNG_SRC_CD,   DECODE(SIGN(( ( ALLOC_ESTIMATED_SHPMENT.ESTD_SHIPMN_QTY+ALLOC_ESTIMATED_SHPMENT.CNTLD_ALLOC_QTY+ALLOC_ESTIMATED_SHPMENT.TEMPRY_ALLOC_QTY ) )-( SUM(ALLOC_WKLY_ALLOC_VOLUME.WK_FINAL_ALLOC_QTY+ALLOC_WKLY_ALLOC_VOLUME.FINAL_WK_ADJ_QTY) )),1,( ( ( ALLOC_ESTIMATED_SHPMENT.ESTD_SHIPMN_QTY+ALLOC_ESTIMATED_SHPMENT.CNTLD_ALLOC_QTY+ALLOC_ESTIMATED_SHPMENT.TEMPRY_ALLOC_QTY ) )-( SUM(ALLOC_WKLY_ALLOC_VOLUME.WK_FINAL_ALLOC_QTY+ALLOC_WKLY_ALLOC_VOLUME.FINAL_WK_ADJ_QTY) ) ),0) AS AllocDeclined,   SUM(ALLOC_WKLY_ALLOC_VOLUME.WK_FINAL_ALLOC_QTY+ALLOC_WKLY_ALLOC_VOLUME.FINAL_WK_ADJ_QTY) AS FinalAlloc  FROM VSMDDM.ALLOC_ESTIMATED_SHPMENT ALLOC_ESTIMATED_SHPMENT,   VSMDDM.ALLOC_WKLY_ALLOC_VOLUME ALLOC_WKLY_ALLOC_VOLUME,   VSMDDM.BRAND_ALLOC_GRP_HIERARCHY BRAND_ALLOC_GRP_HIERARCHY  WHERE ( ( ALLOC_ESTIMATED_SHPMENT.SELLNG_SRC_CD = ALLOC_WKLY_ALLOC_VOLUME.SELLNG_SRC_CD   AND ALLOC_ESTIMATED_SHPMENT.DSTRBN_ENT_TYPE_CD = ALLOC_WKLY_ALLOC_VOLUME.DSTRBN_ENT_TYPE_CD   AND ALLOC_ESTIMATED_SHPMENT.ALLOC_GRP_CD = ALLOC_WKLY_ALLOC_VOLUME.ALLOC_GRP_CD   AND ALLOC_ESTIMATED_SHPMENT.CONSNS_PRD_CD = ALLOC_WKLY_ALLOC_VOLUME.ALLOC_PRD_CD   AND ALLOC_ESTIMATED_SHPMENT.BUSNS_ASCT_CD = ALLOC_WKLY_ALLOC_VOLUME.BUSNS_ASCT_CD   AND ALLOC_ESTIMATED_SHPMENT.MODEL_YR_NUM = ALLOC_WKLY_ALLOC_VOLUME.MODEL_YR_NUM )   AND ( BRAND_ALLOC_GRP_HIERARCHY.ALLOC_GRP_CD = ALLOC_ESTIMATED_SHPMENT.ALLOC_GRP_CD ) )   AND ( ALLOC_ESTIMATED_SHPMENT.CONSNS_PRD_CD = ALLOC_ESTIMATED_SHPMENT.FCST_PRD_CD )   AND ( ALLOC_estimated_shpment.DSTRBN_ENT_TYPE_CD = 'RET' )  GROUP BY ( ALLOC_ESTIMATED_SHPMENT.ESTD_SHIPMN_QTY+ALLOC_ESTIMATED_SHPMENT.CNTLD_ALLOC_QTY+ALLOC_ESTIMATED_SHPMENT.TEMPRY_ALLOC_QTY ),   ALLOC_ESTIMATED_SHPMENT.ALLOC_GRP_CD,   BRAND_ALLOC_GRP_HIERARCHY.BRAND,   ALLOC_ESTIMATED_SHPMENT.BUSNS_ASCT_CD,   ALLOC_ESTIMATED_SHPMENT.CONSNS_PRD_CD,   ALLOC_ESTIMATED_SHPMENT.SELLNG_SRC_CD  ORDER BY ALLOC_ESTIMATED_SHPMENT.CONSNS_PRD_CD ASC,   ALLOC_ESTIMATED_SHPMENT.BUSNS_ASCT_CD ASC

"""


sqlsub1="""
SELECT column-names
  FROM table_name1,
  (select * from table_name3 where some_id in (select id from table_name4)) tab3
 WHERE value IN (SELECT column-name
                   FROM table_name2 
                  WHERE condition)"""

sqlsub2="""SELECT FirstName, LastName, 
       OrderCount = (SELECT COUNT(O.Id) FROM [Order] O WHERE O.CustomerId = C.Id)
  FROM Customer C """ 

sqlunion="""
SELECT 'Customer' As Type, 
       FirstName + ' ' + LastName AS ContactName, 
       City, Country, Phone
  FROM Customer, mantab
MINUS 
SELECT 'Supplier', 
       ContactName, City, Country, Phone
  FROM Supplier, mytab
"""  

sqlex2="""
SELECT DISTINCT FirstName + ' ' + LastName as CustomerName
  FROM Customer, [Order]
 WHERE Customer.Id = [Order].CustomerId
   AND TotalAmount > ALL 
       (SELECT AVG(TotalAmount)
          FROM [Order]
         GROUP BY CustomerId) 
 """          

print(extract_tables(sqlex2))
