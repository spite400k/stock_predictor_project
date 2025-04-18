import psycopg2

def save_to_db(product_id, product_name, site, stock_status, price):
    conn = psycopg2.connect("dbname=stock_db user=admin password=pass host=localhost")
    cursor = conn.cursor()
    
    query = """
    INSERT INTO trn_ranked_item_stock (product_id, product_name, site, stock_status, price)
    VALUES (%s, %s, %s, %s, %s);
    """
    cursor.execute(query, (product_id, product_name, site, stock_status, price))
    
    conn.commit()
    cursor.close()
    conn.close()
