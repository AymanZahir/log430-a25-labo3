"""
Product stocks (write-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from sqlalchemy import text
from stocks.models.stock import Stock
from stocks.models.product import Product
from db import get_redis_conn, get_sqlalchemy_session

def set_stock_for_product(product_id, quantity):
    """Set stock quantity for product in MySQL"""
    session = get_sqlalchemy_session()
    try: 
        result = session.execute(
            text(f"""
                UPDATE stocks 
                SET quantity = :qty 
                WHERE product_id = :pid
            """),
            {"pid": product_id, "qty": quantity}
        )
        response_message = f"rows updated: {result.rowcount}"
        if result.rowcount == 0:
            new_stock = Stock(product_id=product_id, quantity=quantity)
            session.add(new_stock)
            session.flush() 
            response_message = f"rows added: {new_stock.product_id}"
        session.commit()

        product = session.query(Product).filter(Product.id == product_id).first()
        redis_payload = {"quantity": quantity}
        if product:
            redis_payload.update({
                "name": product.name,
                "sku": product.sku,
                "price": float(product.price)
            })
  
        r = get_redis_conn()
        r.hset(f"stock:{product_id}", mapping=redis_payload)
        return response_message
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
    
def update_stock_mysql(session, order_items, operation):
    """ Update stock quantities in MySQL according to a given operation (+/-) """
    try:
        for item in order_items:
            if hasattr(order_items[0], 'product_id'):
                pid = item.product_id
                qty = item.quantity
            else:
                pid = item['product_id']
                qty = item['quantity']
            session.execute(
                text(f"""
                    UPDATE stocks 
                    SET quantity = quantity {operation} :qty 
                    WHERE product_id = :pid
                """),
                {"pid": pid, "qty": qty}
            )
    except Exception as e:
        raise e
    
def check_out_items_from_stock(session, order_items):
    """ Decrease stock quantities in Redis """
    update_stock_mysql(session, order_items, "-")
    
def check_in_items_to_stock(session, order_items):
    """ Increase stock quantities in Redis """
    update_stock_mysql(session, order_items, "+")

def update_stock_redis(order_items, operation):
    """ Update stock quantities in Redis """
    if not order_items:
        return
    r = get_redis_conn()
    if not list(r.scan_iter("stock:*")):
        _populate_redis_from_mysql(r)

    product_ids = set()
    normalized_items = []
    for item in order_items:
        if hasattr(item, 'product_id'):
            product_id = item.product_id
            quantity = item.quantity
            unit_price = getattr(item, 'unit_price', None)
        else:
            product_id = item['product_id']
            quantity = item['quantity']
            unit_price = item.get('unit_price')
        normalized_items.append((product_id, quantity, unit_price))
        product_ids.add(product_id)

    session = get_sqlalchemy_session()
    try:
        products = (
            session.query(Product)
            .filter(Product.id.in_(product_ids))
            .all()
        ) if product_ids else []
        product_map = {product.id: product for product in products}

        pipeline = r.pipeline()
        for product_id, quantity, unit_price in normalized_items:
            current_stock = r.hget(f"stock:{product_id}", "quantity")
            current_stock = int(current_stock) if current_stock else 0

            if operation == '+':
                new_quantity = current_stock + quantity
            else:
                new_quantity = current_stock - quantity

            mapping = {"quantity": new_quantity}
            product = product_map.get(product_id)
            if product:
                mapping.update({
                    "name": product.name,
                    "sku": product.sku,
                    "price": float(product.price)
                })
            elif unit_price is not None:
                mapping["price"] = float(unit_price)

            pipeline.hset(f"stock:{product_id}", mapping=mapping)

        pipeline.execute()
    finally:
        session.close()

def _populate_redis_from_mysql(redis_conn):
    """ Helper function to populate Redis from MySQL stocks table """
    session = get_sqlalchemy_session()
    try:
        stocks = session.execute(
            text("""
                SELECT s.product_id, s.quantity, p.name, p.sku, p.price
                FROM stocks s
                JOIN products p ON s.product_id = p.id
            """)
        ).fetchall()

        if not len(stocks):
            print("Il n'est pas nécessaire de synchronisér le stock MySQL avec Redis")
            return
        
        pipeline = redis_conn.pipeline()
        
        for product_id, quantity, name, sku, price in stocks:
            pipeline.hset(
                f"stock:{product_id}", 
                mapping={
                    "quantity": int(quantity),
                    "name": name,
                    "sku": sku,
                    "price": float(price)
                }
            )
        
        pipeline.execute()
        print(f"{len(stocks)} enregistrements de stock ont été synchronisés avec Redis")
        
    except Exception as e:
        print(f"Erreur de synchronisation: {e}")
        raise e
    finally:
        session.close()
