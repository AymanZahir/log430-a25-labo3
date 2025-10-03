"""
Order controller
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from flask import jsonify
from orders.commands.write_order import add_order, delete_order
from orders.queries.read_order import get_order_by_id, get_best_selling_products, get_highest_spending_users

def create_order(request):
    """Create order, use WriteOrder model"""
    payload = request.get_json() or {}
    user_id = payload.get('user_id')
    items = payload.get('items', [])
    try:
        order_id = add_order(user_id, items)
        return jsonify({'order_id': order_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def remove_order(order_id):
    """Delete order, use WriteOrder model"""
    try:
        deleted = delete_order(order_id)
        if deleted:
            return jsonify({'deleted': True})
        return jsonify({'deleted': False}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_order(order_id):
    """Create order, use ReadOrder model"""
    try:
        order = get_order_by_id(order_id)
        return jsonify(order), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
def get_report_highest_spending_users():
    """Get orders report: highest spending users"""
    return get_highest_spending_users()

def get_report_best_selling_products():
    """Get orders report: best selling products"""
    return get_best_selling_products()