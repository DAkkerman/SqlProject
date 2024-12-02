from flask import Flask, request, render_template, redirect, url_for
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException
from faker import Faker
import random
import math

app = Flask(__name__)

try:
    cluster = Cluster('couchbase://127.0.0.1', ClusterOptions(PasswordAuthenticator('Administrator', 'chepak123')))
    bucket = cluster.bucket('b1')
    users_collection = bucket.scope('sc1').collection('users')
    orders_collection = bucket.scope('sc1').collection('orders')
    products_collection = bucket.scope('sc1').collection('products')
    categories_collection = bucket.scope('sc1').collection('categories')
    order_items_collection = bucket.scope('sc1').collection('order_items')
except CouchbaseException as e:
    print(f"Failed to connect to Couchbase: {e}")
    exit(1)

# Initialize Faker
fake = Faker()

# Function to clear all data in a collection
def clear_collection(collection):
    query = f"DELETE FROM `{bucket.name}`.`sc1`.`{collection.name}`"
    try:
        cluster.query(query).execute()
    except CouchbaseException as e:
        print(f"Failed to clear collection {collection.name}: {e}")

# Function to generate test data
def generate_test_data():
    # Clear existing data
    clear_collection(users_collection)
    clear_collection(categories_collection)
    clear_collection(products_collection)
    clear_collection(orders_collection)
    clear_collection(order_items_collection)

    # Generate users
    users = []
    for _ in range(100):
        user = {
            "key": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email()
        }
        users.append(user)
        users_collection.upsert(user['key'], user)

    # Generate categories
    categories = []
    for _ in range(100):
        category = {
            "key": fake.uuid4(),
            "name": fake.word()
        }
        categories.append(category)
        categories_collection.upsert(category['key'], category)

    # Generate products
    products = []
    for _ in range(100):
        product = {
            "key": fake.uuid4(),
            "name": fake.word(),
            "price": round(random.uniform(10, 1000), 2),
            "category_id": random.choice(categories)['key']
        }
        products.append(product)
        products_collection.upsert(product['key'], product)

    # Generate orders
    orders = []
    for _ in range(100):
        order = {
            "key": fake.uuid4(),
            "user_id": random.choice(users)['key'],
            "order_date": fake.date_this_decade().isoformat()
        }
        orders.append(order)
        orders_collection.upsert(order['key'], order)

    # Generate order items
    order_items = []
    order_item_count = 0
    while order_item_count < 100:
        order = random.choice(orders)
        order_item = {
            "key": fake.uuid4(),
            "order_id": order['key'],
            "product_id": random.choice(products)['key'],
            "quantity": random.randint(1, 10)
        }
        order_items.append(order_item)
        order_items_collection.upsert(order_item['key'], order_item)
        order_item_count += 1

# Generate test data
generate_test_data()

def get_paginated_data(collection, page, per_page):
    offset = (page - 1) * per_page
    query = f"SELECT * FROM `{bucket.name}`.`sc1`.`{collection}` LIMIT {per_page} OFFSET {offset}"
    result = cluster.query(query)

    # Execute the query and fetch the results
    rows = []
    for row in result.rows():
        rows.append(row)

    total_items_query = f"SELECT COUNT(*) AS total FROM `{bucket.name}`.`sc1`.`{collection}`"
    total_items_result = cluster.query(total_items_query)

    # Execute the total items query and fetch the results
    total_items = 0
    for row in total_items_result.rows():
        total_items = row['total']

    total_pages = math.ceil(total_items / per_page)
    return rows, total_pages

@app.route('/', methods=['GET', 'POST'])
def index():
    query_text = None
    rows = []
    if request.method == 'POST':
        query_text = request.form.get('query')
        try:
            result = cluster.query(query_text)
            rows = [row for row in result.rows()]
        except CouchbaseException as e:
            rows = [{"error": str(e)}]
    return render_template('index.html', query=query_text, rows=rows)

@app.route('/users')
def users_index():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    users, total_pages = get_paginated_data('users', page, per_page)
    return render_template('users.html', users=users, page=page, per_page=per_page, total_pages=total_pages)

@app.route('/orders')
def orders_index():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    orders, total_pages = get_paginated_data('orders', page, per_page)
    return render_template('orders.html', orders=orders, page=page, per_page=per_page, total_pages=total_pages)

@app.route('/products')
def products_index():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    products, total_pages = get_paginated_data('products', page, per_page)
    return render_template('products.html', products=products, page=page, per_page=per_page, total_pages=total_pages)

@app.route('/categories')
def categories_index():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    categories, total_pages = get_paginated_data('categories', page, per_page)
    return render_template('categories.html', categories=categories, page=page, per_page=per_page, total_pages=total_pages)

@app.route('/order_items')
def order_items_index():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    order_items, total_pages = get_paginated_data('order_items', page, per_page)
    return render_template('order_items.html', order_items=order_items, page=page, per_page=per_page, total_pages=total_pages)

@app.route('/create/<collection>', methods=['GET', 'POST'])
def create(collection):
    if request.method == 'POST':
        data = request.form.to_dict()
        key = data['key']
        try:
            if collection == 'users':
                users_collection.upsert(key, data)
            elif collection == 'orders':
                orders_collection.upsert(key, data)
            elif collection == 'products':
                products_collection.upsert(key, data)
            elif collection == 'categories':
                categories_collection.upsert(key, data)
            elif collection == 'order_items':
                order_items_collection.upsert(key, data)
        except CouchbaseException as e:
            return f"Error creating record: {e}", 500
        return redirect(url_for(f'{collection}_index'))
    return render_template(f'create_{collection}.html')

@app.route('/update/<collection>/<key>', methods=['GET', 'POST'])
def update(collection, key):
    if request.method == 'POST':
        data = request.form.to_dict()
        try:
            if collection == 'users':
                users_collection.upsert(key, data)
            elif collection == 'orders':
                orders_collection.upsert(key, data)
            elif collection == 'products':
                products_collection.upsert(key, data)
            elif collection == 'categories':
                categories_collection.upsert(key, data)
            elif collection == 'order_items':
                order_items_collection.upsert(key, data)
        except CouchbaseException as e:
            return f"Error updating record: {e}", 500
        return redirect(url_for(f'{collection}_index'))

    try:
        if collection == 'users':
            result = users_collection.get(key)
        elif collection == 'orders':
            result = orders_collection.get(key)
        elif collection == 'products':
            result = products_collection.get(key)
        elif collection == 'categories':
            result = categories_collection.get(key)
        elif collection == 'order_items':
            result = order_items_collection.get(key)
        record = result.content_as[dict]
    except CouchbaseException as e:
        return f"Error fetching record: {e}", 500
    return render_template(f'update_{collection}.html', record=record)

@app.route('/delete/<collection>/<key>', methods=['GET', 'POST'])
def delete(collection, key):
    if request.method == 'POST':
        try:
            if collection == 'users':
                users_collection.remove(key)
            elif collection == 'orders':
                orders_collection.remove(key)
            elif collection == 'products':
                products_collection.remove(key)
            elif collection == 'categories':
                categories_collection.remove(key)
            elif collection == 'order_items':
                order_items_collection.remove(key)
        except CouchbaseException as e:
            return f"Error deleting record: {e}", 500
        return redirect(url_for(f'{collection}_index'))

    try:
        if collection == 'users':
            result = users_collection.get(key)
        elif collection == 'orders':
            result = orders_collection.get(key)
        elif collection == 'products':
            result = products_collection.get(key)
        elif collection == 'categories':
            result = categories_collection.get(key)
        elif collection == 'order_items':
            result = order_items_collection.get(key)
        record = result.content_as[dict]
    except CouchbaseException as e:
        return f"Error fetching record: {e}", 500
    return render_template(f'delete_{collection}.html', record=record)

if __name__ == '__main__':
    app.run(debug=True)
