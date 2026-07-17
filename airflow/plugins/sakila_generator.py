# airflow/plugins/sakila_generator.py
import random
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Static array seed pools for transaction generation
FIRST_NAMES = ["ALEX", "JORDAN", "TAYLOR", "MORGAN", "SAM", "JAMIE", "CASEY", "ROBIN", "QUINN", "VIET"]
LAST_NAMES = ["SMITH", "NGUYEN", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS"]
FILM_ADJECTIVES = ["COSMIC", "SILENT", "GOLDEN", "SHADOW", "LOST", "VINTAGE", "MIDNIGHT", "QUANTUM"]
FILM_NOUNS = ["ODYSSEY", "LEGACY", "STRANGER", "VOYAGE", "WHISPER", "GLOW", "STORM", "CHRONICLES"]

def generate_new_customer(cursor) -> None:
    """Simulates new dimensional data entry by provisioning a brand new customer record."""
    cursor.execute("SELECT address_id FROM address ORDER BY RAND() LIMIT 1;")
    address_id = cursor.fetchone()[0]
    
    cursor.execute("SELECT store_id FROM store ORDER BY RAND() LIMIT 1;")
    store_id = cursor.fetchone()[0]
    
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    email = f"{first_name.lower()}.{last_name.lower()}@sakilacustomer.org"
    
    insert_query = """
        INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date, last_update)
        VALUES (%s, %s, %s, %s, %s, 1, NOW(), NOW());
    """
    cursor.execute(insert_query, (store_id, first_name, last_name, email, address_id))
    print(f"[DIMENSION] Created new customer record: {first_name} {last_name} (ID: {cursor.lastrowid})")

def update_existing_customer(cursor) -> None:
    """Simulates an SCD Type 2 data mutation by altering state fields on an existing customer."""
    cursor.execute("SELECT customer_id, first_name, last_name, active FROM customer ORDER BY RAND() LIMIT 1;")
    customer = cursor.fetchone()
    if not customer:
        return
    
    customer_id, first_name, last_name, current_active = customer
    mutation_choice = random.choice(["status", "email", "address"])
    
    if mutation_choice == "status":
        new_active = 0 if current_active == 1 else 1
        cursor.execute("UPDATE customer SET active = %s, last_update = NOW() WHERE customer_id = %s;", (new_active, customer_id))
        print(f"[DIMENSION] Mutated Customer {customer_id} Status: Toggled active state to {new_active}")
    elif mutation_choice == "email":
        new_email = f"{first_name.lower()}.{last_name.lower()}@interactive-de-platform.vn"
        cursor.execute("UPDATE customer SET email = %s, last_update = NOW() WHERE customer_id = %s;", (new_email, customer_id))
        print(f"[DIMENSION] Mutated Customer {customer_id} Email: Changed target address to {new_email}")
    elif mutation_choice == "address":
        cursor.execute("SELECT address_id FROM address ORDER BY RAND() LIMIT 1;")
        new_address_id = cursor.fetchone()[0]
        cursor.execute("UPDATE customer SET address_id = %s, last_update = NOW() WHERE customer_id = %s;", (new_address_id, customer_id))
        print(f"[DIMENSION] Mutated Customer {customer_id} Address: Mapped to new address key {new_address_id}")

def generate_new_film(cursor) -> None:
    """Simulates catalog dimension extension by inserting a new movie listing."""
    cursor.execute("SELECT language_id FROM language ORDER BY RAND() LIMIT 1;")
    language_id = cursor.fetchone()[0]
    
    title = f"{random.choice(FILM_ADJECTIVES)} {random.choice(FILM_NOUNS)}"
    rental_rate = random.choice([0.99, 2.99, 4.99])
    
    insert_query = """
        INSERT INTO film (title, description, release_year, language_id, rental_duration, rental_rate, replacement_cost, last_update)
        VALUES (%s, 'Modern DE pipeline validation catalog record.', 2026, %s, 5, %s, 19.99, NOW());
    """
    cursor.execute(insert_query, (title, language_id, rental_rate))
    print(f"[DIMENSION] Appended new catalog asset: '{title}' (ID: {cursor.lastrowid})")

def generate_operational_transactions(cursor) -> None:
    """Generates pure daily transactional business facts (Rentals & Payments)."""
    cursor.execute("SELECT customer_id FROM customer WHERE active = 1 ORDER BY RAND() LIMIT 1;")
    customer_id = cursor.fetchone()[0]
    
    cursor.execute("SELECT staff_id FROM staff ORDER BY RAND() LIMIT 1;")
    staff_id = cursor.fetchone()[0]
    
    # Locate an item in inventory currently sitting on shelves (no open rentals)
    query_inventory = """
        SELECT i.inventory_id, i.film_id FROM inventory i
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id AND r.return_date IS NULL
        WHERE r.rental_id IS NULL ORDER BY RAND() LIMIT 1;
    """
    cursor.execute(query_inventory)
    inventory_record = cursor.fetchone()
    if not inventory_record:
        return
        
    inventory_id, film_id = inventory_record
    
    cursor.execute("SELECT rental_rate FROM film WHERE film_id = %s;", (film_id,))
    rental_rate = cursor.fetchone()[0]
    
    # Execute atomic insertion of facts
    cursor.execute(
        "INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, last_update) VALUES (NOW(), %s, %s, %s, NOW());",
        (inventory_id, customer_id, staff_id)
    )
    rental_id = cursor.lastrowid
    
    cursor.execute(
        "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date, last_update) VALUES (%s, %s, %s, %s, NOW(), NOW());",
        (customer_id, staff_id, rental_id, rental_rate)
    )
    print(f"[OPERATIONAL] Logged factual delta: Rental {rental_id} associated with Payment of ${rental_rate}")

def run_comprehensive_generation(mysql_conn_id: str = 'mysql_local') -> None:
    """Orchestrates operational logs and analytical mutations within a clean transaction block."""
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        conn.autocommit = False
        print("=== COMMENCING SOURCE MUTATION ENGINE ===")
        
        # Always output operational fact records
        for _ in range(random.randint(2, 4)):
            generate_operational_transactions(cursor)
            
        # Probabilistic application of dimensional shifts
        if random.random() < 0.35:
            generate_new_customer(cursor)
        if random.random() < 0.50:
            update_existing_customer(cursor)
        if random.random() < 0.25:
            generate_new_film(cursor)
            
        conn.commit()
        print("=== TRANSACTION BATCH SYSTEM SUCCESSFULLY COMMITTED ===")
    except Exception as error:
        conn.rollback()
        print(f"[CRITICAL ERROR] Data generation execution failed. Rollback invoked: {error}")
        raise error
    finally:
        cursor.close()
        conn.close()