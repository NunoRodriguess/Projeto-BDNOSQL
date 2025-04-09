from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

# Helper function to get status name
def get_status_name(status_id):
    status_names = {
        1: "Order Received",
        2: "Pending Delivery",
        3: "Delivery In Progress",
        4: "Delivered",
        5: "Cancelled",
        6: "Returned"
    }
    return status_names.get(status_id, "Unknown Status")

def update_order_status(order_id, status_id):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bookstore']
    orders_collection = db['orders']
    
    try:
        # Find the order document
        order = orders_collection.find_one({"_id": ObjectId(order_id)})
        
        if not order:
            print(f"Order {order_id} not found")
            return
        
        # Get the current status
        current_status = None
        if order.get('history') and len(order['history']) > 0:
            current_status = order['history'][-1]['status']['_id']
        
        # If status is different and valid, update it
        if current_status != status_id and status_id in range(1,7):
            # Get the next history ID
            next_history_id = 1
            if order.get('history'):
                next_history_id = max(h['_id'] for h in order['history']) + 1
            
            # Create new status entry
            new_status = {
                "_id": next_history_id,
                "status": {
                    "_id": status_id,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "status": get_status_name(status_id)
                }
            }
            
            # Update the document
            result = orders_collection.update_one(
                {"_id": ObjectId(order_id)},
                {"$push": {"history": new_status}}
            )
            
            if result.modified_count > 0:
                print(f'Status updated successfully for order {order_id}')
            else:
                print(f'Failed to update status for order {order_id}')
        else:
            print(f'Order {order_id} already has status {status_id} or invalid status ID')
            
    except Exception as e:
        print(f"Error updating order status: {str(e)}")
    finally:
        client.close()

# ===== TEST OPERATIONS =====
def test_order_lifecycle():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bookstore']
    orders_collection = db['orders']
    
    try:
        # 1. CREATE a test order
        test_order = {
            "order_date": datetime.now().strftime("%Y-%m-%d"),
            "method": {
                "_id": 1,
                "name": "Standard",
                "cost": 5.99
            },
            "address": {
                "_id": 100,
                "street_number": 123,
                "street_name": "Test Street",
                "city": "Testville",
                "country": {
                    "_id": 1,
                    "name": "Testland"
                }
            },
            "history": [
                {
                    "_id": 1,
                    "status": {
                        "_id": 1,
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "status": "Order Received"
                    }
                }
            ],
            "lines": [
                {
                    "_id": 1,
                    "book_id": 101,
                    "price": 12.99
                }
            ]
        }
        
        insert_result = orders_collection.insert_one(test_order)
        order_id = insert_result.inserted_id
        print(f"\nCreated test order with ID: {order_id}")
        
        # 2. RETRIEVE the order
        print("\nRetrieving order...")
        retrieved_order = orders_collection.find_one({"_id": order_id})
        print("Current order status:", retrieved_order['history'][-1]['status']['status'])
        
        # 3. UPDATE the status (twice to test changes)
        print("\nUpdating status to Pending Delivery (2)...")
        update_order_status(order_id, 2)
        
        print("\nUpdating status to Delivery In Progress (3)...")
        update_order_status(order_id, 3)
        
        # Try to set same status again (should show message)
        print("\nAttempting to set same status again...")
        update_order_status(order_id, 3)
        
        # 4. Show final state
        print("\nFinal order state:")
        final_order = orders_collection.find_one({"_id": order_id})
        for status in final_order['history']:
            print(f"Status {status['status']['_id']}: {status['status']['status']} on {status['status']['date']}")
        
        # 5. DELETE the test order (comment this out if you want to keep it)
        delete_result = orders_collection.delete_one({"_id": order_id})
        if delete_result.deleted_count > 0:
            print(f"\nSuccessfully deleted test order {order_id}")
        else:
            print(f"\nFailed to delete test order {order_id}")
            
    except Exception as e:
        print(f"Error during test operations: {str(e)}")
    finally:
        client.close()

# Run the test
if __name__ == "__main__":
    test_order_lifecycle()