from neo4j import GraphDatabase
import openrouteservice
import pandas as pd

URI = "bolt://localhost:7687"  
USERNAME = "neo4j"  
PASSWORD = "Tanazi369"  
DATABASE = "neo4j" 
API_KEY = "5b3ce3597851110001cf6248b2b74b27a89f4836aa6790441684f9ea"
client = openrouteservice.Client(key=API_KEY)
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

csv_files = {
    "Vehicle": r"dataset/vehicle_updated.csv",
    "Warehouse": r"dataset/warehouse_updated.csv",
    "Customer": r"dataset/customer_updated.csv",
    "Order": r"dataset/order.csv"
}

def create_nodes(tx, label, properties):
        
    if "Latitude" in properties and "Longitude" in properties:
        lat, lon = properties.pop("Latitude"), properties.pop("Longitude")
        query = f"""
        CREATE (n:{label} {{ {', '.join(f'{key}: ${key}' for key in properties.keys())}, location: point({{latitude: $lat, longitude: $lon}}) }})
        """
        properties["lat"] = lat
        properties["lon"] = lon
   
    else:
        query = f"""
        CREATE (n:{label} {{ {', '.join(f'{key}: ${key}' for key in properties.keys())} }})
        """
    tx.run(query, **properties)


def load_to_neo4j():
    with driver.session(database=DATABASE) as session:
        for label, file_path in csv_files.items():
            df = pd.read_csv(file_path)            
            for _, row in df.iterrows():
                properties = {col: row[col] for col in df.columns if pd.notna(row[col])}  
                
                if label=="Order":
                    properties = {key: value for key, value in properties.items() if key not in ["Customer_ID", "Warehouse_ID"]}      

                session.execute_write(create_nodes, label, properties)

            

def relation_from_orders(tx, order_id, customer_id, warehouse_id):
    query = """
    MATCH (o:Order {Order_ID: $order_id})
    MATCH (c:Customer {Customer_ID: $customer_id})
    MATCH (w:Warehouse {Warehouse_ID: $warehouse_id})
    MERGE (o)-[:PLACED_BY]->(c)
    MERGE (o)-[:SHIPPED_FROM]->(w)
    """
    tx.run(query, order_id=order_id, customer_id=customer_id, warehouse_id=warehouse_id)

def connect_orders(file_path):
    df = pd.read_csv(file_path)
        
    with driver.session(database=DATABASE) as session:
        for _, row in df.iterrows():
            order_id = row["Order_ID"]
            customer_id = row["Customer_ID"]
            warehouse_id = row["Warehouse_ID"]
            session.execute_write(relation_from_orders, order_id, customer_id, warehouse_id)

        print("Orders are connected with respective warehouse and customer!")
        
def route_between_two_points(coords):
    route = client.directions(coords, profile='driving-car', format='json')
    distance = route["routes"][0]["summary"]["distance"]
    duration = route["routes"][0]["summary"]["duration"]
    return distance,duration

def route_relationship(tx, label1, id1, label2, id2, rel_type, properties):    
   
        properties_str = ", ".join(f"{key}: ${key}" for key in properties.keys()) if properties else ""     
        query = f"""
            MATCH (a:{label1} {{{label1}_ID: $id1}}), (b:{label2} {{{label2}_ID: $id2}})
            MERGE (a)-[r:{rel_type} {{ {properties_str} }}]->(b)
            """
        params = {"id1": id1, "id2": id2}    
        if properties:
            params.update(properties)
        tx.run(query, **params)

def generate_routes():
    
    with driver.session(database=DATABASE) as session:
        query = """
        MATCH (o:Order)-[:SHIPPED_FROM]->(w:Warehouse), (o)-[:PLACED_BY]->(c:Customer)
        RETURN o.Order_ID AS order_id, w.Warehouse_ID AS warehouse_id, w.location AS warehouse_location,
               c.Customer_ID AS customer_id, c.location AS customer_location
        """
        
        orders = session.execute_read(lambda tx: 
            [{"order_id": record["order_id"], 
              "warehouse_id": record["warehouse_id"], 
              "warehouse_location": record["warehouse_location"], 
              "customer_id": record["customer_id"], 
              "customer_location": record["customer_location"]}
             for record in tx.run(query)]
        )

        failed_connections=[]
        
        for order in orders:
            warehouse_location = order["warehouse_location"]
            customer_location = order["customer_location"]
            warehouse_id=order["warehouse_id"]
            customer_id=order["customer_id"]
            
            warehouse_coords = (warehouse_location.x, warehouse_location.y)  
            customer_coords = (customer_location.x, customer_location.y)  
            coords = [warehouse_coords, customer_coords]           
                
            try:
                distance,duration=route_between_two_points(coords)
                distance_km = round(distance / 1000, 4)  
                duration_hr = round(duration / 3600, 4)
                session.execute_write(route_relationship, "Warehouse",warehouse_id,"Customer", customer_id,"DELIVERS_TO",{"distance": distance_km, "duration": duration_hr})
                
            except Exception as e:
                print(f"Failed to connect {warehouse_id} -> {customer_id}: {str(e)}")
                failed_connections.append({"warehouse_id": warehouse_id, "customer_id": customer_id})
            
        
        print()        
        
def generate_warehouse_links():
    with driver.session(database=DATABASE) as session:
        
        query = """
        MATCH (w:Warehouse)
        RETURN w.Warehouse_ID AS id, w.location AS location
        """
        warehouses = session.execute_read(lambda tx: [
            {"id": record["id"], "location": record["location"]}
            for record in tx.run(query)
        ])

        failed_connections = []

        
        for i in range(len(warehouses)):
            for j in range(i + 1, len(warehouses)):
                w1 = warehouses[i]
                w2 = warehouses[j]

                loc1 = (w1["location"].x, w1["location"].y)
                loc2 = (w2["location"].x, w2["location"].y)
                coords = [loc1, loc2]

                try:
                    distance, duration = route_between_two_points(coords)
                    distance_km = round(distance / 1000, 4)
                    duration_hr = round(duration / 3600, 4)

                   
                    session.execute_write(
                        route_relationship,
                        "Warehouse", w1["id"],
                        "Warehouse", w2["id"],
                        "EXTERNAL", {"distance": distance_km, "duration": duration_hr}
                    )
                    session.execute_write(
                        route_relationship,
                        "Warehouse", w2["id"],
                        "Warehouse", w1["id"],
                        "EXTERNAL", {"distance": distance_km, "duration": duration_hr}
                    )

                   
                except Exception as e:
                    print(f"Failed to connect {w1['id']} <--> {w2['id']}: {str(e)}")
                    failed_connections.append((w1["id"], w2["id"]))

        print()
        


load_to_neo4j()
connect_orders(csv_files["Order"])
generate_routes()
generate_warehouse_links()
