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

def print_vehicle_route(vehicle_id):
    with driver.session(database=DATABASE) as session:
        result = session.run("""
            MATCH (from)-[r:ROUTE_TO]->(to)
            WHERE r.vehicle_id = $vehicle_id
            RETURN 
                r.order AS order, 
                from AS from_node, 
                to AS to_node, 
                r.distance AS distance, 
                r.duration AS duration
            ORDER BY r.order ASC
        """, vehicle_id=vehicle_id)

        print(f"\nRoute for Vehicle {vehicle_id}:")
        for record in result:
            order = record["order"]
            distance = record["distance"]
            duration = record["duration"]
            from_node = record["from_node"]
            to_node = record["to_node"]

            from_label = list(from_node.labels)[0]
            to_label = list(to_node.labels)[0]

            from_id = from_node.get("Warehouse_ID") if from_label == "Warehouse" else from_node.get("Customer_ID", "N/A")
            to_id = to_node.get("Warehouse_ID") if to_label == "Warehouse" else to_node.get("Customer_ID", "N/A")

            print(f"Order {order}: {from_label} {from_id} → {to_label} {to_id} | Distance: {distance} km | Duration: {duration} hr")

print_vehicle_route('V002')