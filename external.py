def external():
    
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
        
    def get_orders_from_warehouse(session,warehouse_id):    
        result = session.execute_read(lambda tx: tx.run("""
            MATCH (o:Order)-[:SHIPPED_FROM]->(w:Warehouse {Warehouse_ID: $wid})
            WHERE o.Status = 'Pending'
            RETURN o.Order_ID AS order_id, o
        """, wid=warehouse_id).data())
        if result:
            return [{"order_id": record["order_id"], "order_data": dict(record["o"])} for record in result]
        else:
            return None
        
    def get_all_pending_orders(session):
        pending_orders = session.execute_read(lambda tx: [
            {
                "order_id": record["order_id"], 
                "warehouse_id": record["warehouse_id"],
                "customer_id": record["customer_id"],
                "customer_location": record["customer_location"]
            }
            for record in tx.run("""
                MATCH (o:Order)-[:SHIPPED_FROM]->(w:Warehouse), (o)-[:PLACED_BY]->(c:Customer)
                WHERE o.Status = 'Pending'
                RETURN o.Order_ID AS order_id, 
                       w.Warehouse_ID AS warehouse_id,
                       c.Customer_ID AS customer_id,
                       c.location AS customer_location
            """)
        ])
        
        if not pending_orders:            
            return None        
        return pending_orders
    
    def get_nearest_warehouse_by_external_distance(session,warehouse_id):          
        pending_orders = get_all_pending_orders(session)        
        if not pending_orders:
            return None        
        warehouses_with_orders = set(order["warehouse_id"] for order in pending_orders)        
        result = session.execute_read(lambda tx: tx.run("""
            MATCH (w1:Warehouse {Warehouse_ID: $wid})-[r:EXTERNAL]->(w2:Warehouse)
            WHERE w2.Warehouse_ID IN $valid_targets
            RETURN w2.Warehouse_ID AS neighbor_id, 
                r.distance AS distance, 
                r.duration AS duration
            ORDER BY r.distance ASC
            LIMIT 1
        """, wid=warehouse_id, valid_targets=list(warehouses_with_orders)).single())
        if result:
            return {
                "warehouse_id": result["neighbor_id"],
                "distance_km": result["distance"],
                "duration_hr": result["duration"]
            }
        else:
            return None  
        
    def update_current_warehouse(session,vehicles):    
        for vehicle in vehicles:
            vehicle_id = vehicle["vehicle_id"]
            current_warehouse_id = vehicle["current_warehouse"]
            if get_orders_from_warehouse(session,current_warehouse_id) is None:
                best_warehouse = get_nearest_warehouse_by_external_distance(session,current_warehouse_id)                
                if best_warehouse is not None:
                    best_warehouse_id = best_warehouse["warehouse_id"]
                    distance_km = round(best_warehouse["distance_km"], 4)
                    duration_hr = round(best_warehouse["duration_hr"], 4)                    
                    vehicle_status = session.execute_read(lambda tx: tx.run("""
                        MATCH ()-[r:ROUTE_TO {vehicle_id: $vid}]->()
                        RETURN coalesce(max(r.order), 0) AS max_order,
                coalesce(max(r.time_taken), 0) AS max_time
                    """, vid=vehicle_id).single())

                    order_counter = vehicle_status["max_order"] + 1 
                    cumulative_duration = vehicle_status["max_time"] + duration_hr 
                    session.execute_write(
                        route_relationship,
                        "Warehouse", current_warehouse_id,
                        "Warehouse", best_warehouse_id,
                        "ROUTE_TO",
                        {
                            "distance": distance_km,
                            "duration": duration_hr,
                            "order": order_counter,
                            "vehicle_id": vehicle_id,
                            "time_taken": round(cumulative_duration, 4),
                            "repositioning": True
                        }
                    )
                    vehicle["current_warehouse"] = best_warehouse_id
        
        return vehicles
    
    def create_in_cluster_edges_for_order_group(session,customers):        
        from collections import defaultdict        
        failed_connections = []
        for i in range(len(customers)):
            for j in range(i + 1, len(customers)):
                cust1 = customers[i]
                cust2 = customers[j]

                coords1 = (cust1["location"].x, cust1["location"].y)
                coords2 = (cust2["location"].x, cust2["location"].y)
                coords = [coords1, coords2]

                try:
                    distance, duration = route_between_two_points(coords)
                    distance_km = round(distance / 1000, 4)
                    duration_hr = round(duration / 3600, 4)

                    session.execute_write(
                        route_relationship, "Customer", cust1["customer_id"], "Customer", cust2["customer_id"],
                        "IN_CLUSTER", {"distance": distance_km, "duration": duration_hr}
                    )
                    session.execute_write(
                        route_relationship, "Customer", cust2["customer_id"], "Customer", cust1["customer_id"],
                        "IN_CLUSTER", {"distance": distance_km, "duration": duration_hr}
                    )

                except Exception as e:
                    print(f"Failed IN_CLUSTER: C{cust1['customer_id']} <-> C{cust2['customer_id']}: {str(e)}")
                    failed_connections.append({
                        "customer1": cust1["customer_id"],
                        "customer2": cust2["customer_id"]
                    })

        return failed_connections
    
    def get_node_location(session, node_id, is_warehouse):
            label = "Warehouse" if is_warehouse else "Customer"
            id_field = "Warehouse_ID" if is_warehouse else "Customer_ID"
            query = f"""
                MATCH (n:{label} {{{id_field}: $id}})
                RETURN n.location AS location
            """
            result = session.run(query, {"id": node_id})
            record = result.single()
            if record and record["location"]:
                loc = record["location"]
                return [loc.longitude, loc.latitude] 
            return None
    
    def get_edge_data(session, from_id, to_id, is_from_warehouse=False,to_warehouse=False):    
        if is_from_warehouse:
            query = """
                MATCH (:Warehouse {Warehouse_ID: $from})-[r:DELIVERS_TO]->(:Customer {Customer_ID: $to})
                RETURN r.distance AS dist, r.duration AS dur
            """
            
        elif to_warehouse:
            query = """
                MATCH (:Warehouse {Warehouse_ID: $to})-[r:DELIVERS_TO]->(:Customer {Customer_ID: $from})
                RETURN r.distance AS dist, r.duration AS dur
            """
        else:
            query = """
                MATCH (:Customer {Customer_ID: $from})-[r:IN_CLUSTER]->(:Customer {Customer_ID: $to})
                RETURN r.distance AS dist, r.duration AS dur
            """
        result = session.run(query, {"from": from_id, "to": to_id})
        record = result.single()
        
        if record and record["dist"] != float("inf"):
            return record["dist"], record["dur"]    
        from_coords = get_node_location(session, from_id, is_from_warehouse or not to_warehouse)
        to_coords = get_node_location(session, to_id, to_warehouse)
        if from_coords is None or to_coords is None:
             return float("inf"), float("inf")
    
        dis,dur=route_between_two_points([from_coords, to_coords])
        dis=round(dis/1000,4)
        dur=round(dur/3600,4)
        return dis,dur
    
    def get_clustered_orders_around_closest(session,vehicle_id, current_warehouse_id, cluster_threshold=30):   
        
        closest_order = session.execute_read(lambda tx: tx.run("""
            MATCH (w:Warehouse {Warehouse_ID: $wid})<-[:SHIPPED_FROM]-(o:Order)-[:PLACED_BY]->(c:Customer),
                  (w)-[r:DELIVERS_TO]->(c)
            WHERE o.Status = 'Pending'
            RETURN o.Order_ID AS order_id,
                   w.Warehouse_ID AS warehouse_id,
                   c.Customer_ID AS customer_id,
                   c.location AS customer_location,
                   r.distance AS delivery_distance
            ORDER BY r.distance ASC
            LIMIT 1
        """, wid=current_warehouse_id).single())

        if not closest_order:
            return None 
        customer_id = closest_order["customer_id"]      
        
        clustered_orders = session.execute_read(lambda tx: [
            {
                "vehicle_id": vehicle_id,
                "order_id": record["order_id"],
                "warehouse_id": record["warehouse_id"],
                "customer_id": record["customer_id"],
                "customer_location": record["customer_location"],
                "distance_km": record["distance_to_center"]
            }
            for record in tx.run("""
                MATCH (w:Warehouse {Warehouse_ID: $wid})<-[:SHIPPED_FROM]-(o:Order)-[:PLACED_BY]->(c:Customer),
                      (wc:Customer {Customer_ID: $center_id}),
                      (c)<-[r:DELIVERS_TO]-(w)
                WHERE o.Status = 'Pending'
                WITH o, w, c, point.distance(c.location, wc.location)/1000 AS distance_to_center
                WHERE distance_to_center <= $threshold
                RETURN o.Order_ID AS order_id,
                       w.Warehouse_ID AS warehouse_id,
                       c.Customer_ID AS customer_id,
                       c.location AS customer_location,
                       distance_to_center
                ORDER BY distance_to_center ASC
            """, wid=current_warehouse_id, center_id=customer_id, threshold=cluster_threshold)
        ])
        
        if clustered_orders:
            
            customers = [
                {"customer_id": o["customer_id"], "location": o["customer_location"]}
                for o in clustered_orders
            ]
            
            create_in_cluster_edges_for_order_group(session,customers)
       
        return clustered_orders if clustered_orders else None
    
    def tsp(session,vehicle,cluster_threshold=30):
        vehicle_id=vehicle['vehicle_id']
        current_warehouse_id=vehicle['current_warehouse']   
        order_result = session.run("""
        MATCH ()-[r:ROUTE_TO {vehicle_id: $vid}]->()
        RETURN coalesce(max(r.order), 0) AS max_order,
                coalesce(max(r.time_taken), 0) AS max_time
            """, vid=vehicle_id)

        vehicle_data = order_result.single()
        order_counter = vehicle_data["max_order"] + 1  
        cumulative_duration = vehicle_data["max_time"] if vehicle_data else 0 
        
        clustered_orders = get_clustered_orders_around_closest(session,vehicle_id, current_warehouse_id, cluster_threshold)      
        if not clustered_orders:
                return vehicle_data  

        customers = [order["customer_id"] for order in clustered_orders]
        
        unvisited = set(customers)
        route = []
        current = current_warehouse_id
        is_warehouse = True

        while unvisited:
            best_next = None
            min_dist = float('inf')
            for candidate in unvisited:
                dist, _ = get_edge_data(session, current, candidate, is_warehouse)
                if dist < min_dist:
                    min_dist = dist
                    best_next = candidate
            
            if best_next is None:
                break
            route.append(best_next)
            unvisited.remove(best_next)
            current = best_next
            is_warehouse = False
    

        
        last_customer = route[-1] if route else None
        return_warehouse_id = current_warehouse_id  

        if last_customer:
            wh_result = session.run("""
            MATCH (c:Customer {Customer_ID: $cid}), (w:Warehouse)
            RETURN w.Warehouse_ID AS wid, 
            point.distance(c.location, w.location) AS dist
            ORDER BY dist ASC
            LIMIT 1
            """, cid=last_customer)

            closest = wh_result.single()
            if closest:
                return_warehouse_id = closest["wid"]

        full_route = [("Warehouse", current_warehouse_id)] + [("Customer", cid) for cid in route] + [("Warehouse", return_warehouse_id)]
    
        for i in range(len(full_route) - 1):
                label1, id1 = full_route[i]
                label2, id2 = full_route[i + 1]
                is_from_warehouse = label1 == "Warehouse"
                to_warehouse = label2 == "Warehouse"
                
                dist, dur = get_edge_data(session, id1, id2, is_from_warehouse, to_warehouse)
                if dist == float('inf') or dur == float('inf'):
                    continue

                duration_hours = dur
                cumulative_duration += duration_hours

                
                session.execute_write(
                    route_relationship,
                    label1, id1,
                    label2, id2,
                    "ROUTE_TO",
                    {
                        "distance": round(dist, 4),
                        "duration": round(duration_hours, 4),
                        "order": order_counter,
                        "vehicle_id": vehicle_id,
                        "time_taken": round(cumulative_duration, 4)
                    }
                )

                if label2 == "Customer":
                    
                    session.run("""
                        MATCH (o:Order)-[:PLACED_BY]->(c:Customer {Customer_ID: $cust_id})
                        SET o.Status = 'scheduled',
                            o.time_taken = $time_taken
                    """, {
                        "cust_id": id2,
                        "time_taken": round(cumulative_duration, 4)
                    })

                order_counter += 1

        session.run("""
            MATCH ()-[r:IN_CLUSTER]->()
            DELETE r
        """)
            
        vehicle['current_warehouse']= return_warehouse_id
        vehicle['time_taken']=round(cumulative_duration, 4)
        return vehicle
            
    def route_external_deliveries(session):  
        
        vehicles = session.execute_read(lambda tx: [
            {"vehicle_id": record["vehicle_id"], 
             "current_warehouse": record["warehouse_id"],
             "time_taken": record["time_taken"],
             "active": True} 
            for record in tx.run("""
                MATCH (v:Vehicle)-[r:ROUTE_TO]->(w:Warehouse)
                WITH v, MAX(r.order) AS max_order
                MATCH (v)-[r2:ROUTE_TO]->(w:Warehouse)
                WHERE r2.order = max_order
                RETURN v.Vehicle_ID AS vehicle_id, w.Warehouse_ID AS warehouse_id,r2.time_taken AS time_taken
            """)
        ])      
       
        warehouses = session.execute_read(lambda tx: [
            {"warehouse_id": record["warehouse_id"], 
             "location": record["location"]}
            for record in tx.run("""
                MATCH (w:Warehouse)
                RETURN w.Warehouse_ID AS warehouse_id, w.location AS location
            """)
        ])
        
        iteration = 0
        while True:
            iteration += 1
            print(f"\n=== Iteration {iteration} ===")
            
            pending_orders=get_all_pending_orders(session)            
            if pending_orders is None:
                print("No more pending orders - routing complete!")
                break           
            print(f"Remaining pending orders: {len(pending_orders)}")
            
            vehicles=update_current_warehouse(session,vehicles)
            print(vehicles)
            
            vehicles.sort(key=lambda v: v.get("time_taken", 0))
            for vehicle_data in vehicles:
                updated_vehicle_data = tsp(session,vehicle_data)
                vehicle_data.update(updated_vehicle_data)
            
          
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
    
    with driver.session(database=DATABASE) as session:
        route_external_deliveries(session)


external()

