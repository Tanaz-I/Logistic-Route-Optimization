
def local_routing():
    
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
        
    def assign_vehicles_to_nearest_warehouse(session):      
            
        vehicles = session.execute_read(lambda tx: [
            {"vehicle_id": record["vehicle_id"], "location": record["location"]}
            for record in tx.run("MATCH (v:Vehicle) RETURN v.Vehicle_ID AS vehicle_id, v.location AS location")
        ])

        warehouses = session.execute_read(lambda tx: [
            {"warehouse_id": record["warehouse_id"], "location": record["location"]}
            for record in tx.run("MATCH (w:Warehouse) RETURN w.Warehouse_ID AS warehouse_id, w.location AS location")
        ])

        for vehicle in vehicles:
            v_loc = vehicle["location"]
            v_coords = (v_loc.x, v_loc.y)

            min_distance = float("inf")
            closest_warehouse = None
            duration_for_closest = None

            for warehouse in warehouses:
                w_loc = warehouse["location"]
                w_coords = (w_loc.x, w_loc.y)

                try:
                    distance, duration = route_between_two_points([v_coords, w_coords])
                    if distance < min_distance:
                        min_distance = distance
                        duration_for_closest = duration
                        closest_warehouse = warehouse
                except:
                    continue

            if closest_warehouse:
                warehouse_id = closest_warehouse["warehouse_id"]
                vehicle_id = vehicle["vehicle_id"]
                distance_km = round(min_distance / 1000, 2)
                duration_hr = round(duration_for_closest / 3600, 2)  

                def create_route(tx):
                    query = """
                    MATCH (v:Vehicle {Vehicle_ID: $vehicle_id})
                    MATCH (w:Warehouse {Warehouse_ID: $warehouse_id})
                    MERGE (v)-[r:ROUTE_TO]->(w)
                    SET r.vehicle_id = $vehicle_id,
                        r.order = 1,
                        r.distance = $distance_km,
                        r.duration = $duration_hr,
                        r.time_taken = $duration_hr
                    """
                    tx.run(query, vehicle_id=vehicle_id, warehouse_id=warehouse_id, distance_km=distance_km, duration_hr=duration_hr)

                session.execute_write(create_route)
               

    def create_projection(session,distance_threshold_km=30):
        
            query = """
            CALL gds.graph.project.cypher(
                'localDeliveryGraph',
                $nodeQuery,
                $relQuery,
                { parameters: { threshold: $threshold } }
            )
            YIELD graphName, nodeCount, relationshipCount, projectMillis
            RETURN graphName, nodeCount, relationshipCount, projectMillis
            """
            
            node_query = """
            MATCH (n) 
            WHERE n:Warehouse OR n:Customer 
            RETURN id(n) AS id, labels(n) AS labels
            """
            
            rel_query = """
            MATCH (w:Warehouse)-[r:DELIVERS_TO]->(c:Customer) 
            WHERE r.distance <= $threshold 
            RETURN id(w) AS source, id(c) AS target, r.distance AS distance
            """
            
            result = session.run(query, 
                                nodeQuery=node_query,
                                relQuery=rel_query,
                                threshold=distance_threshold_km)
            stats = result.single()
            
            return stats
        
    def apply_wcc_clustering(session):        
            result = session.run("""
                CALL gds.wcc.write('localDeliveryGraph', {
                    writeProperty: 'local_cluster'
                })
                YIELD nodePropertiesWritten, componentCount
            """)
            summary = result.single()        
            session.run("CALL gds.graph.drop('localDeliveryGraph')")           
    
        
    def local_cluster(session): 
        
            
            query = """
        MATCH (c:Customer)
        RETURN c.Customer_ID AS customer_id, 
            c.location AS location, 
            c.local_cluster AS cluster_id
            """
            customers = session.execute_read(lambda tx: 
            [ 
                {
                "customer_id": record["customer_id"],
                "location": record["location"],
                "cluster_id": record["cluster_id"]
                }
                for record in tx.run(query)
            ]
            )

            from collections import defaultdict

            
            clusters = defaultdict(list)
            for customer in customers:
                clusters[customer["cluster_id"]].append(customer)

            failed_connections = []

            for cluster_id, cluster_customers in clusters.items():
                for i in range(len(cluster_customers)):
                    for j in range(i + 1, len(cluster_customers)):
                        cust1 = cluster_customers[i]
                        cust2 = cluster_customers[j]

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
                            print(f"Failed to connect C{cust1['customer_id']} <-> C{cust2['customer_id']}: {str(e)}")
                            failed_connections.append({
                                "customer1": cust1["customer_id"],
                                "customer2": cust2["customer_id"]
                            })
            
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
        return (record["dist"], record["dur"]) if record else (float("inf"), float("inf"))
    
    def local_routes_with_tsp(session):
        
        cluster_ids_result = session.run("""
            MATCH (n) WHERE n.local_cluster IS NOT NULL
            RETURN DISTINCT n.local_cluster AS cluster_id
        """)
        cluster_ids = [record["cluster_id"] for record in cluster_ids_result]

        for cluster_id in cluster_ids:
            result = session.run("""
                MATCH (w:Warehouse)<-[:SHIPPED_FROM]-(o:Order)-[:PLACED_BY]->(c:Customer)
                WHERE w.local_cluster = $cid AND c.local_cluster = $cid
                RETURN DISTINCT w.Warehouse_ID AS warehouse_id, collect(DISTINCT c.Customer_ID) AS customers
            """, cid=cluster_id)

            record = result.single()
            if not record:
                continue

            warehouse_id = record["warehouse_id"]
            customers = record["customers"]

            vehicle_result = session.run("""
                MATCH (v:Vehicle)-[:ROUTE_TO]->(w:Warehouse {Warehouse_ID: $wid})
                RETURN v.Vehicle_ID AS vehicle_id
            """, wid=warehouse_id)

            vehicle_record = vehicle_result.single()
            if not vehicle_record:
                continue

            vehicle_id = vehicle_record["vehicle_id"]

            order_result = session.run("""
                MATCH (:Vehicle {Vehicle_ID: $vid})-[r:ROUTE_TO]->()
                RETURN coalesce(max(r.order), 0) AS max_order,
                    coalesce(max(r.time_taken), 0) AS max_time
            """, vid=vehicle_id)

            vehicle_data = order_result.single()
            order_counter = vehicle_data["max_order"] + 1
            cumulative_duration = vehicle_data["max_time"] if vehicle_data else 0

        
            unvisited = set(customers)
            route = []
            current = warehouse_id
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

            full_route = [("Warehouse", warehouse_id)] + [("Customer", cid) for cid in route] + [("Warehouse", warehouse_id)]
            full_route.append(("Warehouse", warehouse_id))
            
            for i in range(len(full_route) - 1):
                label1, id1 = full_route[i]
                label2, id2 = full_route[i + 1]
                is_from_warehouse = label1 == "Warehouse"
                to_warehouse = label2 == "Warehouse"
                dist, dur = get_edge_data(session, id1, id2, is_from_warehouse,to_warehouse)
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
                        SET c.time_taken = $time_taken,
                            o.Status = 'scheduled',
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
        assign_vehicles_to_nearest_warehouse(session)
        create_projection(session,distance_threshold_km=30) 
        apply_wcc_clustering(session)
        local_cluster(session)
        local_routes_with_tsp(session)


local_routing()      


