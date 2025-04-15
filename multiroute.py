import openrouteservice
import folium

API_KEY = "5b3ce3597851110001cf6248b2b74b27a89f4836aa6790441684f9ea"
client = openrouteservice.Client(key=API_KEY)

# Coordinates: (lon, lat) — including intermediate stops
coords = [
    (8.34234, 48.23424),  # Start
    (8.34300, 48.24000),  # Stop 1
    (8.34400, 48.25000),  # Stop 2
    (8.34423, 48.26424)   # End
]

# Request directions
routes = client.directions(coords, profile='driving-car', format='geojson')
geometry = routes["features"][0]["geometry"]["coordinates"]

# Convert to (lat, lon) for Folium
route_coords = [(lat, lon) for lon, lat in geometry]

# Initialize map
m = folium.Map(location=route_coords[0], zoom_start=14)

# Draw route
folium.PolyLine(route_coords, color="blue", weight=5, opacity=0.7).add_to(m)

# Mark all stops
for i, (lon, lat) in enumerate(coords):
    if i == 0:
        popup = "Start"
        icon = folium.Icon(color="green")
    elif i == len(coords) - 1:
        popup = "End"
        icon = folium.Icon(color="red")
    else:
        popup = f"Stop {i}"
        icon = folium.Icon(color="blue", icon="info-sign")
    
    folium.Marker([lat, lon], popup=popup, icon=icon).add_to(m)

# Save and show
m.save("route_with_stops.html")
print("Map saved as route_with_stops.html. Open it in a browser.")
