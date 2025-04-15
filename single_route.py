import openrouteservice
import folium
import polyline  # To decode the polyline geometry

# Define API key (Replace with your own key)
API_KEY = "5b3ce3597851110001cf6248b2b74b27a89f4836aa6790441684f9ea"

# Create an OpenRouteService client
client = openrouteservice.Client(key=API_KEY)

# Define coordinates (longitude, latitude) - OpenRouteService uses (lon, lat) format
coords = [(8.34234, 48.23424), (8.34423, 48.26424)]

# Request route directions
routes = client.directions(coords, profile='driving-car', format='geojson')

# Extract geometry (encoded polyline)
geometry = routes["features"][0]["geometry"]["coordinates"]

# Convert to (latitude, longitude) for folium
route_coords = [(lat, lon) for lon, lat in geometry]  # Folium needs (lat, lon)

# Create a Folium map centered at the start point
m = folium.Map(location=route_coords[0], zoom_start=14)

# Add route as a polyline
folium.PolyLine(route_coords, color="blue", weight=5, opacity=0.7).add_to(m)

# Add start and end markers
folium.Marker(route_coords[0], popup="Start", icon=folium.Icon(color="green")).add_to(m)
folium.Marker(route_coords[-1], popup="End", icon=folium.Icon(color="red")).add_to(m)

# Save map to an HTML file and display
m.save("route_map.html")
print("Map saved as route_map.html. Open it in a browser.")
