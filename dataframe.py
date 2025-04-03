import pandas as pd
from opencage.geocoder import OpenCageGeocode

api_key = "0e99e320879e4bad82952d146cd7494d"
geocoder = OpenCageGeocode(api_key)


datasets = {    
    r"D:/sem 6/big data lab/bd_dataset/vehicle.csv": "Current_Location",  
    r"D:/sem 6/big data lab/bd_dataset/warehouse.csv": "Address",
    r"D:/sem 6/big data lab/bd_dataset/customer.csv": "Address"     
}


modified_dataframes = {}

def get_coordinates(address):
    
    if pd.isna(address) or address.strip() == "":
        return None, None  
    location = geocoder.geocode(address)
    if location:
        return location[0]['geometry']['lat'], location[0]['geometry']['lng']
    return None, None


for file, address_column in datasets.items():
    df = pd.read_csv(file)      
    
    if address_column in df.columns:
        df[["Latitude", "Longitude"]] = df[address_column].apply(lambda x: pd.Series(get_coordinates(x)))
    else:
        print(f"Column '{address_column}' not found in {file}. Skipping.")
    
    print("Done for", file)
    modified_dataframes[file] = df  


for file_path, df in modified_dataframes.items():
    new_file_path = file_path.replace(".csv", "_updated.csv") 
    df.to_csv(new_file_path, index=False)  
    print(f"Saved: {new_file_path}")
