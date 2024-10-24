import geopandas as gpd
import yaml
import matplotlib.pyplot as plt
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Configuration file', default='configs.yaml')
    args = parser.parse_args()
    config_file = args.config

    if config_file is None:
        raise ValueError("Please provide a configuration file")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        master_dates_path = config['master_dates_path']

    # read the master dates geopackage and plot the data
    master_dates_df = gpd.read_file(master_dates_path.replace('parquet', 'gpkg'))
    countries = gpd.read_file('/Users/ldp/Downloads/adm0_polygons.gpkg')
    print(master_dates_df.head(10))

    # make a map of the geometry column of master_dates_df
    # Create the map
    fig, ax = plt.subplots(figsize=(10, 8))
    countries.boundary.plot(ax=ax, color='lightgray')
    master_dates_df.plot(ax=ax, color='blue')

    # Add title and labels
    plt.title('Global Map')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    # Adjust the plot limits to fit the entire world
    plt.xlim(-180, 180)
    plt.ylim(-90, 90)

    # Show the plot
    plt.show()

    #save the plot as a png
    fig.savefig('global_map.png')