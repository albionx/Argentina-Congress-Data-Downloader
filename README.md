# Argentina Congress Data Downloader

This script automatically retrieves the following datasets from the House of Representatives in Argentina:
1. COVID19 Subsidies
2. Laws
3. House of Representatives Sessions (Diputados)
4. List of Representatives (Diputados)

## Features
- The script automatically creates a SQLite database and allows for downloading any of the above datasets
- Support for API pagination
- Data schema detection. Tables are created dynamically following the specifications in the JSON API. 

## Known limitations
- The script will not work if the resource handlers in the Open Data government website change. Updating them on the script is trivial.