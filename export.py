from setup_connection import exportCursor, exportConnection
import csv

# Export Data from Location Hierarchy table in file format
get_location_hierarchy = 'SELECT * FROM Dim_Location_Hierarchy'
exportCursor.execute(get_location_hierarchy)

result = exportCursor.fetchall()

with open('location_hierarchy.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([i[0] for i in exportCursor.description])
    for row in result:
        writer.writerow(row)

# Export Data from Sales table in file format
get_sales = 'SELECT * FROM F_SALES'
exportCursor.execute(get_sales)

result = exportCursor.fetchall()

with open('sales.csv', 'w+', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([i[0] for i in exportCursor.description])
    for row in result:
        writer.writerow(row)

exportConnection.close()
