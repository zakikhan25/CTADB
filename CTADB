from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB 
connection_url = "your_url_here"
client = MongoClient(connection_url)
db = client["comp_353_5"]

# Bus class 
class bus:
    def __init__(self, bID, bStopNum):
        self.bID = bID
        self.bStopNum = bStopNum
    
    def save(self):
        bus_data = {
            "bID": self.bID,
            "bStopNum": self.bStopNum
        }
        db.bus.insert_one(bus_data)

# Bus schedule class 
class bus_schedule:
    def __init__(self, bDirection, bRoute, bID, bArrivalTime, bDepartureTime, bNum):
        self.bDirection = bDirection
        self.bID = bID
        self.bNum = bNum
        self.bRoute = bRoute
        self.bArrivalTime = bArrivalTime
        self.bDepartureTime = bDepartureTime
    
    def save(self):
        bus_schedule_data = {
            "bDirection": self.bDirection,
            "bID": self.bID,
            "bNum": self.bNum,
            "bRoute": self.bRoute,
            "bArrivalTime": self.bArrivalTime,
            "bDepartureTime": self.bDepartureTime
        }
        db.bus_schedule.insert_one(bus_schedule_data)

# Query get_buses_for_station
def get_buses_for_station(station_name):
    """
    Gets buses scheduled for a specific station
    Output: bID, bRoute, bDirection, arrival_time, departure_time
    Relations: bus_schedule, bus
    """
    pipeline = [
        {
            "$addFields": {
                "stopNames": {"$map": {"input": "$bRoute", "as": "stop", "in": "$$stop.sName"}}
            }
        },
        {"$match": {"stopNames": station_name}},
        {"$lookup": {
            "from": "bus",
            "localField": "bID",
            "foreignField": "bID",
            "as": "bus_details"
        }},
        {"$unwind": "$bus_details"},
        {
            "$project": {
                "_id": 0,
                "bID": 1,
                "bRoute": 1,
                "bDirection": 1,
                "arrival_time": {
                    "$let": {
                        "vars": {
                            "idx": {"$indexOfArray": ["$stopNames", station_name]}
                        },
                        "in": {"$arrayElemAt": ["$bArrivalTime", "$$idx"]}
                    }
                },
                "departure_time": {
                    "$let": {
                        "vars": {
                            "idx": {"$indexOfArray": ["$stopNames", station_name]}
                        },
                        "in": {"$arrayElemAt": ["$bDepartureTime", "$$idx"]}
                    }
                },
                "bStopNum": "$bus_details.bStopNum"
            }
        }
    ]
    
    results = list(db.bus_schedule.aggregate(pipeline))
    
    print(f"\nBuses scheduled for {station_name} station:")
    for bus in results:
        print(f"\nBus ID: {bus['bID']}")
        print(f"Route: {bus['bRoute']}")
        print(f"Direction: {bus['bDirection']}")
        print(f"Arrival: {bus['arrival_time'].strftime('%Y-%m-%d %H:%M')}")
        print(f"Departure: {bus['departure_time'].strftime('%Y-%m-%d %H:%M')}")
        print(f"Total stops: {bus['bStopNum']}")
        print("----------------------------")

# Sample data + query
if __name__ == "__main__":
    # Clear collections first (so it doesn't duplicate every time you run it)
    db.bus.delete_many({})
    db.bus_schedule.delete_many({})

    # Insert buses
    buses = [
        bus("001", 3),
        bus("002", 3),
        bus("003", 2),
        bus("004", 2)
    ]
    for b in buses:
        b.save()
    
    # Insert schedules
    schedules = [
        bus_schedule(
            "west", "155", "001",
            [{"sName": "Howard"}, {"sName": "Dempster-Skokie"}],
            [datetime(2025, 4, 14, 12, 10), datetime(2025, 4, 14, 12, 25)],
            [datetime(2025, 4, 14, 12, 12), datetime(2025, 4, 14, 12, 27)],
            "155"
        ),
        bus_schedule(
            "east", "155", "002",
            [{"sName": "Dempster-Skokie"}, {"sName": "Howard"}],
            [datetime(2025, 4, 14, 13, 0), datetime(2025, 4, 14, 13, 15)],
            [datetime(2025, 4, 14, 13, 5), datetime(2025, 4, 14, 13, 20)],
            "155"
        )
    ]
    for s in schedules:
        s.save()
    
    # Run query
    get_buses_for_station("Howard")
