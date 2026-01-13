from pymongo import MongoClient
from datetime import datetime
import pprint

# Σύνδεση στη MongoDB Atlas

client = MongoClient("mongodb+srv://<USERNAME>:<PASSWORD>@cluster0.n6928z1.mongodb.net/")
db = client["bigdata"]
raw = db["raw_events"]

start = datetime(2025, 6, 13, 0, 0, 0)
end = datetime(2025, 6, 13, 23, 59, 59)

pp = pprint.PrettyPrinter(indent=2)

# Ερώτημα 1: Πόλη με τις περισσότερες κρατήσεις
result1 = raw.aggregate([
    {
        "$match": {
            "timestamp": {"$gte": start, "$lte": end},
            "event_type": "booking",
            "location": {"$exists": True, "$ne": None}
        }
    },
    {"$group": {"_id": "$location", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
print("Περισσότερες κρατήσεις:")
pp.pprint(list(result1))

# Ερώτημα 2: Πόλη με τις περισσότερες αναζητήσεις
result2 = raw.aggregate([
    {
        "$match": {
            "timestamp": {"$gte": start, "$lte": end},
            "event_type": "search",
            "location": {"$exists": True, "$ne": None}
        }
    },
    {"$group": {"_id": "$location", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
print("\nΠερισσότερες αναζητήσεις:")
pp.pprint(list(result2))

# Ερώτημα 3: Μέση διάρκεια παραμονής ανά πόλη
bookings = raw.find({
    "timestamp": {"$gte": start, "$lte": end},
    "event_type": "booking",
    "check_in_date": {"$exists": True, "$ne": None},
    "check_out_date": {"$exists": True, "$ne": None},
    "location": {"$exists": True, "$ne": None}
})

stay_data = {}

for b in bookings:
    try:
        loc = b["location"]
        check_in = datetime.fromisoformat(b["check_in_date"])
        check_out = datetime.fromisoformat(b["check_out_date"])
        stay_length = (check_out - check_in).days
        if stay_length < 0:
            continue
        if loc in stay_data:
            stay_data[loc]["total_days"] += stay_length
            stay_data[loc]["count"] += 1
        else:
            stay_data[loc] = {"total_days": stay_length, "count": 1}
    except Exception as e:
        continue

print("\nΜέση διάρκεια παραμονής ανά πόλη:")
for loc, stats in stay_data.items():
    avg = stats["total_days"] / stats["count"]
    print(f"{loc}: {avg:.2f} μέρες")
