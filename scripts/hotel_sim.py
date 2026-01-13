import random
import datetime
import uuid
import json

# --- Configuration ---
NUM_USERS = 1000  # Number of unique users to simulate
SESSIONS_PER_USER = (1, 5)  # Range of sessions per user
ACTIONS_PER_SESSION = (5, 30)  # Range of actions per session
TIME_BETWEEN_ACTIONS_SECONDS = (1, 600)  # Range of seconds between actions
MAX_HOTELS_VIEWED_AFTER_SEARCH = 5 # Maximum number of hotels a user might view after search

# Possible locations for searches
LOCATIONS = ["New York", "London", "Paris", "Tokyo", "Rome", "Dubai", "Singapore", "Sydney", "Rio de Janeiro"]

# Simulate a range of hotel IDs
HOTEL_IDS = [f"HOTEL_{i:04d}" for i in range(1, 501)]

# Simulate a range of room types
ROOM_TYPES = ["Standard", "Deluxe", "Suite", "Family Room"]

# Base URLs for the site
BASE_URL = "https://hotelbooking.example.com"

# Define possible event types and their relative probabilities
# Removed 'check_availability' as per request
EVENT_TYPES = {
    "page_view": 0.5,
    "search_hotels": 0.2,
    "view_hotel_details": 0.15,
    "add_to_cart": 0.05, # Increased probability slightly after removing check_availability
    "start_checkout": 0.02,
    "complete_booking": 0.01,
    "cancel_booking": 0.005,
    "view_booking_history": 0.01,
    "remove_from_cart": 0.005,
    "view_cart": 0.01,
    "logout": 0.01
}

# Define simplified user flows (sequences of event types)
# Probabilities for choosing a flow (sum should be 1)
FLOW_PROBABILITIES = {
    "browse_and_book": 0.4,
    "browse_and_leave": 0.3,
    #"view_details_and_book": 0.15, # This flow still starts with viewing details directly
    #"repeat_visitor_history": 0.1,
    "quick_exit": 0.3 # Users who leave quickly
}

# Define the sequences for each flow
# Modified browse_and_book and browse_and_leave to allow multiple hotel views
# Removed check_availability from all flows
USER_FLOWS = {
    "browse_and_book": [
        "page_view", "search_hotels", # Multiple view_hotel_details will be inserted here
        "add_to_cart", "view_cart", "start_checkout", "add_payment_info", "complete_booking", "logout"
    ],
    "browse_and_leave": [
        "page_view", "search_hotels", # Multiple view_hotel_details will be inserted here
        "logout"
    ],
    #"view_details_and_book": [
    #    "page_view", "view_hotel_details", "add_to_cart", # Removed check_availability
    #    "start_checkout", "complete_booking", "logout"
    #],
    # "repeat_visitor_history": [
    #    "page_view", "view_booking_history", "logout"
    #],
    "quick_exit": [
        "page_view", "logout"
    ]
}

# --- Helper Functions ---

def generate_event_details(event_type, context=None):
    """Generates realistic details for each event type, using context from previous events."""
    details = {}
    if event_type == "page_view":
        pages = ["/"]#, "/search", "/hotel", "/cart", "/checkout", "/bookings"]
        details["page_url"] = BASE_URL + random.choice(pages)
    elif event_type == "search_hotels":
        details["location"] = random.choice(LOCATIONS)
        start_date = datetime.date.today() + datetime.timedelta(days=random.randint(1, 365))
        end_date = start_date + datetime.timedelta(days=random.randint(1, 14))
        details["check_in_date"] = start_date.strftime("%Y-%m-%d")
        details["check_out_date"] = end_date.strftime("%Y-%m-%d")
        details["num_guests"] = random.randint(1, 4)
    elif event_type == "view_hotel_details":
        # If context from a search exists, maybe pick from potential results.
        # For simplicity here, we'll just pick a random hotel ID.
        # In a real scenario, this would ideally be linked to the search results.
        details["hotel_id"] = random.choice(HOTEL_IDS)
        details["page_url"] = f"{BASE_URL}/hotel/{details['hotel_id']}"
    elif event_type == "add_to_cart":
        # If context from viewing hotel details exists, use that hotel.
        # Otherwise, pick a random hotel (less realistic but handles edge cases).
        if context and "last_viewed_hotel_id" in context:
            details["hotel_id"] = context["last_viewed_hotel_id"]
            details["check_in_date"] = context.get("search_check_in_date") # Try to use search dates if available
            details["check_out_date"] = context.get("search_check_out_date")
            details["location"] = context.get("search_location")
        else: # Fallback if no recent view_hotel_details context
            details["hotel_id"] = random.choice(HOTEL_IDS)
            start_date = datetime.date.today() + datetime.timedelta(days=random.randint(1, 365))
            end_date = start_date + datetime.timedelta(days=random.randint(1, 14))
            details["check_in_date"] = context.get("search_check_in_date")
            details["check_out_date"] = context.get("search_check_out_date")
            details["location"] = context.get("search_location")

        details["room_type"] = random.choice(ROOM_TYPES) # Assume a room type is selected
        details["item_id"] = str(uuid.uuid4()) # Unique ID for cart item
        details["price"] = round(random.uniform(50, 500), 2) # Simulate price
        details["page_url"] = f"{BASE_URL}/cart"
    elif event_type == "remove_from_cart":
        # In a real scenario, we'd track items in the cart. Here, we'll just simulate removing a random item ID.
        details["item_id"] = str(uuid.uuid4())
        details["page_url"] = f"{BASE_URL}/cart"
    elif event_type == "view_cart":
        details["page_url"] = f"{BASE_URL}/cart"
    elif event_type == "start_checkout":
        details["page_url"] = f"{BASE_URL}/checkout"
    elif event_type == "add_payment_info":
        details["payment_method"] = random.choice(["credit_card", "paypal", "bank_transfer"])
        details["page_url"] = f"{BASE_URL}/checkout/payment"
    elif event_type == "complete_booking":
        details["booking_id"] = str(uuid.uuid4()) # Unique booking ID
        # Simulate total price - ideally sum of cart items
        details["total_price"] = round(random.uniform(100, 2000), 2)
        details["page_url"] = f"{BASE_URL}/booking/confirmation/{details['booking_id']}"
        details["check_in_date"] = context.get("search_check_in_date")
        details["check_out_date"] = context.get("search_check_out_date")
        details["location"] = context.get("search_location")
        details["hotel_id"] = context.get("last_viewed_hotel_id")
    elif event_type == "cancel_booking":
        details["booking_id"] = str(uuid.uuid4()) # Simulate cancelling a random booking ID
        details["page_url"] = f"{BASE_URL}/booking/cancel"
    elif event_type == "view_booking_history":
        details["page_url"] = f"{BASE_URL}/bookings"
    elif event_type == "logout":
        details["page_url"] = f"{BASE_URL}/logout"

    return details

def simulate_user_session(user_id, session_id, start_time):
    """Simulates a single user session."""
    clickstream_events = []
    current_time = start_time
    context = {} # Dictionary to pass context like search details or last viewed hotel ID

    # Choose a user flow based on probabilities
    chosen_flow_name = random.choices(list(USER_FLOWS.keys()), weights=list(FLOW_PROBABILITIES.values()), k=1)[0]
    chosen_flow = USER_FLOWS[chosen_flow_name]
    #print(chosen_flow_name, chosen_flow)

    # Decide on the actual number of actions, ensuring it's within the flow length
    # This needs adjustment because view_hotel_details count is dynamic
    # Let's simulate the flow sequence and add time between steps

    for i, event_type in enumerate(chosen_flow):
        # Add random time delay between actions (except for the very first action)
        if i > 0 or (i == 0 and len(clickstream_events) > 0): # Ensure delay after first event too if flow starts with non-page_view
             time_delta = datetime.timedelta(seconds=random.randint(*TIME_BETWEEN_ACTIONS_SECONDS))
             current_time += time_delta

        if event_type == "search_hotels":
            # Generate search details and store them in context
            details = generate_event_details(event_type, context)
            context["search_location"] = details.get("location")
            context["search_check_in_date"] = details.get("check_in_date")
            context["search_check_out_date"] = details.get("check_out_date")
            context["search_num_guests"] = details.get("num_guests")

            event = {
                "user_id": user_id,
                "session_id": session_id,
                "timestamp": current_time.isoformat(),
                "event_type": event_type,
                "event_details": details
            }
            clickstream_events.append(event)

            # After search, simulate viewing a random number of hotel details
            num_hotels_to_view = random.randint(0, MAX_HOTELS_VIEWED_AFTER_SEARCH)
            viewed_hotel_ids = [] # Keep track of viewed hotels in this sequence

            for _ in range(num_hotels_to_view):
                time_delta = datetime.timedelta(seconds=random.randint(5, 120)) # Shorter time between viewing details
                current_time += time_delta
                view_details_event_type = "view_hotel_details"
                details = generate_event_details(view_details_event_type, context)
                viewed_hotel_ids.append(details.get("hotel_id")) # Store viewed hotel ID

                event = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "timestamp": current_time.isoformat(),
                    "event_type": view_details_event_type,
                    "event_details": details
                }
                clickstream_events.append(event)

            # Update context with the last viewed hotel ID if any were viewed
            if viewed_hotel_ids:
                 context["last_viewed_hotel_id"] = viewed_hotel_ids[-1]
            else:
                 context.pop("last_viewed_hotel_id", None) # Remove if no hotels were viewed

        else:
            # For other event types, generate details using the current context
            details = generate_event_details(event_type, context)
            event = {
                "user_id": user_id,
                "session_id": session_id,
                "timestamp": current_time.isoformat(),
                "event_type": event_type,
                "event_details": details
            }
            clickstream_events.append(event)

            # Update context based on the event type if needed (e.g., clear context after booking)
            if event_type in ["complete_booking", "cancel_booking", "logout"]:
                context = {} # Clear context after session-ending events

    return clickstream_events

def simulate_clickstream(num_users, ndays):
    """Simulates clickstream data for multiple users."""
    all_clickstream_events = []
    start_time = datetime.datetime.now() - datetime.timedelta(days=ndays) # Start simulation 30 days ago

    for i in range(num_users):
        user_id = f"user_{i+1:05d}"
        num_sessions = random.randint(*SESSIONS_PER_USER)

        for j in range(num_sessions):
            session_id = f"session_{user_id}_{j+1:03d}_{uuid.uuid4().hex[:6]}"
            # Start subsequent sessions later
            session_start_time = start_time + datetime.timedelta(days=random.randint(0, ndays-1), seconds=random.randint(0, 86400))
            session_events = simulate_user_session(user_id, session_id, session_start_time)
            all_clickstream_events.extend(session_events)

    # Sort events by timestamp (optional, but often useful)
    all_clickstream_events.sort(key=lambda x: x["timestamp"])

    return all_clickstream_events

# --- Main Execution ---
if __name__ == "__main__":
    print(f"Simulating clickstream data for {NUM_USERS} users...")
    clickstream_data = simulate_clickstream(NUM_USERS,ndays=10)

    print(f"Generated {len(clickstream_data)} clickstream events.")

    # --- Output Data ---
    # You can choose to output to a file (e.g., JSON Lines or CSV)
    output_format = "csv" #"json_lines" # or "csv"

    if output_format == "json_lines":
        output_filename = "hotel_clickstream.jsonl"
        with open(output_filename, "w") as f:
            for event in clickstream_data:
                f.write(json.dumps(event) + "\n")
        print(f"Clickstream data saved to {output_filename} (JSON Lines format).")

    elif output_format == "csv":
        # For CSV, we need to flatten the event_details dictionary
        output_filename = "hotel_clickstream.csv"
        import csv

        # Determine all possible keys in event_details for the header
        all_detail_keys = set()
        for event in clickstream_data:
            all_detail_keys.update(event["event_details"].keys())

        header = ["user_id", "session_id", "timestamp", "event_type"] + sorted(list(all_detail_keys))

        with open(output_filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for event in clickstream_data:
                row = {k: event[k] for k in ["user_id", "session_id", "timestamp", "event_type"]}
                # Add event_details, handling missing keys
                for key in all_detail_keys:
                    row[key] = event["event_details"].get(key, "") # Use empty string for missing details
                writer.writerow(row)
        print(f"Clickstream data saved to {output_filename} (CSV format).")

    else:
        print("Invalid output format specified.")

    # Optional: Print a few sample events
    print("\nSample Events:")
    for i in range(min(10, len(clickstream_data))):
        print(json.dumps(clickstream_data[i], indent=2))

