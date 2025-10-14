import requests
from datetime import datetime, timedelta, timezone
from opensearchpy import OpenSearch
import json
import random
import time
import math

# API Configuration
API_BASE_URL = "https://api.demo.truckit.com"
USERNAME = "support_sales_demos"
PASSWORD = "welcome"

# OpenSearch Configuration
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_HOST = 'vpc-stack-truckit-es7-r55zgy5aqm24i6tabwb6zcejcu.us-east-1.es.amazonaws.com'
ES_AUTH = ('master', 'C=BU42NWyUW2IjQsK0eCU95')

# Truck definitions - must match trucks that exist in the system
TRUCKS = [
    {"id": 575123, "device_name": "Truck1", "idle_threshold": 20.0},
    {"id": 575124, "device_name": "Truck2", "idle_threshold": 22.0},
    {"id": 575125, "device_name": "Truck3", "idle_threshold": 0.0},
]

# Company ID for Sales Demos
COMPANY_ID = 2879

# Event type constants
EVENT_TYPE_ENTERED = "entered"
EVENT_TYPE_LEFT = "left"

# Route coordinates
PICKUP_COORDS = {"lat": 34.888100, "lng": -79.706100}  # 153 Eddies Lane, Hamlet, NC
DROPOFF_COORDS = {"lat": 32.854622, "lng": -79.974808}  # 1981 Harley St, North Charleston, SC

# Global variable to store auth token
AUTH_TOKEN = None

# Initialize OpenSearch client
es_client = OpenSearch(
    hosts=[{'host': ES_HOST, 'port': 443}],
    http_auth=ES_AUTH,
    use_ssl=True,
    verify_certs=True
)


def authenticate_without_device():
    """Authenticate using standard method without device info."""
    auth_data = {
        "username": USERNAME,
        "password": PASSWORD
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        print("🔑 Authenticating (without device)...")
        response = requests.post(
            f"{API_BASE_URL}/api/2/signin",
            json=auth_data,
            headers=headers
        )

        if response.status_code == 200:
























            auth_response = response.json()
            token = auth_response.get("authToken")
            if token:
                print("✅ Authenticated without device.")
                return token
            else:
                print("❌ Token not found in response")
        else:
            print(f"❌ Auth failed (no device). Status: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"❌ Error during no-device auth: {e}")
    return None


def authenticate_with_device():
    """Authenticate simulating mobile device with proper fields."""
    auth_data = {
        "username": USERNAME,
        "password": PASSWORD,
        "deviceId": "817C344B-E6DB-4090-939C-0D804E91426A",
        "deviceName": "iPhone 12 mini"
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        print("📱 Authenticating with mobile device...")

        response = requests.post(
            f"{API_BASE_URL}/api/2/signin",
            json=auth_data,
            headers=headers,
            timeout=15
        )

        if response.status_code == 200:
            auth_response = response.json()
            token = auth_response.get("authToken")
            if token:
                print("✅ Authenticated with device")
                return token
            else:
                print("❌ Token not found in mobile auth response")
        else:
            print(f"❌ Auth with device failed. Status: {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"❌ Error during device auth: {e}")

    return None




def get_truck_regions():
    """Fetch truck regions for all trucks from the correct API endpoint"""
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return []

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    all_regions = []
    seen_regions = set()

    # Get regions for each truck in our TRUCKS list
    for truck in TRUCKS:
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/1/trucks/truck-regions?truck={truck['id']}",
                headers=headers
            )

            if response.status_code == 200:
                regions_data = response.json()
                print(f"Raw response for truck {truck['id']}: {regions_data}")

                # Extract regions from the data field
                regions_list = regions_data.get("data", [])
                regions_added = 0

                for region in regions_list:
                    region_id = region.get("id")
                    if region_id and region_id not in seen_regions:
                        all_regions.append(region)
                        seen_regions.add(region_id)
                        regions_added += 1

                if regions_added > 0:
                    print(f"Found {regions_added} region(s) for truck {truck['device_name']} (ID: {truck['id']})")
                else:
                    print(f"No regions found for truck {truck['device_name']} (ID: {truck['id']})")
            else:
                print(f"Failed to get regions for truck {truck['id']}. Status: {response.status_code}")
                print(f"Response: {response.text}")

        except Exception as e:
            print(f"Error getting regions for truck {truck['id']}: {e}")

    # If no regions found for any truck, create a default region with ID 0
    if not all_regions:
        print("No regions found for any trucks. Using default region with ID 0.")
        default_region = {"id": 0, "name": "Default Region"}
        all_regions.append(default_region)

    print(f"Found {len(all_regions)} unique truck regions across all trucks")
    return all_regions


def get_trucks_with_regions():
    """Fetch trucks with their associated regions"""
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return {}

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Get all trucks for the company
        response = requests.get(
            f"{API_BASE_URL}/api/2/trucks?company_id={COMPANY_ID}",
            headers=headers
        )

        if response.status_code == 200:
            trucks_data = response.json()
            trucks = trucks_data.get("data", [])
            print(f"Found {len(trucks)} trucks")

            # Create a map of truck_id to region_id
            truck_to_region = {}
            for truck in trucks:
                truck_id = truck.get("id")
                region_id = truck.get("truck_region_id")

                # If truck has no region assigned, use default region ID 0
                if not region_id:
                    region_id = 0
                    print(f"Truck {truck_id} has no region assigned, using default region ID 0")

                truck_to_region[truck_id] = region_id

            return truck_to_region
        else:
            print(f"Failed to get trucks. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return {}

    except Exception as e:
        print(f"Error getting trucks: {e}")
        return {}


def create_job_order(pickup_region_id, dropoff_region_id):
    """Create a job order via POST endpoint with specific pickup/dropoff regions"""

    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return None, None, None, None, None

    # Get sites associated with the regions
    pickup_site_id, pickup_site_name = get_site_for_region(pickup_region_id)
    dropoff_site_id, dropoff_site_name = get_site_for_region(dropoff_region_id)

    if not pickup_site_id or not dropoff_site_id:
        print("Could not find sites for the specified regions. Using default sites.")
        pickup_site_id = 176376  # Default pickup site
        dropoff_site_ids = [176377]  # Default dropoff site
        pickup_site_name = get_site_name(pickup_site_id)
        dropoff_site_names = [get_site_name(dropoff_site_ids[0])]
    else:
        dropoff_site_ids = [dropoff_site_id]
        dropoff_site_names = [dropoff_site_name]

    # Job order data for initial creation (no trucks assigned)
    job_order_data = {
        "startDate": datetime.now(timezone.utc).isoformat(),
        "endDate": (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat(),
        "pickUpSite": pickup_site_id,
        "dropOffSites": dropoff_site_ids,
        "poLineItemId": 22627,  # Required field - using the provided value
        "totalQuantity": 100.0,  # Required field as float
        "unlimited": False,  # Required field
        "smartDispatch": False,
        "allowToNotify": False,
        "items": [
            {
                "startDate": datetime.now(timezone.utc).isoformat(),
                "quantity": 100,
                "unlimited": False,
                "autoApprove": True,
                "terms": 1,  # Required field - payment terms in days
                "trucks": [truck["id"] for truck in TRUCKS],  # Required field - empty for initial creation
                "haulers": []  # Start with empty haulers list
            }
        ],
        "jobMode": 1,  # Adjust based on your needs
        "autoAccept": True,
        "allowUnfilledStart": True,
        "requestNotes": "Job order for idle time testing",
        "notes": "Created via API for idle time alerts",
        "costCode": "DEMO001",  # Optional
        "extRef": f"DEMO-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",  # External reference
        "loadTimeSec": 900,  # 15 minutes in seconds
        "unloadTimeSec": 900,  # 15 minutes in seconds
        "backhaulAllowed": False,
        "isOvernight": False,
        "isSplit": False,
        "allowToNotify": True,
        "prohibitExceedingNumberOfRequestedTrucks": True,
    }

    # Headers for API request
    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to create job order
        response = requests.post(
            f"{API_BASE_URL}/api/2/job-orders",  # Adjust endpoint path if needed
            json=job_order_data,
            headers=headers
        )

        if response.status_code == 200 or response.status_code == 201:
            job_order = response.json()
            job_order_id = job_order.get("data", {}).get("id")  # Extract from data object
            print(f"Successfully created job order: {job_order_id}")

            # Debug: Print the full job order response to see the actual line item IDs
            print(f"Job order response structure: {json.dumps(job_order, indent=2)}")

            print(f"Pickup site: {pickup_site_name}")
            print(f"Dropoff sites: {dropoff_site_names}")

            return job_order_id, job_order, pickup_site_name, dropoff_site_names, pickup_site_id
        else:
            print(f"Failed to create job order. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None, None, None, None, None

    except Exception as e:
        print(f"Error creating job order: {e}")
        return None, None, None, None, None


def get_site_for_region(region_id):
    """Get a site associated with a region"""
    if not AUTH_TOKEN:
        return None, None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Try to find a site that belongs to this region
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/2/sites?region_id={region_id}",
            headers=headers
        )

        if response.status_code == 200:
            sites_data = response.json()
            sites = sites_data.get("data", [])
            if sites:
                site = sites[0]  # Take the first site associated with this region
                return site.get("id"), site.get("name")
            else:
                print(f"No sites found for region {region_id}")
                return None, None
        else:
            print(f"Failed to get sites for region {region_id}. Status: {response.status_code}")
            return None, None

    except Exception as e:
        print(f"Error getting sites for region: {e}")
        return None, None


def get_site_name(site_id):
    """Get site name from site ID"""
    if not AUTH_TOKEN:
        return None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            f"{API_BASE_URL}/api/2/sites/{site_id}",
            headers=headers
        )

        if response.status_code == 200:
            site_data = response.json()
            return site_data.get("name")
        else:
            print(f"Failed to get site info for ID {site_id}. Status: {response.status_code}")
            # Use a default name as fallback - this is less than ideal but better than nothing
            return f"Site_{site_id}"

    except Exception as e:
        print(f"Error getting site info: {e}")
        return f"Site_{site_id}"


def encode_region_name(region_name, company_id):
    """Encode region name in the format expected by the system: name__company_id"""
    return f"{region_name}__{company_id}"


def create_ticket(job_order_id, truck_id, jo_line_item_id, quantity, latitude=40.7128, longitude=-74.0060,
                  driver_id=None, hauler_id=None, weight=0.0, external_ref=None, drop_off_location=None):
    """
    Create a ticket via POST endpoint

    Args:
        job_order_id (int): The job order ID this ticket belongs to
        truck_id (int): The truck ID for this ticket
        jo_line_item_id (int): The job order line item ID
        quantity (float): The quantity for this ticket (minimum 0)
        latitude (float): Latitude coordinate (-90 to 90). Defaults to NYC coordinates
        longitude (float): Longitude coordinate (-180 to 180). Defaults to NYC coordinates
        driver_id (int, optional): Driver ID
        hauler_id (int, optional): Hauler ID
        weight (float): Weight for the ticket (defaults to 0.0)
        external_ref (str, optional): External reference string
        drop_off_location (int, optional): Drop off location ID

    Returns:
        tuple: (ticket_id, ticket_data) on success, (None, None) on failure
    """
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return None, None

    # Validate required coordinates
    if not (-90 <= latitude <= 90):
        print(f"Invalid latitude {latitude}. Must be between -90 and 90.")
        return None, None

    if not (-180 <= longitude <= 180):
        print(f"Invalid longitude {longitude}. Must be between -180 and 180.")
        return None, None

    # Prepare ticket data
    ticket_data = {
        "jobOrderId": job_order_id,
        "truckId": truck_id,
        "joLineItemId": jo_line_item_id,
        "quantity": quantity,
        "coordinates": {
            "latitude": latitude,
            "longitude": longitude
        },
        "weight": weight,
        "isDuplicate": False,
        "reconcile": False
    }

    # Add optional fields if provided
    if driver_id is not None:
        ticket_data["driverId"] = driver_id

    if hauler_id is not None:
        ticket_data["haulerId"] = hauler_id

    if external_ref is not None:
        ticket_data["externalRef"] = external_ref

    if drop_off_location is not None:
        ticket_data["dropOffLocation"] = drop_off_location

    # Headers for API request
    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to create ticket
        response = requests.post(
            f"{API_BASE_URL}/api/2/tickets",
            json=ticket_data,
            headers=headers
        )

        if response.status_code == 200 or response.status_code == 201:
            ticket_response = response.json()
            ticket_id = ticket_response.get("data", {}).get("id")
            if ticket_id:
                print(f"Successfully created ticket: {ticket_id}")
                print(f"  Job Order: {job_order_id}")
                print(f"  Truck: {truck_id}")
                print(f"  Quantity: {quantity}")
                print(f"  Location: ({latitude}, {longitude})")
                return ticket_id, ticket_response
            else:
                print("Ticket created but ID not found in response")
                return None, ticket_response
        else:
            print(f"Failed to create ticket. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None, None

    except Exception as e:
        print(f"Error creating ticket: {e}")
        return None, None


def create_air_ticket_lite(truck_id, truck_name, job_order_id=None, pickup_location=None):
    """
    Create an ATP Air Ticket Lite using multipart/form-data, uploading an image,
    then patching the ticket with hardcoded values (no OCR weight used).
    """
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return None, None

    import os
    from http.client import IncompleteRead

    image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "image.jpg")
    if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
        print("❌ Image not found or empty. Skipping air ticket creation.")
        return None, None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}"
    }

    # Step 1: Upload the image for data extraction (for ticket_num, payload, supplier)
    extracted_data = {}
    try:
        with open(image_path, "rb") as image_file:
            files = {
                "files": ("image.jpg", image_file, "image/jpeg")
            }
            data = {
                "jobId": str(job_order_id or 0),
                "driverId": str(0),
                "truckId": str(truck_id),
                "longitude": "0.0",
                "latitude": "0.0",
                "Content-Type": "image/jpeg"
            }

            upload_response = requests.post(
                "https://tptest.truckit.com/uploadImage",
                data=data,
                files=files,
                headers=headers,
                timeout=30
            )

        if upload_response.status_code == 200:
            result = upload_response.json()
            extracted_data = {
                "ticket_num": result.get("ticket_num"),
                "payload": result.get("payload"),
                "supplier": result.get("supplier"),
                "signatureDetected": str(result.get("signature", False)).lower()
            }
            print("🧠 Extracted data from image upload:", extracted_data)
        else:
            print(f"⚠️ OCR upload failed with status {upload_response.status_code}")
    except Exception as e:
        print(f"❌ OCR upload error: {e}")

    # Step 2: Compose fields with hardcoded values
    gross_tons = 200
    tare_tons = 20
    net_tons = gross_tons - tare_tons

    data = {
        "companyId": str(COMPANY_ID),
        "truckId": str(truck_id),
        "truckName": truck_name,
        "corrected": "false",
        "voided": "false",
        "grossTons": str(gross_tons),
        "tareTons": str(tare_tons),
        "netTons": str(net_tons),
        "loadNumber": str(random.randint(1, 9)),
        "weighmaster": "Demo Weighmaster",
        "dotNumber": f"DOT{random.randint(100000, 999999)}",
        "dataExtractedFromAtp": "true",
        "createdLongitude": "0.0",
        "createdLatitude": "0.0",
        "closedLongitude": "0.0",
        "closedLatitude": "0.0",
        "remarks": "ATP LITE TICKET",
        "signatureDetected": extracted_data.get("signatureDetected", "false"),
        "externalRef": f"ATP-DEMO-{random.randint(10000, 99999)}"
    }

    if job_order_id:
        data["jobNumber"] = str(job_order_id)
    if extracted_data.get("ticket_num"):
        data["ticketNumber"] = extracted_data["ticket_num"]
    if extracted_data.get("payload"):
        data["payload"] = extracted_data["payload"]
    if extracted_data.get("supplier"):
        data["supplier"] = extracted_data["supplier"]

    # Step 3: Submit ticket
    air_ticket_id = None
    air_ticket_response = None
    try:
        with open(image_path, "rb") as image_file:
            files = {
                "photo": ("ATP-LITE-TICKET.jpeg", image_file, "image/jpeg")
            }

            response = requests.post(
                f"{API_BASE_URL}/api/2/companies/{COMPANY_ID}/atp-air-tickets-lite",
                data=data,
                files=files,
                headers=headers,
                timeout=30
            )

        if response.status_code in [200, 201]:
            air_ticket_response = response.json()
            air_ticket_id = air_ticket_response.get("data", {}).get("id") or air_ticket_response.get("id")
            print(f"✅ Air ticket created: {air_ticket_id}")
        else:
            print(f"❌ Air ticket creation failed. Status: {response.status_code}")
            print(response.text)
            return None, None
    except IncompleteRead as e:
        print(f"❌ IncompleteRead error: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Error during air ticket creation: {e}")
        return None, None

    # Step 4: Patch ticket with hardcoded quantity and weight
    try:
        patch_payload = {
            "quantity": "100",
            "weight": "200",
            "unitOfMeasure": 2,
            "externalRef": f"UPDATED-ATP-{random.randint(10000, 99999)}",
            "ticketType": "air_ticket",
            "isDuplicate": False
        }

        patch_response = requests.patch(
            f"{API_BASE_URL}/api/2/atp-air-tickets-lite/{air_ticket_id}",
            headers={
                "Authorization": f"Token {AUTH_TOKEN}",
                "Content-Type": "application/json"
            },
            json=patch_payload,
            timeout=10
        )

        if patch_response.status_code == 200:
            print(f"✏️  Patched air ticket {air_ticket_id} with hardcoded values")
        else:
            print(f"⚠️  Patch failed. Status: {patch_response.status_code}")
            print(patch_response.text)
    except Exception as e:
        print(f"❌ Error patching air ticket {air_ticket_id}: {e}")

    return air_ticket_id, air_ticket_response






def create_tickets_for_job_order(job_order_id, job_order_data, jo_line_item_id=None):
    """
    Create tickets for all trucks in the predefined TRUCKS list for a given job order

    Args:
        job_order_id (int): The job order ID to create tickets for
        job_order_data (dict): The full job order response data
        jo_line_item_id (int, optional): The job order line item ID - if None, will extract from job_order_data

    Returns:
        list: List of created ticket IDs
    """
    if not job_order_id:
        print("No job order ID provided. Cannot create tickets.")
        return []

    # Extract the actual line item ID from the job order response if not provided
    if jo_line_item_id is None:
        try:
            items = job_order_data.get("data", {}).get("items", [])
            if items and len(items) > 0:
                jo_line_item_id = items[0].get("id")
                print(f"Extracted line item ID from job order: {jo_line_item_id}")
            else:
                print("No items found in job order response. Using fallback ID 22627.")
                jo_line_item_id = 22627
        except Exception as e:
            print(f"Error extracting line item ID: {e}. Using fallback ID 22627.")
            jo_line_item_id = 22627

    created_tickets = []

    # Create a ticket for each truck
    for i, truck in enumerate(TRUCKS):
        # Use slightly different coordinates for each truck to simulate different locations
        latitude = 40.7128 + (i * 0.01)  # Offset each truck slightly
        longitude = -74.0060 + (i * 0.01)

        # Create ticket with realistic quantity
        quantity = random.uniform(1.0, 10.0)  # Random quantity between 50-150

        # Generate external reference
        external_ref = f"TICKET-{truck['device_name']}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        print(f"\nCreating ticket for {truck['device_name']} (ID: {truck['id']})...")
        print(f"Using job order ID: {job_order_id}, line item ID: {jo_line_item_id}")

        ticket_id, ticket_data = create_ticket(
            job_order_id=job_order_id,
            truck_id=truck["id"],
            jo_line_item_id=jo_line_item_id,
            quantity=quantity,
            latitude=latitude,
            longitude=longitude,
            external_ref=external_ref,
            weight=quantity * 2.5  # Assume 2.5 units of weight per quantity unit
        )

        if ticket_id:
            created_tickets.append(ticket_id)
        else:
            print(f"Failed to create ticket for truck {truck['device_name']}")

    print(f"\nCreated {len(created_tickets)} tickets for job order {job_order_id}")
    return created_tickets


def create_air_tickets_for_trucks(job_order_id=None, pickup_site_id=None):
    """
    Create ATP Air Tickets Lite for all trucks with multiple fallback strategies

    Args:
        job_order_id (int, optional): Job order ID to associate tickets with
        pickup_site_id (int, optional): Pickup site ID

    Returns:
        list: List of created air ticket IDs
    """
    created_air_tickets = []

    print(f"✈️  Creating ATP Air Tickets Lite for trucks with fallback strategies...")
    print(f"🚛 Trucks: {[truck['device_name'] for truck in TRUCKS]}")

    # Create an air ticket for each truck
    for truck in TRUCKS:
        print(f"\n🔄 Processing {truck['device_name']} (ID: {truck['id']})...")

        air_ticket_id, air_ticket_data = create_air_ticket_lite(
            truck_id=truck["id"],
            truck_name=truck["device_name"],
            job_order_id=job_order_id,
            pickup_location=pickup_site_id
        )

        if air_ticket_id:
            created_air_tickets.append(air_ticket_id)
            print(f"✅ Air ticket created successfully for {truck['device_name']}!")
        else:
            print(f"❌ All attempts failed for {truck['device_name']}")

    print(f"\n📊 Air Ticket Creation Summary:")
    print(f"  ✅ Successful: {len(created_air_tickets)}/{len(TRUCKS)}")
    print(f"  ❌ Failed: {len(TRUCKS) - len(created_air_tickets)}/{len(TRUCKS)}")

    if len(created_air_tickets) > 0:
        print(f"  🎫 Air ticket IDs: {created_air_tickets}")
    else:
        print(f"  ⚠️  No air tickets were created - this may be due to:")
        print(f"     • User permissions for ATP Air Ticket creation")
        print(f"     • Company {COMPANY_ID} ATP features not enabled")
        print(f"     • API endpoint changes or requirements")

    return created_air_tickets


def create_idle_time_alerts(job_order_id, truck_regions):
    """Create idle time alerts in OpenSearch for each truck"""

    if not job_order_id:
        print("No job order ID provided. Skipping alert creation.")
        return

    # Create alerts for each truck
    for truck in TRUCKS:
        region_id = truck_regions.get(truck["id"])
        if region_id is None:  # Check for None specifically, since 0 is a valid region ID
            print(f"No region found for truck {truck['device_name']} (ID: {truck['id']}). Using default region.")
            region_id = next((r_id for r_id in truck_regions.values() if r_id is not None), 0)

        doc = {
            "datetime": datetime.now(timezone.utc).strftime(DATETIME_FORMAT),
            "type": 11,  # AnomalyAlertEventType.IDLE_TIME
            "description": "idle_time_alert",
            "region_id": region_id,
            "is_silent": False,
            "truck_id": truck["id"],
            "truck_device_name": truck["device_name"],
            "threshold": truck["idle_threshold"],
            "job_order_id": job_order_id,
            "user_company_id": COMPANY_ID,
        }

        try:
            # Index the document
            response = es_client.index(index="anomaly_alert_event_index", body=doc)
            print(f"Successfully indexed alert for {truck['device_name']}")
        except Exception as e:
            print(f"Error indexing alert for {truck['device_name']}: {e}")


def create_truck_activity_events(job_order_id, truck_regions, regions_data):
    """Create truck activity events in OpenSearch for the trucks assigned to the job order"""

    if not job_order_id:
        print("No job order ID provided. Skipping truck activity creation.")
        return

    # Get region names mapping
    region_names = {}
    for region in regions_data:
        region_names[region.get("id")] = region.get("name")

    # Current time for base calculations
    now = datetime.now(timezone.utc)

    # Create activity for each truck
    for truck in TRUCKS:
        # Get the region ID for this truck
        region_id = truck_regions.get(truck["id"])
        if region_id is None:  # Check for None specifically, since 0 is a valid region ID
            print(f"No region found for truck {truck['device_name']} (ID: {truck['id']}). Using default region.")
            region_id = next((r_id for r_id in truck_regions.values() if r_id is not None), 0)
            if region_id is None:
                print(f"No default region available. Skipping events for truck {truck['device_name']}.")
                continue

        # Get region name
        region_name = region_names.get(region_id, f"Region_{region_id}")

        # Encode region name in the format expected by the system
        encoded_region = encode_region_name(region_name, COMPANY_ID)

        # Create a realistic timeline for truck events
        # Stagger start times for more realistic visualization
        truck_offset = random.randint(5, 30)  # minutes

        # Timeline for truck activities
        pickup_enter_time = now - timedelta(minutes=120 + truck_offset)
        pickup_exit_time = pickup_enter_time + timedelta(minutes=random.randint(15, 30))

        dropoff_enter_time = pickup_exit_time + timedelta(minutes=random.randint(20, 40))
        dropoff_exit_time = dropoff_enter_time + timedelta(minutes=random.randint(15, 25))

        # Create events for pickup location
        # ENTERED event for pickup
        pickup_enter_event = {
            "datetime": pickup_enter_time.strftime(DATETIME_FORMAT),
            "truck_id": truck["id"],
            "type": EVENT_TYPE_ENTERED,
            "truck_device_name": truck["device_name"],
            "job_order_id": job_order_id,
            "ticket_id": None,
            "region": encoded_region,
            "region_id": region_id,
            "is_silent": False
        }

        # LEFT event for pickup
        pickup_exit_event = {
            "datetime": pickup_exit_time.strftime(DATETIME_FORMAT),
            "truck_id": truck["id"],
            "type": EVENT_TYPE_LEFT,
            "truck_device_name": truck["device_name"],
            "job_order_id": job_order_id,
            "ticket_id": None,
            "region": encoded_region,
            "region_id": region_id,
            "is_silent": False
        }

        # Use the first available dropoff region ID (if different from pickup)
        dropoff_region_id = None
        for r_id in truck_regions.values():
            if r_id is not None and r_id != region_id:  # Check for None specifically
                dropoff_region_id = r_id
                break

        # If no different region found, use the pickup region
        if not dropoff_region_id:
            dropoff_region_id = region_id

        # Get dropoff region name and encode it
        dropoff_region_name = region_names.get(dropoff_region_id, f"Region_{dropoff_region_id}")
        encoded_dropoff_region = encode_region_name(dropoff_region_name, COMPANY_ID)

        # Create events for dropoff location
        # ENTERED event for dropoff
        dropoff_enter_event = {
            "datetime": dropoff_enter_time.strftime(DATETIME_FORMAT),
            "truck_id": truck["id"],
            "type": EVENT_TYPE_ENTERED,
            "truck_device_name": truck["device_name"],
            "job_order_id": job_order_id,
            "ticket_id": None,
            "region": encoded_dropoff_region,
            "region_id": dropoff_region_id,
            "is_silent": False
        }

        # LEFT event for dropoff
        dropoff_exit_event = {
            "datetime": dropoff_exit_time.strftime(DATETIME_FORMAT),
            "truck_id": truck["id"],
            "type": EVENT_TYPE_LEFT,
            "truck_device_name": truck["device_name"],
            "job_order_id": job_order_id,
            "ticket_id": None,
            "region": encoded_dropoff_region,
            "region_id": dropoff_region_id,
            "is_silent": False
        }

        # Index all events
        truck_events = [
            pickup_enter_event,
            pickup_exit_event,
            dropoff_enter_event,
            dropoff_exit_event
        ]

        for idx, event in enumerate(truck_events):
            try:
                response = es_client.index(
                    index="location_event_index",  # The index for location events
                    body=event
                )
                event_type = "ENTERED" if idx % 2 == 0 else "LEFT"
                location_type = "pickup" if idx < 2 else "dropoff"
                print(f"Indexed {event_type} event at {location_type} for truck {truck['device_name']}")
            except Exception as e:
                print(f"Error indexing event: {e}")

    print(f"Created truck activity events for job order {job_order_id}")


def generate_route_coordinates(start_coords, end_coords, num_points=20):
    """
    Generate GPS coordinates along a route between two points

    Args:
        start_coords (dict): Starting coordinates with 'lat' and 'lng' keys
        end_coords (dict): Ending coordinates with 'lat' and 'lng' keys
        num_points (int): Number of coordinate points to generate

    Returns:
        list: List of coordinate dictionaries with 'lat', 'lng', and 'timestamp' keys
    """
    coordinates = []

    # Calculate the distance and bearing between start and end points
    lat_diff = end_coords['lat'] - start_coords['lat']
    lng_diff = end_coords['lng'] - start_coords['lng']

    # Generate points along the route with some randomness to simulate real GPS tracking
    for i in range(num_points):
        # Calculate progress along the route (0 to 1)
        progress = i / (num_points - 1) if num_points > 1 else 0

        # Interpolate between start and end coordinates
        base_lat = start_coords['lat'] + (lat_diff * progress)
        base_lng = start_coords['lng'] + (lng_diff * progress)

        # Add some random variation to simulate realistic GPS drift
        # GPS accuracy is typically within 3-5 meters, which is roughly 0.00003-0.00005 degrees
        lat_variance = random.uniform(-0.00005, 0.00005)
        lng_variance = random.uniform(-0.00005, 0.00005)

        # Add some route variation to make it more realistic (trucks don't travel in straight lines)
        if i > 0 and i < num_points - 1:  # Don't vary the start and end points
            # Add a slight curve to the route
            curve_factor = math.sin(progress * math.pi) * 0.001
            lat_variance += curve_factor * random.uniform(-1, 1)
            lng_variance += curve_factor * random.uniform(-1, 1)

        final_lat = base_lat + lat_variance
        final_lng = base_lng + lng_variance

        # Calculate timestamp (spread over the journey time)
        # Assume the journey takes about 2-3 hours (120-180 minutes)
        journey_duration_minutes = random.randint(120, 180)
        time_offset_minutes = (journey_duration_minutes * progress) + random.uniform(-5, 5)  # Add some timing variation

        timestamp = datetime.now(timezone.utc) - timedelta(minutes=journey_duration_minutes - time_offset_minutes)

        coordinates.append({
            'lat': round(final_lat, 6),
            'lng': round(final_lng, 6),
            'timestamp': timestamp,
            'progress': progress
        })

    # Sort by timestamp to ensure chronological order
    coordinates.sort(key=lambda x: x['timestamp'])

    return coordinates


def generate_sensor_data():
    """
    Generate realistic accelerometer, gyroscope, and magnetometer data

    Returns:
        dict: Dictionary containing sensor data
    """
    # Generate accelerometer data (m/s²)
    # Normal driving typically has small accelerations
    accel_x = random.uniform(-2.0, 2.0)  # Lateral acceleration
    accel_y = random.uniform(-3.0, 3.0)  # Forward/backward acceleration
    accel_z = random.uniform(8.0, 12.0)  # Vertical (gravity + road bumps)
    accel_value = math.sqrt(accel_x ** 2 + accel_y ** 2 + accel_z ** 2)

    # Generate gyroscope data (degrees/second)
    # Small rotational movements during normal driving
    gyro_x = random.uniform(-5.0, 5.0)  # Roll
    gyro_y = random.uniform(-5.0, 5.0)  # Pitch
    gyro_z = random.uniform(-10.0, 10.0)  # Yaw (turning)
    gyro_value = math.sqrt(gyro_x ** 2 + gyro_y ** 2 + gyro_z ** 2)

    # Generate magnetometer data (μT - microtesla)
    # Earth's magnetic field varies by location, roughly 25-65 μT
    mag_x = random.uniform(-50.0, 50.0)
    mag_y = random.uniform(-50.0, 50.0)
    mag_z = random.uniform(-60.0, 60.0)
    mag_value = math.sqrt(mag_x ** 2 + mag_y ** 2 + mag_z ** 2)

    return {
        "accelerometer": {
            "x": round(accel_x, 3),
            "y": round(accel_y, 3),
            "z": round(accel_z, 3),
            "value": round(accel_value, 3)
        },
        "gyroscope": {
            "x": round(gyro_x, 3),
            "y": round(gyro_y, 3),
            "z": round(gyro_z, 3),
            "value": round(gyro_value, 3)
        },
        "magnetometer": {
            "x": round(mag_x, 3),
            "y": round(mag_y, 3),
            "z": round(mag_z, 3),
            "value": round(mag_value, 3)
        }
    }


def calculate_bearing(coord1, coord2):
    """
    Calculate the bearing between two GPS coordinates

    Args:
        coord1 (dict): Starting coordinate with 'lat' and 'lng' keys
        coord2 (dict): Ending coordinate with 'lat' and 'lng' keys

    Returns:
        float: Bearing in degrees (0-360)
    """
    if coord1 == coord2:
        return 0

    lat1 = math.radians(coord1['lat'])
    lat2 = math.radians(coord2['lat'])
    lng_diff = math.radians(coord2['lng'] - coord1['lng'])

    x = math.sin(lng_diff) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lng_diff)

    bearing = math.atan2(x, y)
    bearing = math.degrees(bearing)
    bearing = (bearing + 360) % 360  # Normalize to 0-360 degrees

    return round(bearing, 1)


def create_truck_gps_tracking_data(job_order_id, truck_regions, created_tickets=None):
    """
    Create GPS tracking data for trucks traveling from pickup to dropoff

    Args:
        job_order_id (int): The job order ID
        truck_regions (dict): Dictionary mapping truck IDs to region IDs
        created_tickets (list, optional): List of created ticket IDs to associate with GPS data
    """
    if not job_order_id:
        print("No job order ID provided. Skipping GPS tracking data creation.")
        return

    print(f"\nCreating GPS tracking data for trucks traveling from pickup to dropoff...")

    # Create tracking data for each truck
    for truck_idx, truck in enumerate(TRUCKS):
        print(f"\nGenerating GPS route for {truck['device_name']} (ID: {truck['id']})...")

        # Generate route coordinates
        route_coords = generate_route_coordinates(PICKUP_COORDS, DROPOFF_COORDS, num_points=20)

        # Get ticket ID for this truck if available
        ticket_id = None
        if created_tickets and truck_idx < len(created_tickets):
            ticket_id = created_tickets[truck_idx]

        print(f"Generated {len(route_coords)} GPS coordinates for {truck['device_name']}")

        # Create GPS tracking events in OpenSearch using the exact payload structure specified
        for i, coord in enumerate(route_coords):
            # Generate sensor data
            sensor_data = generate_sensor_data()

            # Calculate speed (mph) - varies based on route progress
            if i > 0:
                # Calculate speed based on distance and time difference
                prev_coord = route_coords[i - 1]
                distance_km = calculate_distance(prev_coord, coord)
                time_diff_hours = (coord['timestamp'] - prev_coord['timestamp']).total_seconds() / 3600
                speed_kmh = distance_km / time_diff_hours if time_diff_hours > 0 else 0
                speed_mph = speed_kmh * 0.621371  # Convert to mph
                speed = min(max(speed_mph, 0), 80)  # Cap between 0-80 mph
            else:
                speed = random.uniform(35, 65)  # Initial speed

            # Calculate heading
            heading = 0
            if i > 0:
                heading = calculate_bearing(route_coords[i - 1], coord)

            # Create GPS tracking event with exact payload structure
            gps_event = {
                "accelerometer.value": sensor_data["accelerometer"]["value"],
                "accelerometer.x": sensor_data["accelerometer"]["x"],
                "accelerometer.y": sensor_data["accelerometer"]["y"],
                "accelerometer.z": sensor_data["accelerometer"]["z"],
                "datetime": coord['timestamp'].strftime(DATETIME_FORMAT),
                "gyroscope.value": sensor_data["gyroscope"]["value"],
                "gyroscope.x": sensor_data["gyroscope"]["x"],
                "gyroscope.y": sensor_data["gyroscope"]["y"],
                "gyroscope.z": sensor_data["gyroscope"]["z"],
                "heading": heading,
                "job_order_id": job_order_id,
                "location": {
                    "type": "point",
                    "coordinates": [coord['lng'], coord['lat']]  # GeoJSON format: [longitude, latitude]
                },
                "magnetometer.value": sensor_data["magnetometer"]["value"],
                "magnetometer.x": sensor_data["magnetometer"]["x"],
                "magnetometer.y": sensor_data["magnetometer"]["y"],
                "magnetometer.z": sensor_data["magnetometer"]["z"],
                "speed": round(speed, 2),
                "ticket_id": ticket_id,
                "truck_id": truck["id"],
                "truck_name": truck["device_name"]
            }

            try:
                # Index the GPS event in the "truck" index as specified
                response = es_client.index(
                    index="truck",  # Using "truck" index as specified
                    body=gps_event
                )

                if (i + 1) % 5 == 0:  # Print progress every 5 points
                    print(f"  Indexed GPS point {i + 1}/{len(route_coords)} for {truck['device_name']}")

            except Exception as e:
                print(f"Error indexing GPS event for {truck['device_name']} at point {i + 1}: {e}")

        print(f"Completed GPS tracking data for {truck['device_name']}")

    print(f"\nCompleted GPS tracking data creation for all trucks on job order {job_order_id}")


def calculate_distance(coord1, coord2):
    """
    Calculate distance between two coordinates using Haversine formula

    Args:
        coord1 (dict): Starting coordinate with 'lat' and 'lng' keys
        coord2 (dict): Ending coordinate with 'lat' and 'lng' keys

    Returns:
        float: Distance in kilometers
    """
    R = 6371  # Earth's radius in kilometers

    lat1 = math.radians(coord1['lat'])
    lat2 = math.radians(coord2['lat'])
    dlat = math.radians(coord2['lat'] - coord1['lat'])
    dlng = math.radians(coord2['lng'] - coord1['lng'])

    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(lat1) * math.cos(lat2) *
         math.sin(dlng / 2) * math.sin(dlng / 2))

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


def main():
    """Main execution function"""

    global AUTH_TOKEN

    print("🚀 Starting job order and ticket creation process...")

    # 🔐 Step 0A: Authenticate WITHOUT device info
    AUTH_TOKEN = authenticate_without_device()
    if not AUTH_TOKEN:
        print("❌ Initial authentication failed. Aborting.")
        return

    # Step 1: Get truck regions
    regions_data = get_truck_regions()
    if not regions_data:
        print("❌ No truck regions found. Aborting.")
        return

    # Step 2: Get trucks with their regions
    truck_regions = get_trucks_with_regions()
    if not truck_regions:
        print("❌ No trucks with regions. Aborting.")
        return

    # Step 3: Select pickup/dropoff region IDs
    region_ids = list(set(truck_regions.values()))
    pickup_region_id = region_ids[0]
    dropoff_region_id = region_ids[1] if len(region_ids) > 1 else pickup_region_id

    # Step 4: Create job order
    job_order_id, job_order_data, pickup_site_name, dropoff_site_names, pickup_site_id = create_job_order(
        pickup_region_id, dropoff_region_id
    )
    if not job_order_id:
        print("❌ Job order creation failed. Aborting.")
        return

    # Step 5: Create regular tickets
    created_tickets = create_tickets_for_job_order(job_order_id, job_order_data)

    # Step 6: Create idle time alerts
    create_idle_time_alerts(job_order_id, truck_regions)

    # Step 7: Create truck activity events
    create_truck_activity_events(job_order_id, truck_regions, regions_data)

    # Step 8: Create GPS tracking data
    create_truck_gps_tracking_data(job_order_id, truck_regions, created_tickets)

    # 🔐 Step 0B: Re-authenticate WITH device info
    AUTH_TOKEN = authenticate_with_device()
    if not AUTH_TOKEN:
        print("❌ Authentication with device failed. Skipping air ticket creation.")
    else:
        create_air_tickets_for_trucks(job_order_id, pickup_site_id)

    # ✅ Summary
    print("\n🎉 PROCESS COMPLETE")
    print(f"  📄 Job Order: {job_order_id}")
    print(f"  🚛 Trucks: {[truck['device_name'] for truck in TRUCKS]}")
    print(f"  🎫 Tickets: {len(created_tickets)}")
    print(f"  ✈️ Air Tickets: {'CREATED' if AUTH_TOKEN else 'SKIPPED'}")



# Alternative function to create tickets independently (if you already have a job order)
def create_tickets_only(job_order_id):
    """
    Standalone function to create tickets for an existing job order

    Args:
        job_order_id (int): Existing job order ID
    """
    global AUTH_TOKEN

    # Authenticate if not already done
    if not AUTH_TOKEN:
        print("Authenticating...")
        AUTH_TOKEN = authenticate()
        if not AUTH_TOKEN:
            print("Authentication failed.")
            return

    # Create tickets - using fallback line item ID since we don't have job order data
    print(f"Creating tickets for existing job order {job_order_id}...")
    created_tickets = create_tickets_for_job_order(job_order_id, None, 22627)

    if created_tickets:
        print(f"Successfully created {len(created_tickets)} tickets: {created_tickets}")
    else:
        print("Failed to create tickets.")

    return created_tickets


# Standalone function to create GPS tracking data for existing job order
def create_gps_tracking_only(job_order_id, ticket_ids=None):
    """
    Standalone function to create GPS tracking data for an existing job order

    Args:
        job_order_id (int): Existing job order ID
        ticket_ids (list, optional): List of ticket IDs to associate with GPS data
    """
    global AUTH_TOKEN

    # Authenticate if not already done
    if not AUTH_TOKEN:
        print("Authenticating...")
        AUTH_TOKEN = authenticate()
        if not AUTH_TOKEN:
            print("Authentication failed.")
            return

    # Get truck regions
    truck_regions = get_trucks_with_regions()
    if not truck_regions:
        # Use default region mapping
        truck_regions = {truck["id"]: 0 for truck in TRUCKS}

    # Create GPS tracking data
    print(f"Creating GPS tracking data for existing job order {job_order_id}...")
    create_truck_gps_tracking_data(job_order_id, truck_regions, ticket_ids)

    print(f"GPS tracking data creation completed for job order {job_order_id}")


if __name__ == "__main__":
    main()