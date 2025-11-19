import requests
from datetime import datetime, timedelta, timezone
from opensearchpy import OpenSearch
import json
import random
import time
import math
import os
from pathlib import Path

# API Configuration
API_BASE_URL = "https://api.demo.truckit.com"
USERNAME = "support_sales_demos"
PASSWORD = "welcome"

# OpenSearch Configuration
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_HOST = 'vpc-stack-truckit-es7-r55zgy5aqm24i6tabwb6zcejcu.us-east-1.es.amazonaws.com'
ES_AUTH = ('master', 'C=BU42NWyUW2IjQsK0eCU95')

# Truck definitions - must match trucks that exist in the system
# Using 9 different trucks (3 per job) to avoid state conflicts
TRUCKS = [
    # Job 1 (Hourly) - DEMO trucks 575187-575189
    {"id": 575187, "device_name": "DEMO Truck", "idle_threshold": 20.0},
    {"id": 575188, "device_name": "Demo Truck 2", "idle_threshold": 22.0},
    {"id": 575189, "device_name": "Demo Truck 3", "idle_threshold": 0.0},
    # Job 2 (Tonnage) - DEMO trucks 575190-575192
    {"id": 575190, "device_name": "Demo Truck 4", "idle_threshold": 18.0},
    {"id": 575191, "device_name": "Demo Truck 5", "idle_threshold": 21.0},
    {"id": 575192, "device_name": "Demo Truck 6", "idle_threshold": 19.0},
    # Job 3 (Load) - DEMO trucks 575193-575195
    {"id": 575193, "device_name": "Demo Truck 7", "idle_threshold": 23.0},
    {"id": 575194, "device_name": "Demo Truck 8", "idle_threshold": 20.0},
    {"id": 575195, "device_name": "Demo Truck 9", "idle_threshold": 22.0},
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

# Counter for generating unique ticket numbers
_ticket_number_counter = 1

# Photo counters for cycling through photos
_photo_counters = {
    "atp": 0,
    "tonnage": 0,
    "hourly": 0,
    "timesheets": 0
}

# Specific tonnage values from photos (cycle through these instead of random)
_hourly_tonnage_values = [20.20, 10.0, 10.2]  # Sub-ticket tonnage for hourly jobs
_hourly_tonnage_index = 0

_timesheet_hours = [9.5, 13.0]  # Timesheet hours for hourly jobs
_timesheet_hours_index = 0

_tonnage_ticket_values = [20.32, 36.64]  # Main tonnage ticket values (swapped order)
_tonnage_ticket_index = 0

# ATP ticket net tonnage values from photos (9 tickets total)
_atp_net_tonnages = [21.42, 20.49, 20.89, 21.10, 20.47, 21.17, 21.07, 21.13, 24.31]
_atp_index = 0

def get_next_hourly_tonnage():
    """Get next hourly sub-ticket tonnage value (cycling through photo values)"""
    global _hourly_tonnage_index, _hourly_tonnage_values
    value = _hourly_tonnage_values[_hourly_tonnage_index % len(_hourly_tonnage_values)]
    _hourly_tonnage_index += 1
    return value

def get_next_timesheet_hours():
    """Get next timesheet hours value (cycling through photo values)"""
    global _timesheet_hours_index, _timesheet_hours
    value = _timesheet_hours[_timesheet_hours_index % len(_timesheet_hours)]
    _timesheet_hours_index += 1
    return value

def get_next_tonnage_value():
    """Get next tonnage ticket value (cycling through photo values)"""
    global _tonnage_ticket_index, _tonnage_ticket_values
    value = _tonnage_ticket_values[_tonnage_ticket_index % len(_tonnage_ticket_values)]
    _tonnage_ticket_index += 1
    return value

def get_next_atp_tonnage():
    """Get next ATP net tonnage value (cycling through 9 photo values)"""
    global _atp_index, _atp_net_tonnages
    value = _atp_net_tonnages[_atp_index % len(_atp_net_tonnages)]
    _atp_index += 1
    return value


def generate_ticket_number():
    """Generate a unique ticket number for demo purposes"""
    global _ticket_number_counter
    ticket_num = f"TKT-{datetime.now().strftime('%Y%m%d')}-{_ticket_number_counter:04d}"
    _ticket_number_counter += 1
    return ticket_num


def get_photos_from_folder(photo_type):
    """
    Get all photo files from a specific folder.

    Args:
        photo_type: "atp", "tonnage", "hourly", or "timesheets"

    Returns:
        list: List of photo file paths
    """
    script_dir = Path(__file__).parent
    photo_dir = script_dir / "ticket_photos" / photo_type

    if not photo_dir.exists():
        print(f"⚠️ Warning: Photo directory not found: {photo_dir}")
        return []

    # Get all image files (jpg, jpeg, png)
    photos = []
    for ext in ['*.jpg', '*.jpeg', '*.png', '*.JPG', '*.JPEG', '*.PNG']:
        photos.extend(photo_dir.glob(ext))

    return [str(p) for p in sorted(photos)]


def get_next_photo(photo_type):
    """
    Get the next photo from the folder, cycling through available photos.

    Args:
        photo_type: "atp", "tonnage", "hourly", or "timesheets"

    Returns:
        str: Path to photo file, or None if no photos available
    """
    global _photo_counters

    photos = get_photos_from_folder(photo_type)

    if not photos:
        return None

    # Get current photo and increment counter
    photo_path = photos[_photo_counters[photo_type] % len(photos)]
    _photo_counters[photo_type] += 1

    return photo_path


def upload_ticket_photo(ticket_id, photo_type, remarks="Ticket photo"):
    """
    Upload a photo to a ticket using multipart/form-data.

    Args:
        ticket_id (int): The ticket ID to attach photo to
        photo_type (str): "atp", "tonnage", "hourly", or "timesheets"
        remarks (str): Optional remarks for the photo

    Returns:
        bool: True if successful, False otherwise
    """
    global AUTH_TOKEN

    if not AUTH_TOKEN:
        print(f"⚠️ No auth token available for photo upload")
        return False

    # Get next photo
    photo_path = get_next_photo(photo_type)

    if not photo_path:
        print(f"⚠️ No photos available in ticket_photos/{photo_type}/")
        return False

    if not os.path.exists(photo_path):
        print(f"⚠️ Photo file not found: {photo_path}")
        return False

    try:
        # Prepare multipart form data
        filename = os.path.basename(photo_path)

        # Determine mime type
        ext = os.path.splitext(filename)[1].lower()
        mime_type = 'image/jpeg' if ext in ['.jpg', '.jpeg'] else 'image/png'

        # Open and read the file
        with open(photo_path, 'rb') as f:
            files = {
                'files[0]': (filename, f, mime_type)
            }

            data = {
                'remarks': remarks,
                'signed': 'false'
            }

            headers = {
                "Authorization": f"Token {AUTH_TOKEN}"
            }

            response = requests.post(
                f"{API_BASE_URL}/api/2/tickets/{ticket_id}/notes",
                files=files,
                data=data,
                headers=headers
            )

            if response.status_code in [200, 201]:
                print(f"  ✅ Uploaded photo: {filename}")
                return True
            else:
                print(f"  ⚠️ Photo upload failed: {response.status_code}")
                print(f"     Response: {response.text}")
                return False

    except Exception as e:
        print(f"  ⚠️ Error uploading photo: {e}")
        return False


def upload_air_ticket_photo(air_ticket_id, photo_type="atp", remarks="Air ticket photo"):
    """
    Upload a photo to an air ticket using multipart/form-data.

    Args:
        air_ticket_id (int): The air ticket ID to attach photo to
        photo_type (str): "atp" (default)
        remarks (str): Optional remarks for the photo

    Returns:
        bool: True if successful, False otherwise
    """
    global AUTH_TOKEN

    if not AUTH_TOKEN:
        print(f"⚠️ No auth token available for photo upload")
        return False

    # Get next photo
    photo_path = get_next_photo(photo_type)

    if not photo_path:
        print(f"⚠️ No photos available in ticket_photos/{photo_type}/")
        return False

    if not os.path.exists(photo_path):
        print(f"⚠️ Photo file not found: {photo_path}")
        return False

    try:
        # Prepare multipart form data
        filename = os.path.basename(photo_path)

        # Determine mime type
        ext = os.path.splitext(filename)[1].lower()
        mime_type = 'image/jpeg' if ext in ['.jpg', '.jpeg'] else 'image/png'

        # Open and read the file
        with open(photo_path, 'rb') as f:
            files = {
                'files[0]': (filename, f, mime_type)
            }

            data = {
                'remarks': remarks,
                'signed': 'false'
            }

            headers = {
                "Authorization": f"Token {AUTH_TOKEN}"
            }

            response = requests.post(
                f"{API_BASE_URL}/api/2/air-ticket-lite/{air_ticket_id}/notes",
                files=files,
                data=data,
                headers=headers
            )

            if response.status_code in [200, 201]:
                print(f"  ✅ Uploaded air ticket photo: {filename}")
                return True
            else:
                print(f"  ⚠️ Air ticket photo upload failed: {response.status_code}")
                print(f"     Response: {response.text}")
                return False

    except Exception as e:
        print(f"  ⚠️ Error uploading air ticket photo: {e}")
        return False


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


def get_site_regions(site_id):
    """Get all regions/geofences for a specific site"""
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return []

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            f"{API_BASE_URL}/api/1/regions?siteId={site_id}",
            headers=headers
        )
        if response.status_code == 200:
            data = response.json()
            regions = data.get("data", [])
            print(f"  Found {len(regions)} existing region(s) for site {site_id}")
            if regions:
                print(f"  DEBUG: First region structure: {regions[0]}")
            return regions
        else:
            print(f"  ⚠️ Could not fetch regions for site {site_id}: {response.status_code}")
            return []
    except Exception as e:
        print(f"  ⚠️ Error fetching regions for site {site_id}: {e}")
        return []


def update_site_region(region_id, center_lat, center_lng, radius=100):
    """Update an existing region/geofence with new radius"""
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    update_data = {
        "coordinates": [[center_lat, center_lng]],
        "type": "Circle",
        "radius": radius,
        "isPublic": False
    }

    try:
        print(f"  🔄 Updating region {region_id} to radius {radius}m...")
        response = requests.put(
            f"{API_BASE_URL}/api/2/regions/{region_id}",
            json=update_data,
            headers=headers
        )
        if response.status_code in [200, 201]:
            print(f"  ✅ Updated region {region_id}")
            return True
        else:
            print(f"  ⚠️ Could not update region {region_id}: {response.status_code}")
            if response.text:
                print(f"  DEBUG: Response: {response.text}")
            return False
    except Exception as e:
        print(f"  ⚠️ Error updating region {region_id}: {e}")
        return False


def create_site_geofence(site_id, site_name, center_lat, center_lng, radius=100):
    """
    Create or update a circular geofence for a site with correct radius.

    Args:
        site_id: ID of the site
        site_name: Name of the site
        center_lat: Center latitude
        center_lng: Center longitude
        radius: Radius in meters (default 100m)

    Returns:
        region_id if created/updated, None if failed
    """
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return None

    # Check if site already has regions and update them instead of deleting
    existing_regions = get_site_regions(site_id)
    if existing_regions:
        print(f"  🔄 Site {site_id} has {len(existing_regions)} existing geofence(s), updating with correct radius...")
        region = existing_regions[0]  # Update the first one
        region_id = region.get("id")
        if region_id:
            success = update_site_region(region_id, center_lat, center_lng, radius)
            if success:
                return region_id
            else:
                print(f"  ⚠️ Failed to update existing region, will try to create new one")
        # If update failed or no region_id, fall through to create

    # Create new circular geofence
    region_data = {
        "name": f"{site_name} Geofence",
        "coordinates": [[center_lat, center_lng]],  # Single point for circle center
        "type": "Circle",
        "radius": radius,  # In meters
        "siteId": site_id,
        "isPublic": False
    }

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        print(f"  📍 Creating geofence for {site_name} (ID: {site_id})...")
        print(f"     Center: ({center_lat}, {center_lng}), Radius: {radius}m")
        response = requests.post(
            f"{API_BASE_URL}/api/1/regions",
            json=region_data,
            headers=headers
        )

        if response.status_code in [200, 201]:
            region_id = response.json().get("data")
            print(f"  ✅ Created geofence with ID: {region_id}")
            return region_id
        else:
            print(f"  ⚠️ Geofence creation failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"  ⚠️ Geofence creation error: {e}")
        return None


def ensure_site_has_geofence(site_id, site_name=None, lat=None, lng=None):
    """
    Ensure a site has at least one geofence. Creates one if it doesn't exist.

    Args:
        site_id: ID of the site
        site_name: Name of the site (optional, will fetch if not provided)
        lat: Latitude of site center (optional, will fetch if not provided)
        lng: Longitude of site center (optional, will fetch if not provided)

    Returns:
        True if site has geofence (correctly sized), False otherwise
    """
    # Need site details to create geofence
    if not site_name or lat is None or lng is None:
        print(f"  📍 Fetching site details for site {site_id}...")
        # TODO: Add API call to fetch site details if needed
        # For now, use provided values or defaults
        if not site_name:
            site_name = f"Site {site_id}"
        if lat is None or lng is None:
            print(f"  ⚠️ Cannot create geofence without coordinates")
            return False

    # Create/recreate geofence (will delete existing ones and create new with correct radius)
    region_id = create_site_geofence(site_id, site_name, lat, lng, radius=100)
    return region_id is not None


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


def get_projects():
    """Get list of projects for the company"""
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Get all projects including archived/closed ones
        response = requests.get(
            f"{API_BASE_URL}/api/2/projects?paginate=false&status=1,2,3",
            headers=headers
        )

        if response.status_code == 200:
            projects = response.json().get("data", [])
            print(f"Found {len(projects)} projects")
            return projects
        else:
            print(f"Failed to get projects. Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error getting projects: {e}")
        return None


def get_or_create_purchase_order(project_id, pickup_site_id, dropoff_site_id, unit_of_measure_id, po_name="Demo PO", quantity=None):
    """
    Get existing PO for a project with specific UOM, or create a new one with varied material.

    Args:
        project_id: Project ID
        pickup_site_id: Pickup site ID
        dropoff_site_id: Dropoff site ID
        unit_of_measure_id: UOM ID (1=Hour, 2=Ton, 4=Load)
        po_name: Name/reference for the PO

    Returns:
        tuple: (po_id, po_line_item_id, po_data)
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None, None, None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Try to find existing PO for this project with matching UOM
    try:
        print(f"   Checking for existing {po_name} with UOM={unit_of_measure_id}...")
        response = requests.get(
            f"{API_BASE_URL}/api/2/purchase-orders",
            headers=headers,
            params={
                "projects": project_id,
                "archived": False,
                "page": 1,
                "per_page": 100
            }
        )

        if response.status_code == 200:
            pos = response.json().get("data", [])
            print(f"   Found {len(pos)} total PO(s) for this project")

            # Check each PO for matching UOM in line items
            for po in pos:
                po_id = po.get("id")

                # Fetch line items
                line_items_response = requests.get(
                    f"{API_BASE_URL}/api/1/purchase-orders/{po_id}/items",
                    headers=headers
                )

                if line_items_response.status_code == 200:
                    line_items = line_items_response.json().get("data", [])
                    print(f"      PO #{po_id} has {len(line_items)} line item(s)")

                    for line_item in line_items:
                        line_uom = line_item.get("unitOfMeasure")
                        line_item_id = line_item.get("id")
                        print(f"         Line Item #{line_item_id}: UOM={line_uom} (looking for UOM={unit_of_measure_id})")
                        if line_uom == unit_of_measure_id:
                            print(f"   ✅ Found existing PO #{po_id} with matching UOM={line_uom} (Line Item: {line_item_id})")
                            print(f"   ℹ️  Reusing this PO instead of creating a new one")
                            return po_id, line_item_id, po
    except Exception as e:
        print(f"   ⚠️ Error checking for existing POs: {e}")
        import traceback
        traceback.print_exc()

    # No existing PO found, create a new one with varied material
    print(f"   Creating new {po_name}...")

    # Vary material types (payload_id) based on UOM
    material_options = {
        1: [1, 2, 3],   # Hour: rotate through materials 1, 2, 3
        2: [4, 5, 6],   # Ton: rotate through materials 4, 5, 6
        4: [7, 8, 9]    # Load: rotate through materials 7, 8, 9
    }

    # Pick a random material from the appropriate range
    import random
    payload_id = random.choice(material_options.get(unit_of_measure_id, [1]))

    # Set price based on UOM
    prices = {
        1: 75.0,   # Hour
        2: 50.0,   # Ton
        4: 100.0   # Load
    }
    per_unit_price = prices.get(unit_of_measure_id, 50.0)

    # Set realistic quantities based on UOM if not provided
    if quantity is None:
        if unit_of_measure_id == 1:  # Hourly
            quantity = 35.0  # Realistic: 3 trucks * ~11-12 hours each
        elif unit_of_measure_id == 2:  # Tonnage
            # Calculate total tons delivered: (3 trips + 4 trips + 5 trips) * avg tonnage per trip
            # We have 2 tonnage values (36.64, 20.32) that cycle
            # 12 total trips, requesting MORE than will be delivered
            quantity = 350.0  # Request 350 tons but deliver less (realistic variance)
        else:  # Load
            quantity = 50.0

    return create_purchase_order(
        project_id=project_id,
        pickup_site_id=pickup_site_id,
        dropoff_site_id=dropoff_site_id,
        payload_id=payload_id,
        unit_of_measure_id=unit_of_measure_id,
        per_unit_price=per_unit_price,
        quantity=quantity
    )


def get_project_po_line_items(project_id):
    """Get PO line items for a specific project"""
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            f"{API_BASE_URL}/api/2/projects/{project_id}/po-items",
            headers=headers
        )

        if response.status_code == 200:
            po_items = response.json().get("data", [])
            print(f"Found {len(po_items)} PO line items for project {project_id}")
            if po_items:
                print(f"First PO line item ID: {po_items[0].get('id')}")
            return po_items
        else:
            print(f"Failed to get PO line items. Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error getting PO line items: {e}")
        return None


def create_project(name="Demo Script Project - Restricted Customer"):
    """Create a new project"""
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None

    project_data = {
        "name": name,
        "projectOwnerId": COMPANY_ID,
        "allowToNotify": True,
        "allowUnfilledStart": True,
        "autoAccept": True,
        "photoRequired": False
    }

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/projects",
            json=project_data,
            headers=headers
        )

        if response.status_code in [200, 201]:
            project = response.json().get("data", {})
            project_id = project.get("id")
            print(f"✅ Created project: {name} (ID: {project_id})")
            return project_id, project
        else:
            print(f"❌ Failed to create project. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None, None
    except Exception as e:
        print(f"❌ Error creating project: {e}")
        return None, None


def get_sites_by_name(name):
    """Check if a site with the given name already exists"""
    if not AUTH_TOKEN:
        return None

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Search for sites with the given name
        response = requests.get(
            f"{API_BASE_URL}/api/1/sites?keywords={name}&paginate=false",
            headers=headers
        )

        if response.status_code == 200:
            sites = response.json().get("data", [])
            # Look for exact match
            for site in sites:
                if site.get("name") == name:
                    return site
        return None
    except Exception as e:
        print(f"⚠️ Error checking for existing site: {e}")
        return None


def create_site(name, address, latitude, longitude, site_type="plant"):
    """Create a new site (plant or dump), or return existing if duplicate name"""
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None, None

    # Check if site already exists
    existing_site = get_sites_by_name(name)
    if existing_site:
        site_id = existing_site.get("id")
        print(f"✅ Found existing {site_type} site: {name} (ID: {site_id})")
        return site_id, existing_site

    site_data = {
        "name": name,
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "type": site_type,
        "isPublic": False,
        "alertZoneRadius": 100
    }

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/1/sites",
            json=site_data,
            headers=headers
        )

        if response.status_code in [200, 201]:
            site = response.json().get("data", {})
            site_id = site.get("id")
            print(f"✅ Created {site_type} site: {name} (ID: {site_id})")
            return site_id, site
        else:
            print(f"❌ Failed to create site. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None, None
    except Exception as e:
        print(f"❌ Error creating site: {e}")
        return None, None


def create_purchase_order(project_id, pickup_site_id, dropoff_site_id, payload_id=None, unit_of_measure_id=None, per_unit_price=None, quantity=100.0):
    """
    Create a purchase order with a line item

    Args:
        project_id: Project ID to attach PO to
        pickup_site_id: Pickup site/location ID
        dropoff_site_id: Dropoff site/location ID
        payload_id: Material type ID (if None, will try to get default)
        unit_of_measure_id: UOM ID (if None, defaults to 1 - Ton)
        per_unit_price: Price per unit (if None, defaults to 50.0)
        quantity: Total quantity for the PO line item (default 100.0)

    Returns:
        tuple: (po_id, po_line_item_id, po_data)
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return None, None, None

    # Set defaults
    if payload_id is None:
        payload_id = 1  # Default material type
    if unit_of_measure_id is None:
        unit_of_measure_id = 1  # Default: Ton
    if per_unit_price is None:
        per_unit_price = 50.0

    # Get truck types for the company
    truck_types = []
    try:
        truck_types_response = requests.get(
            f"{API_BASE_URL}/api/1/truck-types",
            headers={"Authorization": f"Token {AUTH_TOKEN}"}
        )
        if truck_types_response.status_code == 200:
            all_truck_types = truck_types_response.json().get("data", [])
            # Get first truck type or use empty list
            if all_truck_types:
                truck_types = [all_truck_types[0].get("id")]
    except Exception as e:
        print(f"⚠️ Could not fetch truck types: {e}")

    po_data = {
        "project": project_id,
        "customerRef": "Demo PO",
        "truckTypes": truck_types,
        "items": [
            {
                "payload": payload_id,
                "unitOfMeasure": unit_of_measure_id,
                "perUnitPrice": per_unit_price,
                "pickUpSite": pickup_site_id,
                "dropOffSites": [dropoff_site_id],
                "quantity": quantity,
                "unlimited": False,
                "ticketsType": 1  # Standalone tickets
            }
        ]
    }

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/1/purchase-orders",
            json=po_data,
            headers=headers
        )

        if response.status_code in [200, 201]:
            response_data = response.json()
            po = response_data.get("data", {})
            po_id = po.get("id")

            # Line items aren't included in the create response, fetch them separately
            if po_id:
                print(f"✅ Created Purchase Order (ID: {po_id}), fetching line items...")
                try:
                    # Fetch PO line items using the items endpoint
                    line_items_response = requests.get(
                        f"{API_BASE_URL}/api/1/purchase-orders/{po_id}/items",
                        headers=headers
                    )
                    if line_items_response.status_code == 200:
                        line_items = line_items_response.json().get("data", [])
                        if line_items and len(line_items) > 0:
                            po_line_item_id = line_items[0].get("id")
                            print(f"✅ Found PO Line Item (ID: {po_line_item_id})")
                            return po_id, po_line_item_id, po
                        else:
                            print(f"❌ No line items found in response")
                            return po_id, None, po
                    else:
                        print(f"⚠️ Could not fetch PO line items. Status: {line_items_response.status_code}")
                        print(f"Response: {line_items_response.text}")
                        return po_id, None, po
                except Exception as e:
                    print(f"⚠️ Error fetching PO line items: {e}")
                    return po_id, None, po
            else:
                print(f"❌ No PO ID in response")
                return None, None, None
        else:
            print(f"❌ Failed to create purchase order. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return None, None, None
    except Exception as e:
        print(f"❌ Error creating purchase order: {e}")
        return None, None, None


def create_job_order(pickup_site_id=None, dropoff_site_id=None, po_line_item_id=None, pickup_region_id=None, dropoff_region_id=None, num_trucks=None, truck_ids=None, quantity=None):
    """Create a job order via POST endpoint with specific pickup/dropoff sites or regions

    Args:
        num_trucks: Number of trucks to assign (None = all trucks, 0 = no trucks, 1 = first truck only)
        truck_ids: Specific list of truck IDs to assign (overrides num_trucks if provided)
        quantity: Total quantity for the job order (defaults to 100.0 if not provided)
    """

    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return None, None, None, None, None

    # If site IDs are provided directly, use them; otherwise derive from regions
    if pickup_site_id and dropoff_site_id:
        dropoff_site_ids = [dropoff_site_id]
        pickup_site_name = get_site_name(pickup_site_id)
        dropoff_site_names = [get_site_name(dropoff_site_id)]
    else:
        # Get sites associated with the regions (legacy behavior)
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

    # Use provided PO line item ID or fall back to default
    if po_line_item_id is None:
        po_line_item_id = 22627  # Default fallback

    # Determine which trucks to assign based on truck_ids or num_trucks parameter
    if truck_ids is not None:
        # Use explicitly provided truck IDs
        pass  # truck_ids already set
    elif num_trucks is None:
        # Assign all trucks (default behavior)
        truck_ids = [truck["id"] for truck in TRUCKS]
    elif num_trucks == 0:
        # No trucks assigned
        truck_ids = []
    elif num_trucks == 1:
        # Assign only the first truck
        truck_ids = [TRUCKS[0]["id"]] if TRUCKS else []
    else:
        # Assign specified number of trucks
        truck_ids = [truck["id"] for truck in TRUCKS[:num_trucks]]

    print(f"DEBUG: num_trucks={num_trucks}, truck_ids={truck_ids}, len={len(truck_ids)}")

    # Use provided quantity or default to 100.0
    if quantity is None:
        quantity = 100.0

    # Job order data for initial creation
    job_order_data = {
        "startDate": datetime.now(timezone.utc).isoformat(),
        "endDate": (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat(),
        "pickUpSite": pickup_site_id,
        "dropOffSites": dropoff_site_ids,
        "poLineItemId": po_line_item_id,  # Use the provided or default value
        "totalQuantity": float(quantity),  # Use provided quantity
        "unlimited": False,  # Required field
        "smartDispatch": False,
        "allowToNotify": False,
        "items": [
            {
                "startDate": datetime.now(timezone.utc).isoformat(),
                "quantity": int(quantity),  # Use provided quantity
                "unlimited": False,
                "autoApprove": True,
                "terms": 1,  # Required field - payment terms in days
                "trucks": truck_ids,  # Assign trucks based on num_trucks parameter
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

            # Fetch full job order details with items array
            if job_order_id:
                get_response = requests.get(
                    f"{API_BASE_URL}/api/2/job-orders/{job_order_id}",
                    headers=headers
                )
                if get_response.status_code == 200:
                    job_order = get_response.json()
                    print(f"Fetched full job order details with items")
                else:
                    print(f"⚠️ Warning: Could not fetch full job order details. Status: {get_response.status_code}")

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


def accept_job_order_for_truck(jo_line_item_id, truck_id):
    """
    Accept a job order line item for a specific truck

    Args:
        jo_line_item_id (int): The job order line item ID
        truck_id (int): The truck ID to accept the job for

    Returns:
        bool: True if successful, False otherwise
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/job-orders/{jo_line_item_id}/accept/{truck_id}",
            headers=headers
        )

        if response.status_code in [200, 201]:
            print(f"✅ Accepted job order line item {jo_line_item_id} for truck {truck_id}")
            return True
        else:
            print(f"❌ Failed to accept job order. Status: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error accepting job order: {e}")
        return False


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


def start_ticket(ticket_id):
    """
    Start a ticket via POST endpoint

    Args:
        ticket_id: ID of the ticket to start

    Returns:
        bool: True if successful, False otherwise
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/1/tickets/{ticket_id}/start",
            headers=headers
        )

        if response.status_code in [200, 201]:
            return True
        else:
            print(f"❌ Failed to start ticket {ticket_id}. Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error starting ticket {ticket_id}: {e}")
        return False


def pause_ticket(ticket_id):
    """
    Pause a ticket via POST endpoint

    Args:
        ticket_id: ID of the ticket to pause

    Returns:
        bool: True if successful, False otherwise
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/1/tickets/{ticket_id}/pause",
            headers=headers
        )

        if response.status_code in [200, 201]:
            return True
        else:
            print(f"❌ Failed to pause ticket {ticket_id}. Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error pausing ticket {ticket_id}: {e}")
        return False


def link_truck_to_device(truck_id):
    """
    Link a truck to the current device using /api/2/device/force-link

    Args:
        truck_id: ID of the truck to link

    Returns:
        bool: True if successful, False otherwise
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "truckId": truck_id
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/device/force-link",
            headers=headers,
            json=payload
        )

        if response.status_code in [200, 201]:
            print(f"✅ Linked truck {truck_id} to device")
            return True
        else:
            print(f"❌ Failed to link truck {truck_id}. Status: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error linking truck {truck_id}: {e}")
        return False


def sync_device_action(action_type, ticket_id, jo_line_item_id, truck_id, latitude=None, longitude=None, quantity=None, additional_quantity=None, event_timestamp=None, external_ref=None):
    """
    Sync a device action using the /api/2/device/sync endpoint

    Action types:
    - ticketOpened: Open a ticket
    - PickupCompleted: Mark pickup as completed
    - DropOffCompleted: Mark dropoff as completed
    - ticketClosed: Close a ticket
    - jobStarted: Start job timer
    - jobPaused: Pause job timer
    - jobResumed: Resume job timer

    Args:
        action_type: The action type (e.g., "ticketOpened", "PickupCompleted")
        ticket_id: ID of the ticket
        jo_line_item_id: Job order line item ID (poLineItem.id from job order response)
        truck_id: Truck ID
        latitude: Optional latitude for the event
        longitude: Optional longitude for the event
        quantity: Optional quantity (tonnage for tonnage-based jobs)
        additional_quantity: Optional additional quantity (tonnage for hourly jobs)
        event_timestamp: Optional ISO format timestamp for backdating (defaults to now)
        external_ref: Optional external ticket number/reference (user-provided ticket number)

    Returns:
        tuple: (success: bool, response_data: list or None)
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Build the action payload
    # For ticketOpened, we use localId instead of ticketId (ticket doesn't exist yet)
    # For other actions, we use ticketId (ticket already exists)
    timestamp_for_action = event_timestamp or datetime.now(timezone.utc).isoformat()
    local_id = f"{action_type}_{int(datetime.now(timezone.utc).timestamp())}_{truck_id}"

    action_data = {
        "actionType": action_type,
        "localId": local_id,
        "eventTimestamp": timestamp_for_action,
        "joLineItemId": jo_line_item_id,
        "truckId": truck_id
    }

    # Only add ticketId for actions on existing tickets (not ticketOpened)
    if ticket_id is not None and action_type != "ticketOpened":
        action_data["ticketId"] = ticket_id

    # Add coordinates as nested object if provided
    if latitude is not None and longitude is not None:
        action_data["coordinates"] = {
            "latitude": latitude,
            "longitude": longitude
        }
        # Also add flat lat/lon for pickup/dropoff processing (camelCase!)
        if action_type == "PickupCompleted":
            action_data["puLat"] = latitude
            action_data["puLon"] = longitude
        elif action_type == "DropOffCompleted":
            action_data["doLat"] = latitude
            action_data["doLon"] = longitude

    # Add quantity for actions that require it
    if quantity is not None and action_type in ["PickupCompleted", "DropOffCompleted", "ticketClosed"]:
        action_data["quantity"] = quantity

    # Add additional_quantity as "weight" if provided (for tonnage on hourly jobs)
    if additional_quantity is not None and action_type in ["PickupCompleted", "DropOffCompleted", "ticketClosed"]:
        action_data["weight"] = additional_quantity

    # Add external reference (ticket number) if provided
    if external_ref is not None:
        action_data["externalRef"] = external_ref

    sync_payload = {
        "actions": [action_data],
        "coordinates": []
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/device/sync",
            headers=headers,
            json=sync_payload
        )

        if response.status_code in [200, 201]:
            # Parse response to get ticket ID mappings (for ticketOpened actions)
            try:
                response_data = response.json()
                return True, response_data.get("data", [])
            except:
                return True, []
        else:
            print(f"❌ Failed to sync {action_type} for ticket {ticket_id}. Status: {response.status_code}, Response: {response.text}")
            return False, None
    except Exception as e:
        print(f"❌ Error syncing {action_type} for ticket {ticket_id}: {e}")
        return False, None


def send_gps_coordinates_batch(truck_id, ticket_id, coordinates_list, jo_line_item_id=None):
    """
    Send a batch of GPS coordinates via device sync

    Args:
        truck_id: Truck ID
        ticket_id: Ticket ID to associate coordinates with
        coordinates_list: List of coordinate dicts with keys: latitude, longitude, event_timestamp, speed, heading
        jo_line_item_id: Optional JO line item ID for geofence event processing

    Returns:
        bool: True if successful
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Build coordinates array for device sync
    coordinates_payload = []
    for coord in coordinates_list:
        heading_value = coord.get("heading", 0)
        coord_item = {
            "latitude": coord["latitude"],
            "longitude": coord["longitude"],
            "eventTimestamp": coord.get("event_timestamp", datetime.now(timezone.utc).isoformat()),
            "currentTicketId": ticket_id,
            "truckId": truck_id,  # Important for geofence event processing
            "speed": coord.get("speed", 0),
            "bearing": heading_value,  # Device sync requires both bearing and heading
            "heading": heading_value   # Keep heading as well
        }
        # Add optional fields if provided
        if jo_line_item_id:
            coord_item["currentJoliId"] = jo_line_item_id  # JO line item ID for geofence processing
        coordinates_payload.append(coord_item)

    sync_payload = {
        "actions": [],
        "coordinates": coordinates_payload
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/device/sync",
            headers=headers,
            json=sync_payload
        )

        if response.status_code in [200, 201]:
            return True
        else:
            print(f"❌ Failed to send GPS coordinates. Status: {response.status_code}")
            try:
                error_msg = response.json()
                print(f"   Error response: {error_msg}")
            except:
                print(f"   Error response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error sending GPS coordinates: {e}")
        return False


def get_jo_line_items(job_order_id):
    """
    Get JOLineItems for a job order using /api/2/job-orders/{id}/items endpoint

    This returns the actual JOLineItem IDs that are used in device sync actions,
    NOT the POLineItem IDs.

    Args:
        job_order_id (int): The job order ID

    Returns:
        list: List of JOLineItem dicts, each with 'id', 'trucks', etc.
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return []

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            f"{API_BASE_URL}/api/2/job-orders/{job_order_id}/items",
            headers=headers
        )

        if response.status_code == 200:
            data = response.json()
            items = data.get("data", [])
            if items:
                print(f"✅ Found {len(items)} JOLineItem(s) for job order {job_order_id}:")
                for item in items:
                    trucks = item.get('trucks', [])
                    print(f"   - JOLineItem ID: {item.get('id')}, {len(trucks)} truck(s) assigned")
            else:
                print(f"⚠️ No JOLineItems found for job order {job_order_id}")
            return items
        else:
            print(f"❌ Failed to get JOLineItems. Status: {response.status_code}, Response: {response.text}")
            return []
    except Exception as e:
        print(f"❌ Error getting JOLineItems: {e}")
        return []


def close_prior_day_jobs():
    """
    Close all active or not started job orders from prior days.
    This should be called at the start of the script to clean up old jobs.

    Returns:
        int: Number of jobs closed
    """
    if not AUTH_TOKEN:
        print("No auth token available for closing prior day jobs.")
        return 0

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Get current date at midnight (start of today)
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Set date range to get ALL job orders (backend defaults to today-only if not specified)
    # Go back 90 days to capture all recent job orders
    start_date = (today_start - timedelta(days=90)).strftime("%Y-%m-%d")
    end_date = today_start.strftime("%Y-%m-%d")

    try:
        # Fetch all job orders for the company with explicit date range
        # Must use startDate/endDate or backend will default to today-only
        response = requests.get(
            f"{API_BASE_URL}/api/2/job-orders",
            params={
                "company": COMPANY_ID,
                "startDate": start_date,
                "endDate": end_date,
                "perPage": 1000  # Large number to ensure we get all results
            },
            headers=headers
        )

        if response.status_code != 200:
            print(f"❌ Failed to fetch job orders. Status: {response.status_code}")
            return 0

        job_orders = response.json().get("data", [])

        if not job_orders:
            print("✅ No job orders found.")
            return 0

        print(f"📋 Found {len(job_orders)} total job order(s) for company {COMPANY_ID}")

        # Filter for jobs that are not closed
        # Note: The backend doesn't return createdAt in the list response, so we can't filter by date
        # Since we're querying with a date range of last 90 days, all these jobs are old enough to close
        jobs_to_close = []
        for job in job_orders:
            job_id = job.get("id")
            status = job.get("status")
            closed = job.get("closed", False)
            job_name = job.get("name", "Unknown")

            # Skip jobs that are already closed
            if closed:
                continue

            # Close all non-closed jobs (they're from the 90-day lookback window, so they're old)
            jobs_to_close.append(job)

        if not jobs_to_close:
            print("✅ No unclosed job orders found.")
            return 0

        print(f"\n🧹 Found {len(jobs_to_close)} job order(s) to close...")

        # Close each job
        closed_count = 0
        for job in jobs_to_close:
            job_id = job.get("id")
            job_name = job.get("name", "Unknown")
            status = job.get("status")

            print(f"  Closing job order {job_id} ('{job_name}', status: {status})...")

            if close_job_order(job_id):
                closed_count += 1
            else:
                print(f"  ⚠️ Failed to close job order {job_id}")

        print(f"✅ Successfully closed {closed_count} of {len(jobs_to_close)} job order(s)\n")
        return closed_count

    except Exception as e:
        print(f"❌ Error in close_prior_day_jobs: {e}")
        return 0


def close_job_order(job_order_id):
    """
    Close a job order via POST endpoint

    Args:
        job_order_id: ID of the job order to close

    Returns:
        bool: True if successful, False otherwise
    """
    if not AUTH_TOKEN:
        print("No auth token available.")
        return False

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/1/job-orders/{job_order_id}/close",
            headers=headers
        )

        if response.status_code in [200, 201]:
            print(f"✅ Successfully closed job order: {job_order_id}")
            return True
        else:
            print(f"❌ Failed to close job order {job_order_id}. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error closing job order {job_order_id}: {e}")
        return False


def close_ticket(ticket_id, quantity=None, latitude=None, longitude=None, message=None,
                 weight=None, external_ref=None, closed_date=None):
    """
    Close a ticket via POST endpoint

    Args:
        ticket_id (int): The ticket ID to close
        quantity (float, optional): Updated quantity
        latitude (float, optional): Latitude coordinate for closing location
        longitude (float, optional): Longitude coordinate for closing location
        message (str, optional): Message/note for closing
        weight (float, optional): Additional weight
        external_ref (str, optional): External reference
        closed_date (str, optional): Closed date in ISO format

    Returns:
        bool: True on success, False on failure
    """
    if not AUTH_TOKEN:
        print("No auth token available. Please authenticate first.")
        return False

    # Prepare close ticket data
    close_data = {}

    # Add coordinates if provided
    if latitude is not None and longitude is not None:
        if not (-90 <= latitude <= 90):
            print(f"Invalid latitude {latitude}. Must be between -90 and 90.")
            return False
        if not (-180 <= longitude <= 180):
            print(f"Invalid longitude {longitude}. Must be between -180 and 180.")
            return False
        close_data["coordinates"] = {
            "latitude": latitude,
            "longitude": longitude
        }

    # Add optional fields if provided
    if quantity is not None:
        close_data["quantity"] = quantity

    if message is not None:
        close_data["message"] = message

    if weight is not None:
        close_data["weight"] = weight

    if external_ref is not None:
        close_data["externalRef"] = external_ref

    if closed_date is not None:
        close_data["closedTimeUTC"] = closed_date

    # Headers for API request
    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Make the POST request to close ticket
        response = requests.post(
            f"{API_BASE_URL}/api/2/tickets/{ticket_id}/close",
            json=close_data,
            headers=headers
        )

        if response.status_code == 200 or response.status_code == 201:
            print(f"✅ Successfully closed ticket: {ticket_id}")
            return True
        else:
            print(f"❌ Failed to close ticket {ticket_id}. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except Exception as e:
        print(f"❌ Error closing ticket {ticket_id}: {e}")
        return False


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

    # Step 2: Compose fields with real values from photos
    net_tons = get_next_atp_tonnage()
    # Typical tare weight for dump trucks is 15-25 tons
    tare_tons = 20  # Fixed realistic tare weight
    gross_tons = net_tons + tare_tons

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

    # Step 4: Patch ticket with real quantity and weight from photo
    try:
        patch_payload = {
            "quantity": str(net_tons),
            "weight": str(gross_tons),
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


def issue_ticket_via_web_api(jo_line_item_id, truck_id, dropoff_location_id=None, quantity=0, coordinates=None, external_ref=None):
    """
    Create a ticket via web API POST /api/2/tickets (mimics mobile app issueTicket())

    This is used for STANDALONE hourly jobs where the mobile app uses web API instead of device sync.

    Args:
        jo_line_item_id: Job order line item ID
        truck_id: Truck ID
        dropoff_location_id: Optional dropoff site ID
        quantity: Optional quantity (defaults to 0 for hourly jobs)
        coordinates: Optional dict with 'latitude' and 'longitude'
        external_ref: Optional external ticket number/reference (user-provided ticket number)

    Returns:
        tuple: (success, ticket_id)
    """
    payload = {
        "joLineItemId": jo_line_item_id,
        "truckId": truck_id,
        "quantity": quantity
    }

    if dropoff_location_id:
        payload["dropOffLocation"] = dropoff_location_id

    if coordinates:
        payload["coordinates"] = coordinates

    if external_ref:
        payload["externalRef"] = external_ref

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/tickets",
            json=payload,
            headers=headers
        )

        if response.status_code in [200, 201]:
            response_data = response.json()
            ticket_id = response_data.get('id') or response_data.get('data', {}).get('id')
            return True, ticket_id
        else:
            print(f"  ⚠️ Ticket creation failed: {response.status_code} - {response.text}")
            return False, None
    except Exception as e:
        print(f"  ⚠️ Ticket creation error: {e}")
        return False, None


def close_ticket_via_web_api(ticket_id, weight=None, coordinates=None):
    """
    Close a ticket via web API POST /api/2/tickets/{id}/close

    Args:
        ticket_id: Ticket ID to close
        weight: Optional weight/tonnage
        coordinates: Optional dict with 'latitude' and 'longitude'

    Returns:
        bool: Success status
    """
    payload = {}

    if weight is not None:
        payload["weight"] = weight

    if coordinates:
        payload["coordinates"] = coordinates

    headers = {
        "Authorization": f"Token {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/2/tickets/{ticket_id}/close",
            json=payload,
            headers=headers
        )

        if response.status_code in [200, 201]:
            return True
        else:
            print(f"  ⚠️ Ticket close failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"  ⚠️ Ticket close error: {e}")
        return False






def create_tickets_for_job_order(job_order_id, job_order_data, jo_line_item_id=None, ticket_open_timestamp=None):
    """
    Create tickets for all trucks in the predefined TRUCKS list for a given job order

    This follows the mobile app flow:
    1. Get JOLineItems for the job order
    2. Send jobStarted action to assign job to each truck
    3. Create tickets via ticketOpened actions

    Args:
        job_order_id (int): The job order ID to create tickets for
        job_order_data (dict): The full job order response data (not used if jo_line_items queried)
        jo_line_item_id (int, optional): The job order line item ID - if None, will query from API
        ticket_open_timestamp (str, optional): ISO timestamp for when tickets should be opened (defaults to now)

    Returns:
        list: List of created ticket IDs
    """
    if not job_order_id:
        print("No job order ID provided. Cannot create tickets.")
        return []

    # Get JOLineItems for this job order (these are the actual JOLineItem IDs, not POLineItem IDs)
    jo_line_items = get_jo_line_items(job_order_id)
    if not jo_line_items:
        print("❌ CRITICAL: No JOLineItems found for job order. Cannot create tickets.")
        return []

    # For direct assignment jobs, there should be one JOLineItem with all trucks assigned
    jo_line_item = jo_line_items[0]
    jo_line_item_id = jo_line_item.get('id')

    # Get UOM from job_order_data (not in JOLineItem)
    job_uom = None
    if job_order_data:
        if isinstance(job_order_data, dict) and 'data' in job_order_data:
            job_uom = job_order_data['data'].get('unitOfMeasure')
        elif isinstance(job_order_data, dict):
            job_uom = job_order_data.get('unitOfMeasure')

    print(f"\n✅ Using JOLineItem ID: {jo_line_item_id}, UOM: {job_uom}")

    # DEBUG: Print jo_line_item structure to see what fields are available
    print(f"DEBUG: JOLineItem keys: {jo_line_item.keys()}")
    if 'trucks' in jo_line_item:
        print(f"DEBUG: trucks field value: {jo_line_item['trucks']}")

    # Get the truck IDs assigned to this job from the JOLineItem
    # The trucks might be in the 'trucks' field or we might need to get them from job_order_data
    # Note: JOLineItem trucks use 'truckId' field, not 'id'
    assigned_truck_ids = [t.get('truckId') for t in jo_line_item.get('trucks', [])]

    # If no trucks in JOLineItem, try getting from job_order_data
    if not assigned_truck_ids:
        print("⚠️ No trucks in JOLineItem, checking job_order_data...")
        print(f"DEBUG: job_order_data type: {type(job_order_data)}")
        print(f"DEBUG: job_order_data keys: {job_order_data.keys() if job_order_data else 'None'}")

        if job_order_data:
            assigned_trucks_from_jo = job_order_data.get('assignedTrucks', [])
            print(f"DEBUG: assignedTrucks from job_order_data: {assigned_trucks_from_jo}")
            assigned_truck_ids = [t.get('id') for t in assigned_trucks_from_jo]
            print(f"   Found {len(assigned_truck_ids)} trucks from job_order_data: {assigned_truck_ids}")
        else:
            print("   ERROR: job_order_data is None or empty!")

    if not assigned_truck_ids:
        print("❌ CRITICAL: No trucks assigned to this JOLineItem. Cannot create tickets.")
        return []

    # Filter TRUCKS to only include trucks assigned to this job
    job_trucks = [truck for truck in TRUCKS if truck['id'] in assigned_truck_ids]
    print(f"✅ Found {len(job_trucks)} trucks assigned to this job: {[t['device_name'] for t in job_trucks]}")

    # Note: When autoAccept=true (which is the case for our jobs), trucks are automatically
    # added to accepted_trucks when assigned, so we DON'T need to send jobAccepted actions.
    # Sending jobAccepted when autoAccept=true results in "Job already accepted" errors.

    # Step 1: Start the job for each truck (this sets current_jo_line_item_id)
    print(f"\n🚀 Starting job for trucks (jobStarted actions)...")
    for truck in job_trucks:
        print(f"   Starting job for {truck['device_name']} (ID: {truck['id']})...")
        success, _ = sync_device_action(
            action_type="jobStarted",
            ticket_id=None,
            jo_line_item_id=jo_line_item_id,
            truck_id=truck["id"],
            latitude=33.7490,  # Atlanta pickup site
            longitude=-84.3880
        )
        if success:
            print(f"   ✅ Job started for {truck['device_name']}")
        else:
            print(f"   ❌ Failed to start job for {truck['device_name']}")

    created_tickets = []
    ticket_id_map = {}  # Maps local_id → real ticket_id

    # Step 2: Open tickets
    # For STANDALONE hourly jobs (UOM=1), mobile app uses web API, not device sync
    # For all other jobs, use device sync
    if job_uom == 1:  # Hourly jobs
        print(f"\n📱 Opening tickets via web API (hourly jobs)...")
    else:
        print(f"\n📱 Opening tickets via device sync...")

    # Collect all ticket open actions
    for i, truck in enumerate(job_trucks):
        # Use Atlanta pickup site coordinates for ticket creation
        latitude = 33.7490 + (i * 0.001)  # Offset each truck slightly
        longitude = -84.3880 + (i * 0.001)

        print(f"\n📋 Opening ticket for {truck['device_name']} (ID: {truck['id']})...")
        print(f"   Job Order: {job_order_id}, JO Line Item: {jo_line_item_id}")

        # Generate unique ticket number
        ticket_number = generate_ticket_number()
        print(f"   Ticket Number: {ticket_number}")

        if job_uom == 1:  # Hourly jobs - use web API
            success, ticket_id = issue_ticket_via_web_api(
                jo_line_item_id=jo_line_item_id,
                truck_id=truck["id"],
                quantity=0,
                coordinates={"latitude": latitude, "longitude": longitude},
                external_ref=ticket_number
            )
            if success and ticket_id:
                created_tickets.append(ticket_id)
                print(f"   ✅ Ticket opened successfully - Real ID: {ticket_id}")
            else:
                print(f"   ❌ Failed to open ticket for {truck['device_name']}")
        else:  # Other jobs - use device sync
            success, response_data = sync_device_action(
                action_type="ticketOpened",
                ticket_id=None,  # No ticket ID yet, will be created
                jo_line_item_id=jo_line_item_id,
                truck_id=truck["id"],
                latitude=latitude,
                longitude=longitude,
                event_timestamp=ticket_open_timestamp,  # Use historical timestamp if provided
                external_ref=ticket_number
            )

            if success:
                # Parse response to get real ticket ID
                if response_data and isinstance(response_data, list):
                    # Try to find matching local ID first
                    real_ticket_id = None
                    for item in response_data:
                        ticket_id_from_item = item.get('ticketId')
                        if ticket_id_from_item:
                            real_ticket_id = ticket_id_from_item
                            break

                    if real_ticket_id:
                        created_tickets.append(real_ticket_id)
                        print(f"   ✅ Ticket opened successfully - Real ID: {real_ticket_id}")
                    else:
                        print(f"   ⚠️ Ticket opened but no ticket ID in response")
                else:
                    print(f"   ⚠️ Ticket opened but no ticket ID in response")
            else:
                print(f"   ❌ Failed to open ticket for {truck['device_name']}")

    print(f"\n✅ Opened {len(created_tickets)} tickets for job order {job_order_id}")
    print(f"   Real ticket IDs: {created_tickets}")
    return created_tickets, jo_line_item_id, job_uom


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
            # Upload ATP photo to air ticket
            upload_air_ticket_photo(air_ticket_id, "atp", "ATP air ticket photo")
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


def create_truck_activity_events(job_order_id, truck_regions, regions_data, trucks_list=None):
    """Create truck activity events in OpenSearch for the trucks assigned to the job order

    Args:
        job_order_id: The job order ID
        truck_regions: Dict mapping truck IDs to region IDs
        regions_data: List of region data dicts
        trucks_list: Optional list of specific trucks to create events for (defaults to TRUCKS)
    """

    if not job_order_id:
        print("No job order ID provided. Skipping truck activity creation.")
        return

    # Use provided trucks list or default to all TRUCKS
    if trucks_list is None:
        trucks_list = TRUCKS

    # Get region names mapping
    region_names = {}
    for region in regions_data:
        region_names[region.get("id")] = region.get("name")

    # Current time for base calculations
    now = datetime.now(timezone.utc)

    # Create activity for each truck
    for truck in trucks_list:
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
        # Assume the journey takes about 1.5-2.5 hours (90-150 minutes) - realistic for truck routes
        journey_duration_minutes = random.randint(90, 150)
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
    # Accelerometer in m/s^2 - realistic truck movements
    accel_x = random.uniform(-1.5, 1.5)  # Lateral acceleration (turns)
    accel_y = random.uniform(-2.5, 2.5)  # Forward/backward acceleration (braking/accelerating)
    accel_z = random.uniform(9.0, 10.5)  # Vertical (gravity ~9.8 + road bumps)
    accel_value = math.sqrt(accel_x ** 2 + accel_y ** 2 + accel_z ** 2)

    # Generate gyroscope data (degrees/second) - realistic truck movements
    # Small rotational movements during normal driving
    gyro_x = random.uniform(-3.0, 3.0)  # Roll (side-to-side tilt)
    gyro_y = random.uniform(-3.0, 3.0)  # Pitch (front-back tilt)
    gyro_z = random.uniform(-8.0, 8.0)  # Yaw (turning rotation)
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


def generate_varied_gps_path(start_coords, end_coords, num_points=25, variation_index=0):
    """
    Generate a GPS path with visible variation from previous paths.

    Args:
        start_coords: Dict with 'lat' and 'lng'
        end_coords: Dict with 'lat' and 'lng'
        num_points: Number of points to generate
        variation_index: Index to create different paths (0-4)

    Returns:
        List of coordinate dicts with lat/lng
    """
    path = []

    # Create curved paths by varying the interpolation
    # variation_index determines how much we deviate from straight line
    for i in range(num_points):
        progress = i / (num_points - 1)

        # Base interpolation
        lat = start_coords['lat'] + (end_coords['lat'] - start_coords['lat']) * progress
        lng = start_coords['lng'] + (end_coords['lng'] - start_coords['lng']) * progress

        # Add perpendicular offset based on variation_index
        # This creates visibly different paths
        perpendicular_offset = 0.01 * math.sin(progress * math.pi)  # Bow in the middle

        if variation_index == 0:
            # Straight-ish path with slight northern bow
            lat += perpendicular_offset * 0.3
        elif variation_index == 1:
            # Southern bow
            lat -= perpendicular_offset * 0.4
        elif variation_index == 2:
            # Eastern bow
            lng += perpendicular_offset * 0.5
        elif variation_index == 3:
            # Western bow
            lng -= perpendicular_offset * 0.4
        else:
            # Zigzag pattern
            if i % 2 == 0:
                lat += perpendicular_offset * 0.2
                lng += perpendicular_offset * 0.2
            else:
                lat -= perpendicular_offset * 0.2
                lng -= perpendicular_offset * 0.2

        # Add random noise for realism
        lat += random.uniform(-0.0005, 0.0005)
        lng += random.uniform(-0.0005, 0.0005)

        path.append({'lat': lat, 'lng': lng})

    return path


def setup_truck_with_multiple_trips(truck, jo_line_item_id, pickup_coords, dropoff_coords, job_uom, num_trips, final_state, truck_offset_minutes=0):
    """
    Generate multiple trips for a single truck with varied GPS paths and tickets.

    Args:
        truck: Truck dict with 'id' and 'device_name'
        jo_line_item_id: JOLineItem ID
        pickup_coords: Dict with 'lat', 'lng', 'site_id'
        dropoff_coords: Dict with 'lat', 'lng', 'site_id'
        job_uom: UOM ID (1=Hour, 2=Ton, 4=Load)
        num_trips: Number of trips to generate (2-5)
        final_state: 'at_dropoff', 'at_pickup', or 'en_route'
        truck_offset_minutes: Time offset in minutes for this truck (default 0)

    Returns:
        List of ticket IDs created
    """
    print(f"\n🚛 Generating {num_trips} trips for {truck['device_name']} (final state: {final_state}, offset: {truck_offset_minutes}min)...")
    print(f"   DEBUG: job_uom={job_uom}, jo_line_item_id={jo_line_item_id}")

    bearing = calculate_bearing(pickup_coords, dropoff_coords)
    tickets_created = []

    # Calculate timestamps - spread trips over realistic time periods
    now = datetime.now(timezone.utc)

    # For hourly jobs, make trips span longer to match photo hours (9.5, 13 hours)
    # For tonnage jobs, keep shorter realistic trip durations
    if job_uom == 1:  # Hourly
        # Spread trips over 8-10 hours total for realistic hourly billing
        trip_duration_minutes = random.randint(90, 120)  # 1.5-2 hours per trip
        gap_between_trips = random.randint(20, 40)  # 20-40 min gaps
    else:  # Tonnage/Load
        trip_duration_minutes = random.randint(40, 50)  # 40-50 min per trip
        gap_between_trips = random.randint(10, 20)  # 10-20 min gaps

    for trip_num in range(num_trips):
        is_last_trip = (trip_num == num_trips - 1)

        # Calculate timestamps for this trip with truck-specific offset
        trip_start_offset = trip_num * (trip_duration_minutes + gap_between_trips)

        # Adjust time window based on job type
        if job_uom == 1:  # Hourly - spread over longer period (8-12 hours)
            total_minutes_back = num_trips * trip_duration_minutes + (num_trips - 1) * gap_between_trips + truck_offset_minutes
            ticket_open_time = now - timedelta(minutes=total_minutes_back - trip_start_offset)
        else:  # Tonnage/Load - spread over 4 hours
            ticket_open_time = now - timedelta(minutes=240 - trip_start_offset - truck_offset_minutes)
        pickup_complete_time = ticket_open_time + timedelta(minutes=15)
        dropoff_complete_time = pickup_complete_time + timedelta(minutes=25)
        ticket_close_time = dropoff_complete_time + timedelta(minutes=5)

        print(f"  Trip {trip_num + 1}/{num_trips}: {ticket_open_time.strftime('%H:%M')} - {ticket_close_time.strftime('%H:%M')}")

        # 1. Open ticket
        ticket_number = generate_ticket_number()
        print(f"    DEBUG: Opening ticket #{ticket_number} for truck {truck['id']}, job_uom={job_uom}")

        # Use appropriate ticket opening method based on job UOM
        if job_uom == 1:  # Hourly - use web API
            print(f"    DEBUG: Using issue_ticket_via_web_api for hourly job")
            success, ticket_id = issue_ticket_via_web_api(
                jo_line_item_id=jo_line_item_id,
                truck_id=truck['id'],
                quantity=0,
                coordinates={"latitude": pickup_coords['lat'], "longitude": pickup_coords['lng']},
                external_ref=ticket_number
            )
            if not success:
                ticket_id = None
            print(f"    DEBUG: Hourly ticket result: success={success}, ticket_id={ticket_id}")
        else:  # Tonnage/Load - use device sync
            print(f"    DEBUG: Using sync_device_action for tonnage/load job")
            success, response_data = sync_device_action(
                action_type="ticketOpened",
                ticket_id=None,
                jo_line_item_id=jo_line_item_id,
                truck_id=truck['id'],
                latitude=pickup_coords['lat'],
                longitude=pickup_coords['lng'],
                event_timestamp=ticket_open_time.isoformat(),
                external_ref=ticket_number
            )
            print(f"    DEBUG: Device sync result: success={success}, response_data={response_data}")

            # Extract ticket ID from response - format is [{'ticketId': 123, 'localId': 'xxx'}]
            ticket_id = None
            if success and response_data and isinstance(response_data, list) and len(response_data) > 0:
                # Just get ticketId from first item in response
                ticket_id = response_data[0].get('ticketId')
                print(f"    DEBUG: Extracted ticket_id={ticket_id} from device sync response")

        if not ticket_id:
            print(f"    ⚠️ Failed to open ticket for trip {trip_num + 1}")
            continue

        print(f"    ✅ Opened ticket #{ticket_id}")

        tickets_created.append(ticket_id)

        # 2. GPS at pickup (loading)
        coords_pickup = []
        for i in range(6):
            lat_offset = random.uniform(-0.0001, 0.0001)
            lng_offset = random.uniform(-0.0001, 0.0001)
            coords_pickup.append({
                "latitude": pickup_coords['lat'] + lat_offset,
                "longitude": pickup_coords['lng'] + lng_offset,
                "speed": 0,
                "heading": bearing,
                "event_timestamp": (ticket_open_time + timedelta(minutes=i*2)).isoformat()
            })
        print(f"    DEBUG: Sending {len(coords_pickup)} pickup GPS points for ticket {ticket_id}")
        send_gps_coordinates_batch(truck['id'], ticket_id, coords_pickup, jo_line_item_id)
        print(f"    ✅ Sent pickup GPS")
        time.sleep(0.3)

        # 3. PickupCompleted
        sync_device_action("PickupCompleted", ticket_id, jo_line_item_id, truck['id'],
                          pickup_coords['lat'], pickup_coords['lng'],
                          event_timestamp=pickup_complete_time.isoformat())
        time.sleep(0.3)

        # 3b. For hourly jobs, create sub-ticket for tonnage tracking
        subticket_id = None
        if job_uom == 1:
            subticket_number = generate_ticket_number()
            subticket_payload = {
                "joLineItemId": jo_line_item_id,
                "truckId": truck['id'],
                "dropOffLocation": dropoff_coords['site_id'],
                "externalRef": subticket_number
            }

            headers = {"Authorization": f"Token {AUTH_TOKEN}", "Content-Type": "application/json"}
            try:
                response = requests.post(
                    f"{API_BASE_URL}/api/2/tickets",
                    json=subticket_payload,
                    headers=headers
                )
                if response.status_code in [200, 201]:
                    subticket_data = response.json().get("data", {})
                    subticket_id = subticket_data.get("id")
                    print(f"    ✅ Created sub-ticket #{subticket_id} for tonnage")
            except Exception as e:
                print(f"    ⚠️ Failed to create sub-ticket: {e}")

        # 4. For last trip with final_state='at_pickup', continue to dropoff then return
        # (will handle at end of loop)

        # 5. Generate varied GPS path en route
        varied_path = generate_varied_gps_path(pickup_coords, dropoff_coords, num_points=30, variation_index=trip_num)
        coords_enroute = []
        time_between_points = (dropoff_complete_time - pickup_complete_time).total_seconds() / len(varied_path)

        for idx, point in enumerate(varied_path):
            coords_enroute.append({
                "latitude": point['lat'],
                "longitude": point['lng'],
                "speed": random.randint(40, 60),  # Highway speed range mph
                "heading": bearing,
                "event_timestamp": (pickup_complete_time + timedelta(seconds=idx * time_between_points)).isoformat()
            })

        print(f"    DEBUG: Sending {len(coords_enroute)} enroute GPS points for ticket {ticket_id}")
        send_gps_coordinates_batch(truck['id'], ticket_id, coords_enroute, jo_line_item_id)
        print(f"    ✅ Sent enroute GPS")
        time.sleep(0.3)

        # 6. For last trip, check if en route
        if is_last_trip and final_state == 'en_route':
            # Stop here - truck is en route
            print(f"    🚗 Final trip - en route (ticket open)")
            break

        # 7. GPS at dropoff (unloading)
        coords_dropoff = []
        for i in range(6):
            lat_offset = random.uniform(-0.0001, 0.0001)
            lng_offset = random.uniform(-0.0001, 0.0001)
            coords_dropoff.append({
                "latitude": dropoff_coords['lat'] + lat_offset,
                "longitude": dropoff_coords['lng'] + lng_offset,
                "speed": 0,
                "heading": bearing,
                "event_timestamp": (dropoff_complete_time + timedelta(minutes=i)).isoformat()
            })
        send_gps_coordinates_batch(truck['id'], ticket_id, coords_dropoff, jo_line_item_id)
        time.sleep(0.3)

        # 8. DropOffCompleted (with tonnage for tonnage jobs)
        tonnage_value = None
        if job_uom == 2:  # Tonnage job - include quantity (use photo values)
            tonnage_value = get_next_tonnage_value()
            sync_device_action("DropOffCompleted", ticket_id, jo_line_item_id, truck['id'],
                              dropoff_coords['lat'], dropoff_coords['lng'],
                              quantity=tonnage_value,
                              event_timestamp=dropoff_complete_time.isoformat())
        else:  # Hourly/Load job - no quantity
            sync_device_action("DropOffCompleted", ticket_id, jo_line_item_id, truck['id'],
                              dropoff_coords['lat'], dropoff_coords['lng'],
                              event_timestamp=dropoff_complete_time.isoformat())
        time.sleep(0.3)

        # 9. Close sub-ticket with tonnage (hourly jobs only)
        if job_uom == 1 and subticket_id:
            tonnage = get_next_hourly_tonnage()
            close_payload = {
                "weight": tonnage,  # API expects 'weight' not 'quantity'
                "latitude": dropoff_coords['lat'],
                "longitude": dropoff_coords['lng'],
                "message": "Sub-ticket closed"
            }
            headers = {"Authorization": f"Token {AUTH_TOKEN}", "Content-Type": "application/json"}
            print(f"    DEBUG: Closing sub-ticket {subticket_id} with {tonnage:.1f} tons")
            try:
                response = requests.post(
                    f"{API_BASE_URL}/api/2/tickets/{subticket_id}/close",
                    json=close_payload,
                    headers=headers
                )
                print(f"    DEBUG: Sub-ticket close response: status={response.status_code}")
                if response.status_code in [200, 201]:
                    print(f"    ✅ Closed sub-ticket #{subticket_id} with {tonnage:.1f} tons")
                    upload_ticket_photo(subticket_id, "hourly", "Hourly job tonnage delivery")
                else:
                    print(f"    ❌ Failed to close sub-ticket. Status: {response.status_code}, Response: {response.text}")
            except Exception as e:
                print(f"    ⚠️ Exception closing sub-ticket: {e}")
            time.sleep(0.3)

        # 10. Close parent ticket
        if job_uom == 1:
            # Hourly jobs: parent ticket closed via web API (no quantity needed - calculated by timer)
            print(f"    DEBUG: Closing parent ticket {ticket_id} (hourly)")
            success, response = sync_device_action("ticketClosed", ticket_id, jo_line_item_id, truck['id'],
                                            dropoff_coords['lat'], dropoff_coords['lng'],
                                            event_timestamp=ticket_close_time.isoformat())
            print(f"    DEBUG: Parent ticket close result: success={success}")
            if success:
                upload_ticket_photo(ticket_id, "timesheets", "Timesheet photo")
                print(f"    ✅ Trip {trip_num + 1} parent ticket #{ticket_id} closed")
            else:
                print(f"    ❌ Failed to close parent ticket #{ticket_id}")
        else:
            # Tonnage jobs: close with same tonnage used in DropOffCompleted
            if tonnage_value is None:
                tonnage_value = get_next_tonnage_value()
            print(f"    DEBUG: Closing tonnage ticket {ticket_id} with {tonnage_value:.2f} tons")
            success, response = sync_device_action("ticketClosed", ticket_id, jo_line_item_id, truck['id'],
                                            dropoff_coords['lat'], dropoff_coords['lng'],
                                            quantity=tonnage_value,
                                            event_timestamp=ticket_close_time.isoformat())
            print(f"    DEBUG: Tonnage ticket close result: success={success}")
            if success:
                upload_ticket_photo(ticket_id, "tonnage", "Delivery ticket photo")
                print(f"    ✅ Trip {trip_num + 1} ticket #{ticket_id} closed with {tonnage_value:.1f} tons")
            else:
                print(f"    ❌ Failed to close tonnage ticket #{ticket_id}")

        # 11. Add return journey GPS for continuous flow
        if not is_last_trip:
            # Non-last trip: always return to pickup for next trip
            return_start_time = ticket_close_time + timedelta(minutes=5)
            return_end_time = return_start_time + timedelta(minutes=20)

            return_path = generate_varied_gps_path(dropoff_coords, pickup_coords, num_points=20, variation_index=trip_num + 10)
            coords_return = []
            time_between_points = (return_end_time - return_start_time).total_seconds() / len(return_path)

            for idx, point in enumerate(return_path):
                coords_return.append({
                    "latitude": point['lat'],
                    "longitude": point['lng'],
                    "speed": random.randint(40, 60),
                    "heading": calculate_bearing(dropoff_coords, pickup_coords),
                    "event_timestamp": (return_start_time + timedelta(seconds=idx * time_between_points)).isoformat()
                })

            print(f"    DEBUG: Sending {len(coords_return)} return journey GPS points")
            send_gps_coordinates_batch(truck['id'], ticket_id, coords_return, jo_line_item_id)
            print(f"    🔄 Added return journey GPS")
            time.sleep(0.3)
        elif final_state == 'at_pickup':
            # Last trip ending at pickup: return to pickup
            return_start_time = ticket_close_time + timedelta(minutes=5)
            return_end_time = return_start_time + timedelta(minutes=20)

            return_path = generate_varied_gps_path(dropoff_coords, pickup_coords, num_points=20, variation_index=trip_num + 10)
            coords_return = []
            time_between_points = (return_end_time - return_start_time).total_seconds() / len(return_path)

            for idx, point in enumerate(return_path):
                coords_return.append({
                    "latitude": point['lat'],
                    "longitude": point['lng'],
                    "speed": random.randint(40, 60),
                    "heading": calculate_bearing(dropoff_coords, pickup_coords),
                    "event_timestamp": (return_start_time + timedelta(seconds=idx * time_between_points)).isoformat()
                })

            print(f"    DEBUG: Sending {len(coords_return)} return journey GPS points (final at pickup)")
            send_gps_coordinates_batch(truck['id'], ticket_id, coords_return, jo_line_item_id)

            # Add stationary GPS at pickup to show truck is there
            stationary_time = return_end_time
            coords_stationary = []
            for i in range(5):
                lat_offset = random.uniform(-0.0001, 0.0001)
                lng_offset = random.uniform(-0.0001, 0.0001)
                coords_stationary.append({
                    "latitude": pickup_coords['lat'] + lat_offset,
                    "longitude": pickup_coords['lng'] + lng_offset,
                    "speed": 0,
                    "heading": calculate_bearing(dropoff_coords, pickup_coords),
                    "event_timestamp": (stationary_time + timedelta(minutes=i*2)).isoformat()
                })
            send_gps_coordinates_batch(truck['id'], ticket_id, coords_stationary, jo_line_item_id)
            print(f"    🅿️  Final position: at pickup")
            time.sleep(0.3)
        elif final_state == 'at_dropoff':
            # Last trip ending at dropoff: add stationary GPS
            stationary_time = ticket_close_time + timedelta(minutes=5)
            coords_stationary = []
            for i in range(5):
                lat_offset = random.uniform(-0.0001, 0.0001)
                lng_offset = random.uniform(-0.0001, 0.0001)
                coords_stationary.append({
                    "latitude": dropoff_coords['lat'] + lat_offset,
                    "longitude": dropoff_coords['lng'] + lng_offset,
                    "speed": 0,
                    "heading": bearing,
                    "event_timestamp": (stationary_time + timedelta(minutes=i*2)).isoformat()
                })
            send_gps_coordinates_batch(truck['id'], ticket_id, coords_stationary, jo_line_item_id)
            print(f"    📍 Final position: at dropoff")
            time.sleep(0.3)
        # For 'en_route' final state, no additional GPS needed (already en route)

        time.sleep(0.5)

    return tickets_created


def setup_truck_states_for_job(job_order_id, jo_line_item_id, created_tickets, trucks, pickup_coords, dropoff_coords, job_uom=None):
    """
    Set up trucks with proper GPS journeys coordinated with ticket lifecycle events via device sync.
    Each truck gets a different end state:
    - Truck 1: Complete journey with CLOSED ticket (PickupCompleted + DropOffCompleted)
    - Truck 2: At pickup with CLOSED ticket (completed a previous trip)
    - Truck 3: En route between pickup and dropoff with OPEN ticket (PickupCompleted only)

    For hourly jobs (UOM=1), creates sub-tickets for tonnage tracking.

    Args:
        job_order_id: The job order ID
        jo_line_item_id: The JOLineItem ID
        created_tickets: List of ticket IDs [ticket1, ticket2, ticket3]
        trucks: List of truck dicts (3 trucks)
        pickup_coords: Dict with 'lat' and 'lng' for pickup location
        dropoff_coords: Dict with 'lat' and 'lng' for dropoff location
        job_uom: Optional UOM ID (1=Hour, 2=Ton, 4=Load). If 1, sub-tickets will be created.
    """
    if not created_tickets or len(created_tickets) < 3:
        print(f"⚠️ Need at least 3 tickets. Got {len(created_tickets) if created_tickets else 0}")
        return

    print(f"\n🚚 Setting up truck states for job {job_order_id}...")

    bearing = calculate_bearing(pickup_coords, dropoff_coords)

    # ===== TRUCK 1: Complete journey with CLOSED ticket =====
    ticket_1 = created_tickets[0]
    truck_1 = trucks[0]
    print(f"  📍 {truck_1['device_name']}: Complete journey → CLOSED ticket")

    # Timestamps for 1.5 hour trip (90 minutes ago to now)
    ticket_open_time = datetime.now(timezone.utc) - timedelta(minutes=90)
    pickup_complete_time = datetime.now(timezone.utc) - timedelta(minutes=70)
    dropoff_complete_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    ticket_close_time = datetime.now(timezone.utc) - timedelta(minutes=2)

    # 1. GPS at pickup (loading) - more realistic with 5-6 points over 15 minutes
    coords_pickup_1 = []
    for i in range(6):
        # Small variations around pickup location to simulate loading activity
        lat_offset = random.uniform(-0.0001, 0.0001)
        lng_offset = random.uniform(-0.0001, 0.0001)
        coords_pickup_1.append({
            "latitude": pickup_coords['lat'] + lat_offset,
            "longitude": pickup_coords['lng'] + lng_offset,
            "speed": 0,
            "heading": bearing,
            "event_timestamp": (ticket_open_time + timedelta(minutes=i*3)).isoformat()
        })
    send_gps_coordinates_batch(truck_1['id'], ticket_1, coords_pickup_1, jo_line_item_id)
    time.sleep(0.5)

    # 2. PickupCompleted action (no quantity - hours calculated by timer)
    success, _ = sync_device_action("PickupCompleted", ticket_1, jo_line_item_id, truck_1['id'],
                                    pickup_coords['lat'], pickup_coords['lng'],
                                    event_timestamp=pickup_complete_time.isoformat())
    if not success:
        print(f"  ⚠️ PickupCompleted failed for {truck_1['device_name']}")
    time.sleep(0.5)

    # 2b. Create OPEN sub-ticket for tonnage tracking (after pickup) - ONLY FOR HOURLY JOBS
    #     Mobile app creates sub-ticket after pickup via POST /api/2/tickets
    #     NOTE: Mobile app does NOT pass historical timestamps - uses current time
    #     DELAY: Wait to spread timestamps across different minutes
    print(f"  ⏳ Waiting 75 seconds before creating sub-ticket...")
    time.sleep(75)

    subticket_1_id = None
    if job_uom == 1:  # Only create sub-tickets for hourly jobs
        subticket_number = generate_ticket_number()
        subticket_payload = {
            "joLineItemId": jo_line_item_id,
            "truckId": truck_1['id'],
            "dropOffLocation": dropoff_coords['site_id'],  # Set dropoff site at creation
            "externalRef": subticket_number
        }
        headers = {
            "Authorization": f"Token {AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(f"{API_BASE_URL}/api/2/tickets", json=subticket_payload, headers=headers)
            if response.status_code in [200, 201]:
                response_data = response.json()
                print(f"  DEBUG: Sub-ticket creation response: {response_data}")
                # Try different possible locations for the ID
                subticket_1_id = response_data.get('id') or response_data.get('data', {}).get('id')
                print(f"  ✅ Created OPEN sub-ticket #{subticket_1_id} ({subticket_number}) for tonnage on {truck_1['device_name']}")
            else:
                print(f"  ⚠️ Sub-ticket creation failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  ⚠️ Sub-ticket creation error: {e}")
        time.sleep(0.5)

    # 3. GPS en route - generate realistic path with 25-30 points
    coords_enroute_1 = []
    num_enroute_points = 28
    journey_minutes = 50  # Travel time from pickup complete to dropoff arrival

    for i in range(num_enroute_points):
        # Calculate progress along route (0 to 1)
        progress = i / (num_enroute_points - 1) if num_enroute_points > 1 else 0

        # Interpolate between pickup and dropoff
        base_lat = pickup_coords['lat'] + (dropoff_coords['lat'] - pickup_coords['lat']) * progress
        base_lng = pickup_coords['lng'] + (dropoff_coords['lng'] - pickup_coords['lng']) * progress

        # Add realistic GPS drift and route variation
        lat_variance = random.uniform(-0.00005, 0.00005)
        lng_variance = random.uniform(-0.00005, 0.00005)

        # Add curve to route (trucks don't drive perfectly straight)
        if 0 < i < num_enroute_points - 1:
            curve_factor = math.sin(progress * math.pi) * 0.0008
            lat_variance += curve_factor * random.uniform(-1, 1)
            lng_variance += curve_factor * random.uniform(-1, 1)

        # Calculate realistic speed (varies during journey)
        if i < 3:  # Accelerating from pickup
            speed = 15 + (i * 10)
        elif i > num_enroute_points - 4:  # Decelerating to dropoff
            speed = 50 - ((num_enroute_points - i) * 10)
        else:  # Cruising speed with variation
            speed = 45 + random.uniform(-8, 12)
        speed = max(5, min(speed, 65))  # Cap between 5-65 mph

        # Calculate timestamp spread over journey
        time_offset = pickup_complete_time + timedelta(minutes=(journey_minutes * progress))

        coords_enroute_1.append({
            "latitude": base_lat + lat_variance,
            "longitude": base_lng + lng_variance,
            "speed": round(speed, 1),
            "heading": bearing + random.uniform(-5, 5),  # Slight heading variation
            "event_timestamp": time_offset.isoformat()
        })

    send_gps_coordinates_batch(truck_1['id'], ticket_1, coords_enroute_1, jo_line_item_id)
    time.sleep(0.5)

    # 4. GPS at dropoff (unloading) - 5-6 points over 10 minutes
    coords_dropoff_1 = []
    for i in range(5):
        # Small variations around dropoff location to simulate unloading activity
        lat_offset = random.uniform(-0.0001, 0.0001)
        lng_offset = random.uniform(-0.0001, 0.0001)
        time_offset = dropoff_complete_time - timedelta(minutes=10-(i*2))
        coords_dropoff_1.append({
            "latitude": dropoff_coords['lat'] + lat_offset,
            "longitude": dropoff_coords['lng'] + lng_offset,
            "speed": 0,
            "heading": bearing,
            "event_timestamp": time_offset.isoformat()
        })
    send_gps_coordinates_batch(truck_1['id'], ticket_1, coords_dropoff_1, jo_line_item_id)
    time.sleep(0.5)

    # 5. DropOffCompleted action (no tonnage - hours calculated by timer)
    success, _ = sync_device_action("DropOffCompleted", ticket_1, jo_line_item_id, truck_1['id'],
                                    dropoff_coords['lat'], dropoff_coords['lng'],
                                    event_timestamp=dropoff_complete_time.isoformat())
    if not success:
        print(f"  ⚠️ DropOffCompleted failed for {truck_1['device_name']}")
    time.sleep(0.5)

    # 6. Close sub-ticket with tonnage (15 tons) - ONLY FOR HOURLY JOBS
    #    Mobile app closes sub-ticket at dropoff via POST /api/2/tickets/{id}/close
    #    NOTE: Mobile app does NOT pass historical timestamps - uses current time
    #    DELAY: Wait to spread timestamps
    print(f"  ⏳ Waiting 60 seconds before closing sub-ticket...")
    time.sleep(60)

    if job_uom == 1 and subticket_1_id:  # Only close sub-tickets for hourly jobs
        close_payload = {
            "weight": 15.0,  # 15 tons delivered
        }
        headers = {
            "Authorization": f"Token {AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/2/tickets/{subticket_1_id}/close",
                json=close_payload,
                headers=headers
            )
            if response.status_code in [200, 201]:
                print(f"  ✅ Closed sub-ticket #{subticket_1_id} with 15 tons on {truck_1['device_name']}")
                # Upload hourly ticket photo to sub-ticket
                upload_ticket_photo(subticket_1_id, "hourly", "Hourly job tonnage delivery")
            else:
                print(f"  ⚠️ Sub-ticket close failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  ⚠️ Sub-ticket close error: {e}")
        time.sleep(0.5)

    # 7. Close parent ticket via web API (for hourly jobs)
    #    Mobile app uses web API for STANDALONE hourly jobs, not device sync
    #    DELAY: Wait to spread timestamps
    print(f"  ⏳ Waiting 45 seconds before closing parent ticket...")
    time.sleep(45)

    if job_uom == 1:  # Hourly jobs
        success = close_ticket_via_web_api(
            ticket_id=ticket_1,
            coordinates={"latitude": dropoff_coords['lat'], "longitude": dropoff_coords['lng']}
        )
        if success:
            print(f"  ✅ Closed parent ticket #{ticket_1} on {truck_1['device_name']}")
            # Upload timesheet photo to parent ticket
            upload_ticket_photo(ticket_1, "timesheets", "Timesheet photo")
        else:
            print(f"  ⚠️ Parent ticket close failed for {truck_1['device_name']}")
    else:
        # For non-hourly jobs, use device sync
        success, _ = sync_device_action("ticketClosed", ticket_1, jo_line_item_id, truck_1['id'],
                                        dropoff_coords['lat'], dropoff_coords['lng'],
                                        event_timestamp=ticket_close_time.isoformat())
        if success:
            # Upload photo based on job type (tonnage or ATP)
            photo_type = "tonnage" if job_uom == 2 else "atp"
            upload_ticket_photo(ticket_1, photo_type, f"Delivery ticket photo")
        else:
            print(f"  ⚠️ ticketClosed failed for {truck_1['device_name']}")
    time.sleep(0.5)

    # ===== TRUCK 2: At pickup with CLOSED ticket (completed previous trip) =====
    ticket_2 = created_tickets[1]
    truck_2 = trucks[1]
    print(f"  📍 {truck_2['device_name']}: At pickup - CLOSED ticket")

    # Timestamps for 2 hour trip (completed)
    ticket2_open_time = datetime.now(timezone.utc) - timedelta(minutes=130)
    ticket2_pickup_complete = datetime.now(timezone.utc) - timedelta(minutes=110)
    ticket2_dropoff_complete = datetime.now(timezone.utc) - timedelta(minutes=20)
    ticket2_close_time = datetime.now(timezone.utc) - timedelta(minutes=15)

    # Simulate a complete previous trip
    # 1. GPS at pickup - 6 points over 18 minutes
    coords_pickup_2_prev = []
    for i in range(6):
        lat_offset = random.uniform(-0.00015, 0.00015)
        lng_offset = random.uniform(-0.00015, 0.00015)
        coords_pickup_2_prev.append({
            "latitude": pickup_coords['lat'] + lat_offset,
            "longitude": pickup_coords['lng'] + lng_offset,
            "speed": 0,
            "heading": bearing,
            "event_timestamp": (ticket2_open_time + timedelta(minutes=i*3)).isoformat()
        })
    send_gps_coordinates_batch(truck_2['id'], ticket_2, coords_pickup_2_prev, jo_line_item_id)
    time.sleep(0.5)

    # 2. PickupCompleted (no quantity - hours calculated by timer)
    success, _ = sync_device_action("PickupCompleted", ticket_2, jo_line_item_id, truck_2['id'],
                                    pickup_coords['lat'], pickup_coords['lng'],
                                    event_timestamp=ticket2_pickup_complete.isoformat())
    if not success:
        print(f"  ⚠️ PickupCompleted failed for {truck_2['device_name']}")
    time.sleep(0.5)

    # 2b. Create OPEN sub-ticket for tonnage tracking (after pickup) - ONLY FOR HOURLY JOBS
    #     NOTE: Mobile app does NOT pass historical timestamps - uses current time
    #     DELAY: Wait to spread timestamps
    print(f"  ⏳ Waiting 80 seconds before creating sub-ticket for Truck 2...")
    time.sleep(80)

    subticket_2_id = None
    if job_uom == 1:  # Only create sub-tickets for hourly jobs
        subticket_number = generate_ticket_number()
        subticket_payload = {
            "joLineItemId": jo_line_item_id,
            "truckId": truck_2['id'],
            "dropOffLocation": dropoff_coords['site_id'],  # Set dropoff site at creation
            "externalRef": subticket_number
        }
        headers = {
            "Authorization": f"Token {AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(f"{API_BASE_URL}/api/2/tickets", json=subticket_payload, headers=headers)
            if response.status_code in [200, 201]:
                response_data = response.json()
                print(f"  DEBUG: Sub-ticket creation response: {response_data}")
                # Try different possible locations for the ID
                subticket_2_id = response_data.get('id') or response_data.get('data', {}).get('id')
                print(f"  ✅ Created OPEN sub-ticket #{subticket_2_id} ({subticket_number}) for tonnage on {truck_2['device_name']}")
            else:
                print(f"  ⚠️ Sub-ticket creation failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  ⚠️ Sub-ticket creation error: {e}")
        time.sleep(0.5)

    # 3. GPS en route - Truck 2 takes a slightly different path (30 points)
    coords_enroute_2 = []
    num_enroute_points = 30
    journey_minutes = 70  # Slightly longer journey for variety

    for i in range(num_enroute_points):
        progress = i / (num_enroute_points - 1) if num_enroute_points > 1 else 0

        # Interpolate between pickup and dropoff
        base_lat = pickup_coords['lat'] + (dropoff_coords['lat'] - pickup_coords['lat']) * progress
        base_lng = pickup_coords['lng'] + (dropoff_coords['lng'] - pickup_coords['lng']) * progress

        # Different route variation pattern for Truck 2
        lat_variance = random.uniform(-0.00008, 0.00008)
        lng_variance = random.uniform(-0.00008, 0.00008)

        if 0 < i < num_enroute_points - 1:
            # Different curve pattern (cosine vs sine)
            curve_factor = math.cos(progress * math.pi) * 0.0012
            lat_variance -= curve_factor * random.uniform(-1, 1)  # Opposite direction
            lng_variance += curve_factor * random.uniform(-1, 1)

        # Different speed profile
        if i < 4:
            speed = 10 + (i * 12)
        elif i > num_enroute_points - 5:
            speed = 55 - ((num_enroute_points - i) * 8)
        else:
            speed = 48 + random.uniform(-10, 10)
        speed = max(5, min(speed, 70))

        # Calculate timestamp from ticket2_pickup_complete
        time_offset = ticket2_pickup_complete + timedelta(minutes=(journey_minutes * progress))

        coords_enroute_2.append({
            "latitude": base_lat + lat_variance,
            "longitude": base_lng + lng_variance,
            "speed": round(speed, 1),
            "heading": bearing + random.uniform(-8, 8),
            "event_timestamp": time_offset.isoformat()
        })

    send_gps_coordinates_batch(truck_2['id'], ticket_2, coords_enroute_2, jo_line_item_id)
    time.sleep(0.5)

    # 4. GPS at dropoff - 5 points over 12 minutes
    coords_dropoff_2 = []
    for i in range(5):
        lat_offset = random.uniform(-0.00012, 0.00012)
        lng_offset = random.uniform(-0.00012, 0.00012)
        time_offset = ticket2_dropoff_complete - timedelta(minutes=12-(i*2.5))
        coords_dropoff_2.append({
            "latitude": dropoff_coords['lat'] + lat_offset,
            "longitude": dropoff_coords['lng'] + lng_offset,
            "speed": 0,
            "heading": bearing,
            "event_timestamp": time_offset.isoformat()
        })
    send_gps_coordinates_batch(truck_2['id'], ticket_2, coords_dropoff_2, jo_line_item_id)
    time.sleep(0.5)

    # 5. DropOffCompleted (no tonnage - hours calculated by timer)
    success, _ = sync_device_action("DropOffCompleted", ticket_2, jo_line_item_id, truck_2['id'],
                                    dropoff_coords['lat'], dropoff_coords['lng'],
                                    event_timestamp=ticket2_dropoff_complete.isoformat())
    if not success:
        print(f"  ⚠️ DropOffCompleted failed for {truck_2['device_name']}")
    time.sleep(0.5)

    # 6. Close sub-ticket with tonnage (20 tons) - ONLY FOR HOURLY JOBS
    #    DELAY: Wait to spread timestamps
    print(f"  ⏳ Waiting 65 seconds before closing sub-ticket for Truck 2...")
    time.sleep(65)

    if job_uom == 1 and subticket_2_id:  # Only close sub-tickets for hourly jobs
        close_payload = {
            "weight": 20.0,  # 20 tons delivered
        }
        headers = {
            "Authorization": f"Token {AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(
                f"{API_BASE_URL}/api/2/tickets/{subticket_2_id}/close",
                json=close_payload,
                headers=headers
            )
            if response.status_code in [200, 201]:
                print(f"  ✅ Closed sub-ticket #{subticket_2_id} with 20 tons on {truck_2['device_name']}")
                # Upload hourly ticket photo to sub-ticket
                upload_ticket_photo(subticket_2_id, "hourly", "Hourly job tonnage delivery")
            else:
                print(f"  ⚠️ Sub-ticket close failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"  ⚠️ Sub-ticket close error: {e}")
        time.sleep(0.5)

    # 7. Close parent ticket via web API (for hourly jobs)
    #    Mobile app uses web API for STANDALONE hourly jobs, not device sync
    #    DELAY: Wait to spread timestamps
    print(f"  ⏳ Waiting 50 seconds before closing parent ticket for Truck 2...")
    time.sleep(50)

    if job_uom == 1:  # Hourly jobs
        success = close_ticket_via_web_api(
            ticket_id=ticket_2,
            coordinates={"latitude": dropoff_coords['lat'], "longitude": dropoff_coords['lng']}
        )
        if success:
            print(f"  ✅ Closed parent ticket #{ticket_2} on {truck_2['device_name']}")
            # Upload timesheet photo to parent ticket
            upload_ticket_photo(ticket_2, "timesheets", "Timesheet photo")
        else:
            print(f"  ⚠️ Parent ticket close failed for {truck_2['device_name']}")
    else:
        # For non-hourly jobs, use device sync
        success, _ = sync_device_action("ticketClosed", ticket_2, jo_line_item_id, truck_2['id'],
                                        dropoff_coords['lat'], dropoff_coords['lng'],
                                        event_timestamp=ticket2_close_time.isoformat())
        if success:
            # Upload photo based on job type (tonnage or ATP)
            photo_type = "tonnage" if job_uom == 2 else "atp"
            upload_ticket_photo(ticket_2, photo_type, f"Delivery ticket photo")
        else:
            print(f"  ⚠️ ticketClosed failed for {truck_2['device_name']}")
    time.sleep(0.5)

    # 8. GPS showing truck CURRENTLY returning from dropoff to pickup (last 10 minutes to now)
    # This simulates the truck actively driving back right now
    coords_return_journey_2 = []
    num_return_points = 25
    return_journey_start = datetime.now(timezone.utc) - timedelta(minutes=10)

    for i in range(num_return_points):
        progress = i / (num_return_points - 1) if num_return_points > 1 else 0
        # Interpolate from dropoff back to pickup
        lat = dropoff_coords['lat'] + (pickup_coords['lat'] - dropoff_coords['lat']) * progress
        lng = dropoff_coords['lng'] + (pickup_coords['lng'] - dropoff_coords['lng']) * progress

        # Add realistic path variation
        if 0.1 < progress < 0.9:
            lat += random.uniform(-0.0004, 0.0004)
            lng += random.uniform(-0.0004, 0.0004)

        # Speed variation (accelerating from 0, cruising, then decelerating)
        if progress < 0.1:
            speed = random.uniform(5, 15)
        elif progress > 0.85:
            speed = random.uniform(10, 25)
        else:
            speed = random.uniform(35, 45)

        timestamp = return_journey_start + timedelta(minutes=10 * progress)

        coords_return_journey_2.append({
            "latitude": lat,
            "longitude": lng,
            "speed": speed,
            "heading": (bearing + 180) % 360,  # Opposite direction
            "event_timestamp": timestamp.isoformat()
        })

    send_gps_coordinates_batch(truck_2['id'], ticket_2, coords_return_journey_2, jo_line_item_id)
    print(f"    📍 Sent {len(coords_return_journey_2)} GPS points for return journey (last 10 min)")
    time.sleep(0.5)

    # 9. GPS at pickup (just arrived, last 2 minutes)
    coords_back_pickup_2 = []
    for i in range(8):
        coords_back_pickup_2.append({
            "latitude": pickup_coords['lat'] + random.uniform(-0.00012, 0.00012),
            "longitude": pickup_coords['lng'] + random.uniform(-0.00012, 0.00012),
            "speed": 0,
            "heading": bearing,
            "event_timestamp": (datetime.now(timezone.utc) - timedelta(seconds=120 - i*15)).isoformat()
        })
    send_gps_coordinates_batch(truck_2['id'], ticket_2, coords_back_pickup_2, jo_line_item_id)
    print(f"    📍 Sent {len(coords_back_pickup_2)} GPS points at pickup (current position)")

    # ===== TRUCK 3: En route with OPEN ticket (PickupCompleted only) =====
    ticket_3 = created_tickets[2]
    truck_3 = trucks[2]
    print(f"  📍 {truck_3['device_name']}: En route - OPEN ticket")

    # Timestamps for ongoing trip (started 40 minutes ago, still en route)
    ticket3_open_time = datetime.now(timezone.utc) - timedelta(minutes=40)
    ticket3_pickup_complete = datetime.now(timezone.utc) - timedelta(minutes=25)

    # 1. GPS at pickup (loading) - 6 points
    coords_pickup_3 = []
    for i in range(6):
        lat_offset = random.uniform(-0.00013, 0.00013)
        lng_offset = random.uniform(-0.00013, 0.00013)
        coords_pickup_3.append({
            "latitude": pickup_coords['lat'] + lat_offset,
            "longitude": pickup_coords['lng'] + lng_offset,
            "speed": 0,
            "heading": bearing,
            "event_timestamp": (ticket3_open_time + timedelta(minutes=i*2.5)).isoformat()
        })
    send_gps_coordinates_batch(truck_3['id'], ticket_3, coords_pickup_3, jo_line_item_id)
    time.sleep(0.5)

    # 2. PickupCompleted action (no quantity - hours calculated by timer)
    success, _ = sync_device_action("PickupCompleted", ticket_3, jo_line_item_id, truck_3['id'],
                                    pickup_coords['lat'], pickup_coords['lng'],
                                    event_timestamp=ticket3_pickup_complete.isoformat())
    if not success:
        print(f"  ⚠️ PickupCompleted failed for {truck_3['device_name']}")
    time.sleep(0.5)

    # 3. GPS en route (currently ~55% of the way to dropoff) - Truck 3 takes yet another path
    coords_enroute_3 = []
    num_enroute_points = 15
    # Start at 40% progress, end at 55% progress (currently en route)
    start_progress = 0.40
    end_progress = 0.55
    minutes_elapsed = 25  # Time since pickup complete

    for i in range(num_enroute_points):
        # Calculate progress along this segment
        segment_progress = i / (num_enroute_points - 1) if num_enroute_points > 1 else 0
        overall_progress = start_progress + (end_progress - start_progress) * segment_progress

        # Interpolate between pickup and dropoff
        base_lat = pickup_coords['lat'] + (dropoff_coords['lat'] - pickup_coords['lat']) * overall_progress
        base_lng = pickup_coords['lng'] + (dropoff_coords['lng'] - pickup_coords['lng']) * overall_progress

        # Third unique route variation pattern
        lat_variance = random.uniform(-0.00006, 0.00006)
        lng_variance = random.uniform(-0.00006, 0.00006)

        if 0 < i < num_enroute_points - 1:
            # Different curve using tangent
            curve_factor = math.tan((overall_progress - 0.5) * 0.5) * 0.0006
            lat_variance += curve_factor
            lng_variance -= curve_factor * 0.5

        # Steady cruising speed with small variations
        speed = 52 + random.uniform(-6, 8)
        speed = max(40, min(speed, 68))

        # Calculate timestamp
        time_offset = ticket3_pickup_complete + timedelta(minutes=(minutes_elapsed * segment_progress))

        coords_enroute_3.append({
            "latitude": base_lat + lat_variance,
            "longitude": base_lng + lng_variance,
            "speed": round(speed, 1),
            "heading": bearing + random.uniform(-6, 6),
            "event_timestamp": time_offset.isoformat()
        })

    send_gps_coordinates_batch(truck_3['id'], ticket_3, coords_enroute_3, jo_line_item_id)

    # NOTE: We do NOT send DropOffCompleted or ticketClosed for Truck3 - ticket stays OPEN

    print(f"  ✅ Truck states configured for job {job_order_id}")


def create_single_gps_point(truck_id, truck_name, job_order_id, ticket_id, lat, lng, speed, heading):
    """
    Create a single GPS tracking point in OpenSearch

    Args:
        truck_id: Truck ID
        truck_name: Truck name
        job_order_id: Job order ID
        ticket_id: Ticket ID
        lat: Latitude
        lng: Longitude
        speed: Speed in mph
        heading: Heading in degrees
    """
    # Generate sensor data
    sensor_data = generate_sensor_data()

    # Create GPS tracking event
    gps_event = {
        "accelerometer.value": sensor_data["accelerometer"]["value"],
        "accelerometer.x": sensor_data["accelerometer"]["x"],
        "accelerometer.y": sensor_data["accelerometer"]["y"],
        "accelerometer.z": sensor_data["accelerometer"]["z"],
        "datetime": datetime.now(timezone.utc).strftime(DATETIME_FORMAT),
        "gyroscope.value": sensor_data["gyroscope"]["value"],
        "gyroscope.x": sensor_data["gyroscope"]["x"],
        "gyroscope.y": sensor_data["gyroscope"]["y"],
        "gyroscope.z": sensor_data["gyroscope"]["z"],
        "heading": heading,
        "job_order_id": job_order_id,
        "location": {
            "type": "point",
            "coordinates": [lng, lat]  # GeoJSON format: [longitude, latitude]
        },
        "magnetometer.value": sensor_data["magnetometer"]["value"],
        "magnetometer.x": sensor_data["magnetometer"]["x"],
        "magnetometer.y": sensor_data["magnetometer"]["y"],
        "magnetometer.z": sensor_data["magnetometer"]["z"],
        "speed": round(speed, 2),
        "ticket_id": ticket_id,
        "truck_id": truck_id,
        "truck_name": truck_name
    }

    try:
        response = es_client.index(
            index="truck",
            body=gps_event
        )
        return True
    except Exception as e:
        print(f"  ❌ Error indexing GPS point for {truck_name}: {e}")
        return False


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
    """Main execution function with controlled setup"""

    global AUTH_TOKEN

    print("🚀 Starting controlled job order and ticket creation process...")

    # 🔐 Step 0: Authenticate WITHOUT device info
    AUTH_TOKEN = authenticate_without_device()
    if not AUTH_TOKEN:
        print("❌ Initial authentication failed. Aborting.")
        return

    # 🧹 Step 0.5: Close all active/not started job orders from prior days
    print("\n🧹 Checking for prior day job orders to close...")
    close_prior_day_jobs()

    # Step 1: Check if "Demo Script Project" exists, create if not
    print("\n📁 Checking for Demo Script Project - Restricted Customer...")
    project_id = None
    project_data = None

    # First try searching by keywords
    headers = {"Authorization": f"Token {AUTH_TOKEN}", "Content-Type": "application/json"}
    try:
        search_response = requests.get(
            f"{API_BASE_URL}/api/2/projects?keywords=Demo Script Project&paginate=false",
            headers=headers
        )
        if search_response.status_code == 200:
            search_results = search_response.json().get("data", [])
            for project in search_results:
                if project.get('name') == 'Demo Script Project - Restricted Customer':
                    project_id = project.get('id')
                    project_data = project
                    print(f"✅ Found existing Demo Script Project - Restricted Customer (ID: {project_id})")
                    break
    except Exception as e:
        print(f"⚠️ Error searching for project: {e}")

    # If not found, get all projects and check
    if not project_id:
        projects = get_projects()
        if projects:
            for project in projects:
                if project.get('name') == 'Demo Script Project - Restricted Customer':
                    project_id = project.get('id')
                    project_data = project
                    print(f"✅ Found existing Demo Script Project - Restricted Customer (ID: {project_id})")
                    break

    if not project_id:
        print("📁 Creating Demo Script Project - Restricted Customer...")
        project_id, project_data = create_project("Demo Script Project - Restricted Customer")
        if not project_id:
            print("❌ Project creation failed. Aborting.")
            return
        print(f"✅ Created Demo Script Project - Restricted Customer (ID: {project_id})")

    # Step 2: Create two sites in Atlanta
    print("\n🏭 Creating pickup site in Atlanta...")
    pickup_site_id, pickup_site_data = create_site(
        name="Demo Pickup Site - Atlanta",
        address="123 Peachtree St NE, Atlanta, GA 30303",
        latitude=33.7490,
        longitude=-84.3880,
        site_type="plant"
    )
    if not pickup_site_id:
        print("❌ Pickup site creation failed. Aborting.")
        return

    print("\n🏗️ Creating dropoff site in Marietta (north of Atlanta)...")
    dropoff_site_id, dropoff_site_data = create_site(
        name="Demo Dropoff Site - Marietta",
        address="2900 Delk Rd SE, Marietta, GA 30067",
        latitude=33.9526,
        longitude=-84.4681,
        site_type="dump"
    )
    if not dropoff_site_id:
        print("❌ Dropoff site creation failed. Aborting.")
        return

    # Tonnage job sites (different from hourly job)
    print("\n🏭 Creating tonnage pickup site in Decatur (east of Atlanta)...")
    tonnage_pickup_site_id, tonnage_pickup_site_data = create_site(
        name="Demo Tonnage Pickup - Decatur",
        address="315 W Ponce de Leon Ave, Decatur, GA 30030",
        latitude=33.7748,
        longitude=-84.2963,
        site_type="plant"
    )
    if not tonnage_pickup_site_id:
        print("❌ Tonnage pickup site creation failed. Aborting.")
        return

    print("\n🏗️ Creating tonnage dropoff site in Sandy Springs (north-west of Atlanta)...")
    tonnage_dropoff_site_id, tonnage_dropoff_site_data = create_site(
        name="Demo Tonnage Dropoff - Sandy Springs",
        address="6600 Roswell Rd NE, Sandy Springs, GA 30328",
        latitude=33.9304,
        longitude=-84.3733,
        site_type="dump"
    )
    if not tonnage_dropoff_site_id:
        print("❌ Tonnage dropoff site creation failed. Aborting.")
        return

    # Step 2b: Ensure sites have geofences for turntimes calculation
    print("\n🔷 Ensuring sites have geofences...")
    print("  Creating geofence for pickup site...")
    ensure_site_has_geofence(
        pickup_site_id,
        site_name="Demo Pickup Site - Atlanta",
        lat=33.7490,
        lng=-84.3880
    )

    print("  Creating geofence for dropoff site...")
    ensure_site_has_geofence(
        dropoff_site_id,
        site_name="Demo Dropoff Site - Marietta",
        lat=33.9526,
        lng=-84.4681
    )

    print("  Creating geofence for tonnage pickup site...")
    ensure_site_has_geofence(
        tonnage_pickup_site_id,
        site_name="Demo Tonnage Pickup - Decatur",
        lat=33.7748,
        lng=-84.2963
    )

    print("  Creating geofence for tonnage dropoff site...")
    ensure_site_has_geofence(
        tonnage_dropoff_site_id,
        site_name="Demo Tonnage Dropoff - Sandy Springs",
        lat=33.9304,
        lng=-84.3733
    )
    print("✅ Geofence setup complete")

    # Step 3: Create three Purchase Orders with different UOMs

    # PO 1: Hourly job (check for existing or create with varied material)
    print("\n📦 Getting or Creating Purchase Order 1 (Hourly)...")
    hourly_po_id, hourly_po_line_item_id, hourly_po_data = get_or_create_purchase_order(
        project_id=project_id,
        pickup_site_id=pickup_site_id,
        dropoff_site_id=dropoff_site_id,
        unit_of_measure_id=1,  # Hour
        po_name="Hourly PO"
    )
    if not hourly_po_line_item_id:
        print("❌ Hourly PO failed. Aborting.")
        return

    # PO 2: Tonnage job (check for existing or create with varied material)
    # Uses DIFFERENT sites from hourly job
    print("\n📦 Getting or Creating Purchase Order 2 (Tonnage)...")
    tonnage_po_id, tonnage_po_line_item_id, tonnage_po_data = get_or_create_purchase_order(
        project_id=project_id,
        pickup_site_id=tonnage_pickup_site_id,
        dropoff_site_id=tonnage_dropoff_site_id,
        unit_of_measure_id=2,  # Ton
        po_name="Tonnage PO"
    )
    if not tonnage_po_line_item_id:
        print("❌ Tonnage PO failed. Aborting.")
        return

    # PO 3: Load-based job (check for existing or create with varied material)
    print("\n📦 Getting or Creating Purchase Order 3 (Load-based)...")
    load_po_id, load_po_line_item_id, load_po_data = get_or_create_purchase_order(
        project_id=project_id,
        pickup_site_id=pickup_site_id,
        dropoff_site_id=dropoff_site_id,
        unit_of_measure_id=4,  # Load
        po_name="Load PO"
    )
    if not load_po_line_item_id:
        print("❌ Load-based PO failed. Aborting.")
        return

    # Step 4: Re-authenticate WITH device info for ticket operations
    print("\n📱 Re-authenticating with mobile device for ticket operations...")
    AUTH_TOKEN = authenticate_with_device()
    if not AUTH_TOKEN:
        print("❌ Device authentication failed. Continuing without ticket start/pause.")
    else:
        # Link all trucks to the device
        print("\n🔗 Linking trucks to device...")
        for truck in TRUCKS:
            link_truck_to_device(truck['id'])
            time.sleep(0.5)  # Small delay between requests

    # Step 5: Get truck regions for activity/GPS data
    regions_data = get_truck_regions()
    truck_regions = get_trucks_with_regions()

    # Step 6: Create three job orders (active, closed, pending) with different UOMs
    print("\n📦 Creating three job orders with different UOMs...")

    # Job 1: Active Hourly job (will have tickets created and left open)
    # Use trucks 0-2 (575123-575125)
    job1_trucks = TRUCKS[0:3]
    print("\n1️⃣ Creating ACTIVE job order (Hourly)...")
    active_job_id, active_job_data, _, _, _ = create_job_order(
        pickup_site_id=pickup_site_id,
        dropoff_site_id=dropoff_site_id,
        po_line_item_id=hourly_po_line_item_id,
        truck_ids=[t["id"] for t in job1_trucks],
        quantity=35.0  # Realistic: 3 trucks * ~11-12 hours each
    )
    if not active_job_id:
        print("❌ Active job creation failed.")
        active_tickets = []
    else:
        print(f"✅ Active job created: {active_job_id}")

        # Create tickets but don't close them
        # Open tickets 90 minutes ago to match the journey start time
        ticket_open_time = (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat()
        active_tickets, active_jo_line_item_id, active_job_uom = create_tickets_for_job_order(
            active_job_id, active_job_data, ticket_open_timestamp=ticket_open_time
        )
        print(f"✅ Created {len(active_tickets)} tickets for active job (left open)")

        # Set up multiple trips for each truck with varied GPS and time offsets
        if active_jo_line_item_id:
            print("\n🚛 Setting up multiple trips for hourly job trucks...")
            pickup_coords = {"lat": pickup_site_data.get("latitude", 33.7490), "lng": pickup_site_data.get("longitude", -84.3880), "site_id": pickup_site_id}
            dropoff_coords = {"lat": dropoff_site_data.get("latitude", 33.9526), "lng": dropoff_site_data.get("longitude", -84.4681), "site_id": dropoff_site_id}

            # Truck 1: 3 trips, final state at dropoff, no offset
            setup_truck_with_multiple_trips(job1_trucks[0], active_jo_line_item_id, pickup_coords, dropoff_coords, active_job_uom, num_trips=3, final_state='at_dropoff', truck_offset_minutes=0)

            # Truck 2: 4 trips, final state at pickup, 45 min offset
            setup_truck_with_multiple_trips(job1_trucks[1], active_jo_line_item_id, pickup_coords, dropoff_coords, active_job_uom, num_trips=4, final_state='at_pickup', truck_offset_minutes=45)

            # Truck 3: 2 trips, final state en route, 90 min offset
            setup_truck_with_multiple_trips(job1_trucks[2], active_jo_line_item_id, pickup_coords, dropoff_coords, active_job_uom, num_trips=2, final_state='en_route', truck_offset_minutes=90)

    # Job 2: Closed Tonnage job (will have tickets created and closed)
    # Use trucks 3-5 (575126-575128)
    # Uses DIFFERENT sites from hourly job
    job2_trucks = TRUCKS[3:6]
    print("\n2️⃣ Creating CLOSED job order (Tonnage)...")
    closed_job_id, closed_job_data, _, _, _ = create_job_order(
        pickup_site_id=tonnage_pickup_site_id,
        dropoff_site_id=tonnage_dropoff_site_id,
        po_line_item_id=tonnage_po_line_item_id,
        truck_ids=[t["id"] for t in job2_trucks],
        quantity=350.0  # Request more than will be delivered (realistic variance)
    )
    if not closed_job_id:
        print("❌ Closed job creation failed.")
    else:
        print(f"✅ Closed job created: {closed_job_id}")

        # Get JO line item for tonnage job
        closed_tickets, closed_jo_line_item_id, closed_job_uom = create_tickets_for_job_order(closed_job_id, closed_job_data)

        # Set up multiple trips for each tonnage truck with varied GPS and time offsets
        if closed_jo_line_item_id:
            print("\n🚛 Setting up multiple trips for tonnage job trucks...")
            print(f"   DEBUG: closed_job_uom={closed_job_uom}, closed_jo_line_item_id={closed_jo_line_item_id}")
            tonnage_pickup_coords = {"lat": tonnage_pickup_site_data.get("latitude", 33.7748), "lng": tonnage_pickup_site_data.get("longitude", -84.2963), "site_id": tonnage_pickup_site_id}
            tonnage_dropoff_coords = {"lat": tonnage_dropoff_site_data.get("latitude", 33.9304), "lng": tonnage_dropoff_site_data.get("longitude", -84.3733), "site_id": tonnage_dropoff_site_id}

            # Truck 4: 5 trips, final state at dropoff, no offset
            setup_truck_with_multiple_trips(job2_trucks[0], closed_jo_line_item_id, tonnage_pickup_coords, tonnage_dropoff_coords, closed_job_uom, num_trips=5, final_state='at_dropoff', truck_offset_minutes=0)

            # Truck 5: 3 trips, final state at pickup, 30 min offset
            setup_truck_with_multiple_trips(job2_trucks[1], closed_jo_line_item_id, tonnage_pickup_coords, tonnage_dropoff_coords, closed_job_uom, num_trips=3, final_state='at_pickup', truck_offset_minutes=30)

            # Truck 6: 4 trips, final state en route, 60 min offset
            setup_truck_with_multiple_trips(job2_trucks[2], closed_jo_line_item_id, tonnage_pickup_coords, tonnage_dropoff_coords, closed_job_uom, num_trips=4, final_state='en_route', truck_offset_minutes=60)

        # Wait for all ticket operations to complete before closing job
        print("⏳ Waiting for all ticket operations to complete...")
        time.sleep(5)

        # Close the job order itself
        print(f"🔒 Closing job order {closed_job_id}...")
        close_job_order(closed_job_id)

    # Job 3: Pending Load-based job (no tickets created)
    # Use truck 6 (575129) - Load-based jobs can only have 0 or 1 truck assigned
    job3_trucks = TRUCKS[6:7]
    print("\n3️⃣ Creating PENDING job order (Load-based)...")
    pending_job_id, pending_job_data, _, _, _ = create_job_order(
        pickup_site_id=pickup_site_id,
        dropoff_site_id=dropoff_site_id,
        po_line_item_id=load_po_line_item_id,
        truck_ids=[job3_trucks[0]["id"]],
        quantity=50.0  # Realistic number of loads
    )
    if not pending_job_id:
        print("❌ Pending job creation failed.")
    else:
        print(f"✅ Pending job created: {pending_job_id} (no tickets created)")

    # Step 7: Create idle time alerts and activity events
    print("\n📊 Creating idle time alerts and activity events...")
    if active_job_id and truck_regions and regions_data:
        create_idle_time_alerts(active_job_id, truck_regions)
        # Create activity events for hourly job trucks only
        create_truck_activity_events(active_job_id, truck_regions, regions_data, trucks_list=job1_trucks)

    # Create activity events for tonnage job trucks
    if closed_job_id and truck_regions and regions_data:
        create_truck_activity_events(closed_job_id, truck_regions, regions_data, trucks_list=job2_trucks)

    # Step 8: Create air tickets (already authenticated with device)
    if AUTH_TOKEN and active_job_id and pickup_site_id:
        create_air_tickets_for_trucks(active_job_id, pickup_site_id)

    # ✅ Summary
    print("\n🎉 PROCESS COMPLETE")
    print(f"  📁 Project: Demo Script Project - Restricted Customer (ID: {project_id})")
    print(f"  🏭 Pickup Site: {pickup_site_id}")
    print(f"  🏗️ Dropoff Site: {dropoff_site_id}")
    print(f"\n  📦 Purchase Orders:")
    print(f"    ⏰ Hourly PO: {hourly_po_id} (Line Item: {hourly_po_line_item_id})")
    print(f"    ⚖️  Tonnage PO: {tonnage_po_id} (Line Item: {tonnage_po_line_item_id})")
    print(f"    🚚 Load-based PO: {load_po_id} (Line Item: {load_po_line_item_id})")
    print(f"\n  📋 Job Orders:")
    print(f"    ✅ Active Job (Hourly): {active_job_id if active_job_id else 'FAILED'}")
    print(f"    🔒 Closed Job (Tonnage): {closed_job_id if closed_job_id else 'FAILED'}")
    print(f"    ⏸️  Pending Job (Load): {pending_job_id if pending_job_id else 'FAILED'}")
    print(f"\n  🚛 Trucks: {[truck['device_name'] for truck in TRUCKS]}")



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
    created_tickets, _, _ = create_tickets_for_job_order(job_order_id, None, 22627)

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
    # NOTE: GPS tracking should be done via setup_truck_states_for_job() with correct coords from job order
    # print(f"Creating GPS tracking data for existing job order {job_order_id}...")
    # create_truck_gps_tracking_data(job_order_id, truck_regions, ticket_ids)  # OLD - uses hardcoded NC coords
    print(f"GPS tracking should be handled via setup_truck_states_for_job() - skipping old function")


if __name__ == "__main__":
    main()