# scripts/generate_sample_data.py
import csv
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image, ImageDraw
import os
import random
from datetime import datetime, timedelta
import uuid

# --- Configuraciones y Datos de Muestra Base ---

# Determinar la ruta raíz del proyecto para que BASE_LANDING_PATH sea consistente
# __file__ es la ruta a este script (scripts/generate_sample_data.py)
# os.path.dirname(__file__) es el directorio 'scripts/'
# os.path.dirname(os.path.dirname(__file__)) es el directorio raíz del proyecto 'farmia_project/'
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_LANDING_PATH = os.path.join(PROJECT_ROOT, "sample_data", "landing")
# Esto asegura que 'sample_data/landing/' siempre se cree en la raíz del proyecto.

# Datos para CSV (Ventas Online)
PRODUCT_IDS_SALES = ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005', 'PROD006']
CUSTOMER_IDS_SALES = ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005', 'CUST006', 'CUST007']

# Datos para JSON (Inventario Local - Estructurado)
ITEM_NAMES_INV = ["Organic Apples", "Free-Range Eggs", "Whole Wheat Bread", "Almond Milk", "Greek Yogurt", "Quinoa"]
WAREHOUSE_IDS_INV = ['WH01', 'WH02', 'WH03', 'WH04']
ITEM_CATEGORIES_INV = ["Fresh Produce", "Dairy & Eggs", "Bakery", "Pantry Staples", "Beverages", "Frozen Foods"]
ITEM_DESCRIPTIONS_INV = [
    "Crisp and juicy, locally sourced.", "Farm-fresh and organic.", "Artisan baked daily.",
    "Essential for every kitchen.", "Refreshing and natural.", "Quick and convenient meal option."
]
UNITS_OF_MEASURE_INV = ["kg", "pieces", "liters", "dozen", "gallons", "packs"]
AISLES_INV = ["A1", "A2", "A3", "B1", "B2", "C1", "C2", "D1"]
SHELVES_INV = ["S1", "S2", "S3", "S4", "S5"]
BINS_INV = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10"]
SUPPLIER_NAMES_INV = [
    "Green Valley Orchards", "Happy Cow Dairy", "Golden Wheat Bakery",
    "PantryGoods Inc.", "Natural Quench Beverages", "Frosty Delights Co."
]
SUPPLIER_EMAILS_INV = [
    "sales@greenvalley.com", "orders@happycow.com", "info@goldenwheat.com",
    "contact@pantrygoods.com", "support@naturalquench.com", "inquiries@frostydelights.com"
]
PRODUCT_GRADES_INV = ["A", "A-", "B+", "B", "Premium"]

# Datos para AVRO (Sensores IoT)
SENSOR_IDS_IOT = ['SENSOR_FIELD_A01', 'SENSOR_FIELD_A02', 'SENSOR_FIELD_B01', 'SENSOR_FIELD_C01', 'SENSOR_GREENHOUSE_01']

# Datos para PARQUET (Proveedores y Logística)
SUPPLIER_IDS_LOG = ['SUPPLIER_X', 'SUPPLIER_Y', 'SUPPLIER_Z', 'SUPPLIER_W', 'SUPPLIER_V']
PRODUCT_IDS_LOG = ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005', 'PROD006', 'PROD007', 'PROD008', 'PROD009', 'PROD010']
SHIPMENT_STATUSES_LOG = ['In Transit', 'Delivered', 'Processing', 'Delayed', 'Pending Pickup', 'Shipped']

# Datos para IMAGES
IMAGE_COLORS = ["blue", "green", "red", "yellow", "purple", "orange", "grey", "darkblue"]
IMAGE_TEXTS_PRODUCTS = ["Tomato", "Lettuce", "Corn", "Strawberry", "Broccoli"]
IMAGE_TEXTS_FIELDS = ["Plot A", "Plot B", "Greenhouse 1", "Section 3", "Area 51"]

# --- Funciones Auxiliares ---
def get_timestamp_filename_suffix():
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")

# (Las funciones get_random_future_date, get_current_iso_timestamp, get_current_timestamp_millis se mantienen igual
# si las necesitas para otros generadores, aunque no se usan en los ejemplos actuales que has proporcionado)

# --- Generadores de Datos Específicos ---

def generate_sales_csv(dataset_name="sales_online"):
    """Genera un archivo CSV de ventas online."""
    # BASE_LANDING_PATH ahora se refiere a farmia_project/sample_data/landing/
    path = os.path.join(BASE_LANDING_PATH, dataset_name)
    os.makedirs(path, exist_ok=True)
    
    filename = f"sales_data_{get_timestamp_filename_suffix()}.csv"
    file_path = os.path.join(path, filename)
    
    header = ['order_id', 'product_id', 'quantity', 'price', 'customer_id', 'order_date']
    data = []
    num_records = random.randint(5, 20)
    start_date = datetime.now() - timedelta(days=random.randint(0,5))

    for i in range(num_records):
        order_date = (start_date + timedelta(minutes=random.randint(i*5, (i+1)*60))).strftime('%Y-%m-%d %H:%M:%S')
        data.append([
            str(uuid.uuid4()),
            random.choice(PRODUCT_IDS_SALES),
            random.randint(1, 10),
            round(random.uniform(5.0, 200.0), 2),
            random.choice(CUSTOMER_IDS_SALES),
            order_date
        ])
        
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)
    print(f"INFO: Archivo CSV creado: {file_path}")
    return file_path

def generate_inventory_json(dataset_name="inventory_local_structured"):
    """Genera un archivo JSON de inventario con estructura anidada."""
    path = os.path.join(BASE_LANDING_PATH, dataset_name)
    os.makedirs(path, exist_ok=True)
    
    filename = f"inventory_data_structured_{get_timestamp_filename_suffix()}.json"
    file_path = os.path.join(path, filename)
    
    data_list = []
    num_records = random.randint(3, 10)
    
    for i in range(num_records):
        item_name_base = random.choice(ITEM_NAMES_INV)
        category = random.choice(ITEM_CATEGORIES_INV)
        supplier_name = random.choice(SUPPLIER_NAMES_INV)
        received_days_ago = random.randint(1, 30)
        record = {
            "item_id": f"INV{random.randint(1000, 9999)}_{i}",
            "product_details": { "name": item_name_base, "category": category, "description": f"{random.choice(ITEM_DESCRIPTIONS_INV)} {item_name_base.lower()} from {category.lower()}."},
            "stock_management": {
                "current_quantity": random.randint(0, 300), "unit_of_measure": random.choice(UNITS_OF_MEASURE_INV),
                "warehouse_id": random.choice(WAREHOUSE_IDS_INV),
                "location_in_warehouse": { "aisle": random.choice(AISLES_INV), "shelf": random.choice(SHELVES_INV), "bin_number": random.choice(BINS_INV)},
                "low_stock_threshold": random.randint(10, 50)
            },
            "supplier_information": {
                "supplier_id": f"SUPP_{supplier_name.replace(' ', '_').upper()[:15]}{random.randint(1,99)}", "supplier_name": supplier_name,
                "contact_email": random.choice(SUPPLIER_EMAILS_INV)
            },
            "quality_metrics": {
                "grade": random.choice(PRODUCT_GRADES_INV), "received_date": (datetime.now() - timedelta(days=received_days_ago)).strftime("%Y-%m-%d"),
                "expiry_date": (datetime.now() + timedelta(days=random.randint(5, 180))).strftime("%Y-%m-%d")
            },
            "last_updated_timestamp": (datetime.now() - timedelta(minutes=random.randint(0,120*24))).isoformat()+"Z"
        }
        data_list.append(record)
        
    with open(file_path, 'w') as file:
        for record in data_list:
            file.write(json.dumps(record) + '\n')
    print(f"INFO: Archivo JSON (NDJSON) creado: {file_path}")
    return file_path

def generate_iot_avro(dataset_name="iot_farm_sensors"):
    path = os.path.join(BASE_LANDING_PATH, dataset_name)
    os.makedirs(path, exist_ok=True)
    filename = f"iot_data_{get_timestamp_filename_suffix()}.avro"
    file_path = os.path.join(path, filename)
    schema_str = """
    {"type": "record", "name": "SensorReading", "namespace": "com.farmia.iot",
     "fields": [
        {"name": "sensor_id", "type": "string"}, {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "temperature_celsius", "type": "float"}, {"name": "humidity_percent", "type": "float"},
        {"name": "soil_quality_ph", "type": ["null", "float"], "default": null},
        {"name": "location_plot_id", "type": ["null", "string"], "default": null}
    ]}"""
    schema = avro.schema.parse(schema_str)
    records = []
    num_records = random.randint(10, 50)
    base_ts = int(datetime.now().timestamp() * 1000)
    for i in range(num_records):
        records.append({
            "sensor_id": random.choice(SENSOR_IDS_IOT), "timestamp": base_ts + (i * 1000 * random.randint(5,30)),
            "temperature_celsius": round(random.uniform(10.0, 35.0), 2), "humidity_percent": round(random.uniform(30.0, 90.0), 2),
            "soil_quality_ph": round(random.uniform(5.0, 8.0), 1) if random.random() > 0.2 else None,
            "location_plot_id": f"PLOT_{chr(random.randint(65,68))}{random.randint(1,5)}" if random.random() > 0.3 else None
        })
    with open(file_path, "wb") as f:
        writer = DataFileWriter(f, DatumWriter(), schema)
        for record in records: writer.append(record)
        writer.close()
    print(f"INFO: Archivo Avro creado: {file_path}")
    return file_path

def generate_logistics_parquet(dataset_name="supplier_logistics"):
    path = os.path.join(BASE_LANDING_PATH, dataset_name)
    os.makedirs(path, exist_ok=True)
    filename = f"logistics_data_{get_timestamp_filename_suffix()}.parquet"
    file_path = os.path.join(path, filename)
    num_records = random.randint(8, 25)
    data_dict = {
        'shipment_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'supplier_id': [random.choice(SUPPLIER_IDS_LOG) for _ in range(num_records)],
        'product_id': [random.choice(PRODUCT_IDS_LOG) for _ in range(num_records)],
        'quantity': [random.randint(10, 1000) for _ in range(num_records)],
        'dispatch_date': [(datetime.now() - timedelta(days=random.randint(1,10))).strftime("%Y-%m-%d") for _ in range(num_records)],
        'expected_delivery_date': [], 'actual_delivery_date': [],
        'status': [random.choice(SHIPMENT_STATUSES_LOG) for _ in range(num_records)]
    }
    for i in range(num_records):
        dispatch = datetime.strptime(data_dict['dispatch_date'][i], "%Y-%m-%d")
        data_dict['expected_delivery_date'].append((dispatch + timedelta(days=random.randint(2,10))).strftime("%Y-%m-%d"))
        data_dict['actual_delivery_date'].append((dispatch + timedelta(days=random.randint(1,12))).strftime("%Y-%m-%d") if data_dict['status'][i] == 'Delivered' else None)
    table = pa.Table.from_pydict(data_dict)
    pq.write_table(table, file_path)
    print(f"INFO: Archivo Parquet creado: {file_path}")
    return file_path

def generate_dummy_image(dataset_folder="images/products", image_prefix="product_img"):
    # Asegurar que dataset_folder es relativo a BASE_LANDING_PATH
    path = os.path.join(BASE_LANDING_PATH, dataset_folder)
    os.makedirs(path, exist_ok=True)
    filename = f"{image_prefix}_{get_timestamp_filename_suffix()}.png"
    file_path = os.path.join(path, filename)
    width, height = random.randint(100, 300), random.randint(100, 300)
    color = random.choice(IMAGE_COLORS)
    text = (random.choice(IMAGE_TEXTS_PRODUCTS) + " " + str(random.randint(1,100)) if "product" in dataset_folder.lower()
            else random.choice(IMAGE_TEXTS_FIELDS) + " Scan" if "field" in dataset_folder.lower()
            else "Sample " + str(random.randint(1,100)))
    img = Image.new('RGB', (width, height), color=color)
    d = ImageDraw.Draw(img)
    try:
        bbox = d.textbbox((0,0), text) # Requires Pillow 9.2.0+
        text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]
    except AttributeError:
        text_width, text_height = d.textsize(text) if hasattr(d, 'textsize') else (len(text) * 7, 12) # Fallback
    x, y = (width - text_width) / 2, (height - text_height) / 2
    d.text((x, y), text, fill=(255,255,0) if color not in ["yellow","orange"] else (0,0,0))
    img.save(file_path, "PNG")
    print(f"INFO: Archivo de imagen creado: {file_path}")
    return file_path

# --- Función Principal para Generar Archivos ---
def create_sample_files_for_all_types():
    """Crea un archivo de muestra de cada tipo configurado."""
    print("Generando todos los tipos de archivos de muestra...")
    generate_sales_csv()
    generate_inventory_json()
    generate_iot_avro()
    generate_logistics_parquet()
    generate_dummy_image(dataset_folder="images/products", image_prefix="product_img")
    generate_dummy_image(dataset_folder="images/field_scans", image_prefix="field_scan")
    print("\nGeneración de todos los tipos de archivos completada.")

if __name__ == "__main__":
    print(f"INFO: Directorio base para landing de datos: {BASE_LANDING_PATH}")
    # Puedes elegir generar todos los archivos o solo algunos específicos
    # llamando a las funciones generate_...() individualmente.
    
    # Ejemplo: Generar solo archivos CSV e JSON para pruebas rápidas
    generate_sales_csv()
    generate_sales_csv()

    generate_sales_csv()


    # generate_inventory_json()

    # Para generar todos los tipos definidos:
    # create_sample_files_for_all_types()
    
    print(f"\nINFO: Generación de datos de muestra completada. Revisa la carpeta '{BASE_LANDING_PATH}'.")