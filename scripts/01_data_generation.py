import psycopg2
import logging
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import numpy as np
from decimal import Decimal  

# ========================
# CONFIGURACIÓN
# ========================
DB_NAME = "fleetlogix"
DB_USER = "fleetlogix_user"
DB_PASS = "fleetlogix123"
DB_HOST = "localhost"
DB_PORT = "5432"

fake = Faker("es_ES")
logging.basicConfig(
    level=logging.INFO,  # Define el nivel mínimo de mensajes que se van a mostrar (INFO, WARNING, ERROR y CRITICAL (pero no DEBUG).)
    format="%(asctime)s - %(levelname)s - %(message)s",
)  # Define cómo se verá cada línea de log en consola (fechahora, nivel, texto)

# ========================
# CONEXIÓN
# ========================
try:
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    logging.info("Conexión a la base de datos establecida.")
except Exception as e:
    logging.error(f"Error al conectar a la base de datos: {e}. Verifique credenciales.")
    raise  # Terminar el script si la conexión falla.


# ========================
# FUNCIONES AUXILIARES
# ========================
def log_load(
    table_name,
    records,
    status="success",
    error_message=None,
    process="synthetic_data_load",
):
    """Inserta registro en tabla de logs"""
    # Usamos cursor aquí, pero es mejor definirlo como argumento de la función si va fuera del MAIN
    start = datetime.now()
    try:
        cursor.execute(
            """
            INSERT INTO load_logs (process_name, table_name, records_inserted, start_time, end_time, status, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
            (
                process,
                table_name,
                records,
                start,
                datetime.now(),
                status,
                error_message,
            ),
        )
        conn.commit()
    except Exception as e:
        # maneja el caso si la tabla load_logs no existe o la inserción falla
        logging.error(f"Error al intentar registrar en load_logs: {e}")
        conn.rollback()

# Función para obtener distribución horaria como vector NumPy
def get_hourly_distribution():
    """Genera el array de probabilidades horarias para usar con np.random.choice."""
    probs = np.array([
    # Madrugada (0h-5h)
    0.02, 0.01, 0.01, 0.01, 0.02, 0.03, 
    # Mañana (6h-11h) - Pico
    0.07, 0.08, 0.10, 0.10, 0.08, 0.07, 
    # Tarde (12h-17h) - Descenso
    0.06, 0.05, 0.05, 0.05, 0.04, 0.04,
    # Noche (18h-23h)
    0.03, 0.02, 0.01, 0.01, 0.01, 0.01,
])
    # Aseguramos que la suma sea 1 (aunque no es estrictamente necesario, es buena práctica)
    return probs / probs.sum()


# ========================
# GENERADORES (Vehicles, Drivers, Routes se mantienen con executemany)
# ========================


# --Vehiculos:
def generate_vehicles(cursor, conn, n=200):
    try:
        vehicle_types = ["Camión Grande", "Camión Mediano", "Van", "Motocicleta"]
        fuel_types = ["Diesel", "Nafta"]
        # Diccionario con rangos de peso por tipo de vehículo
        weight_ranges = {
            "Camión Grande": (10000, 20000),
            "Camión Mediano": (5000, 9999),
            "Van": (1500, 4999),
            "Motocicleta": (200, 1499),
        }
        status_options = ["active", "inactive", "maintenance"]
        weights_status = [
            75,
            5,
            20,
        ]  # 75% de probabilidad de 'active', 5% de 'inactive', 20% de 'maintenance'

        vehicles_data = []

        for _ in range(n):
            vehicle_type = random.choice(vehicle_types)
            acquisition_date = fake.date_between(start_date="-10y", end_date="today")

            vehicles_data.append(
                (
                    fake.unique.license_plate(),
                    vehicle_type,
                    round(
                        random.uniform(*weight_ranges[vehicle_type]), 0
                    ),  # Aleatorio sobre rango del tipo de vehículo desempaquetado (*), 0 decimales.
                    random.choice(fuel_types),
                    acquisition_date,  # fecha de alta entre hace 10 años y hoy.
                    random.choices(status_options, weights=weights_status, k=1)[
                        0
                    ],  # objeto lista de un elemento: [0] se usa para extraerlo
                )
            )

        # Inserción en la tabla
        cursor.executemany(
            """
            INSERT INTO vehicles (license_plate, vehicle_type, capacity_kg, fuel_type, acquisition_date, status)
            VALUES (%s, %s, %s, %s, %s, %s);
        """,
            vehicles_data,
        )
        # Guardar en log
        conn.commit()
        log_load("vehicles", len(vehicles_data), "success")
        logging.info(f"{n} vehicles insertados correctamente")
    except Exception as e:
        conn.rollback()
        log_load("vehicles", 0, "error", str(e))
        logging.error(f"Error insertando vehicles: {e}")


# --Conductores:
def generate_drivers(cursor, conn, n=400):
    try:
        status_options_drivers = ["active", "inactive"]
        weights_status_drivers = [75, 25]  # asigno probabilidades de status

        drivers_data = [
            (
                fake.unique.bothify(text="EMP_###"),
                fake.first_name(),
                fake.last_name(),
                fake.unique.bothify(text="LIC_#####"),
                fake.date_between(start_date="today", end_date="+5y"),
                fake.phone_number(),
                fake.date_between(start_date="-10y", end_date="today"),
                random.choices(
                    status_options_drivers, weights=weights_status_drivers, k=1
                )[0],
            )
            for _ in range(n)
        ]

        cursor.executemany(
            """
            INSERT INTO drivers (employee_code, first_name, last_name, license_number, license_expiry, phone, hire_date, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
            drivers_data,
        )

        conn.commit()
        log_load("drivers", len(drivers_data), "success")
        logging.info(f"{len(drivers_data)} drivers insertados")

    except Exception as e:
        conn.rollback()
        log_load("drivers", 0, "error", str(e))
        logging.error(f"Error insertando drivers: {e}")


# --Rutas:

def generate_routes(cursor, conn, n=50):
    try:
        cities = [
            "Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata",
            "Mar del Plata", "Salta", "San Miguel de Tucumán", "Santa Fe"
        ]

        # Diccionario base de distancias y peajes (solo un sentido)
        routes_info = {
            ("Buenos Aires", "Córdoba"): (695, 7000),
            ("Buenos Aires", "Rosario"): (300, 4500),
            ("Buenos Aires", "Mendoza"): (1050, 9000),
            ("Buenos Aires", "La Plata"): (60, 3000),
            ("Buenos Aires", "Mar del Plata"): (400, 4000),
            ("Buenos Aires", "Salta"): (1500, 12000),
            ("Buenos Aires", "San Miguel de Tucumán"): (1400, 11000),
            ("Buenos Aires", "Santa Fe"): (470, 5000),
            ("Córdoba", "Rosario"): (400, 4000),
            ("Córdoba", "Mendoza"): (650, 6000),
            ("Córdoba", "La Plata"): (750, 7500),
            ("Córdoba", "Mar del Plata"): (1000, 8500),
            ("Córdoba", "Salta"): (1100, 9500),
            ("Córdoba", "San Miguel de Tucumán"): (600, 6000),
            ("Córdoba", "Santa Fe"): (400, 4000),
            ("Rosario", "Mendoza"): (1000, 8500),
            ("Rosario", "La Plata"): (350, 3500),
            ("Rosario", "Mar del Plata"): (500, 5000),
            ("Rosario", "Salta"): (1500, 12000),
            ("Rosario", "San Miguel de Tucumán"): (1300, 11000),
            ("Rosario", "Santa Fe"): (160, 2000),
            ("Mendoza", "La Plata"): (1050, 9500),
            ("Mendoza", "Mar del Plata"): (1200, 11000),
            ("Mendoza", "Salta"): (1600, 13000),
            ("Mendoza", "San Miguel de Tucumán"): (1500, 12500),
            ("Mendoza", "Santa Fe"): (1100, 9000),
            ("La Plata", "Mar del Plata"): (360, 3000),
            ("La Plata", "Salta"): (1550, 12000),
            ("La Plata", "San Miguel de Tucumán"): (1450, 11000),
            ("La Plata", "Santa Fe"): (500, 4500),
            ("Mar del Plata", "Salta"): (1650, 13000),
            ("Mar del Plata", "San Miguel de Tucumán"): (1550, 12500),
            ("Mar del Plata", "Santa Fe"): (600, 5500),
            ("Salta", "San Miguel de Tucumán"): (300, 3500),
            ("Salta", "Santa Fe"): (1200, 10000),
            ("San Miguel de Tucumán", "Santa Fe"): (1100, 9500)
        }

        # Crear también las rutas inversas automáticamente
        full_routes = routes_info.copy()
        for (o, d), (km, toll) in list(routes_info.items()):
            if (d, o) not in full_routes:
                full_routes[(d, o)] = (
                    km * random.uniform(0.97, 1.03),   # pequeña variación de distancia
                    toll * random.uniform(0.9, 1.1)    # pequeña variación de peaje
                )

        # Generar combinaciones posibles de las 9 ciudades
        all_pairs = [(o, d) for o in cities for d in cities if o != d]
        random.shuffle(all_pairs)

        routes_data = []
        for origin, dest in all_pairs:
            if len(routes_data) >= n:
                break

            if (origin, dest) in full_routes:
                distance, toll = full_routes[(origin, dest)]
            else:
                # si no está definido, asignar distancia y peaje promedio al azar
                distance = random.uniform(100, 1600)
                toll = random.uniform(3000, 12000)

            avg_speed = random.uniform(70, 100)
            duration = round(distance / avg_speed, 2)

            routes_data.append((
                f"R_{fake.random_int(1000, 9999)}",
                origin,
                dest,
                round(distance, 1),
                duration,
                round(toll, 0)
            ))

        # Inserción
        cursor.executemany("""
            INSERT INTO routes (route_code, origin_city, destination_city, distance_km, estimated_duration_hours, toll_cost)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, routes_data)

        conn.commit()
        log_load("routes", len(routes_data), "success")
        logging.info(f"{len(routes_data)} rutas insertadas correctamente")

    except Exception as e:
        conn.rollback()
        log_load("routes", 0, "error", str(e))
        logging.error(f"Error insertando rutas: {e}")


        cursor.executemany(
            """
            INSERT INTO routes (route_code, origin_city, destination_city, distance_km, estimated_duration_hours, toll_cost)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            routes_data
        )

        conn.commit()
        log_load("routes", len(routes_data), "success")
        logging.info(f"{len(routes_data)} rutas insertadas correctamente")

    except Exception as e:
        conn.rollback()
        log_load("routes", 0, "error", str(e))
        logging.error(f"Error insertando rutas: {e}")



# --- Trips  ---
def generate_trips(cursor, conn, n=100000, batch_size=5000):
    try:
        logging.info("Iniciando generación de TRIPS con mejoras realistas...")

        # 1. Traigo datos base
        cursor.execute("SELECT vehicle_id, vehicle_type, capacity_kg FROM vehicles WHERE status = 'active'")
        vehicles_data = cursor.fetchall()
        vehicle_to_capacity = {v[0]: float(v[2]) for v in vehicles_data}
        vehicle_to_type = {v[0]: v[1] for v in vehicles_data}

        cursor.execute("SELECT driver_id FROM drivers WHERE status = 'active'")
        drivers = [r[0] for r in cursor.fetchall()]

        cursor.execute("SELECT route_id, distance_km, estimated_duration_hours FROM routes")
        routes_data = cursor.fetchall()
        route_to_distance = {r[0]: float(r[1]) for r in routes_data}
        route_to_duration = {r[0]: float(r[2]) for r in routes_data}
        route_ids = list(route_to_duration.keys())

        if not vehicles_data or not drivers or not route_ids:
            logging.warning("No hay suficientes datos base para generar trips.")
            log_load("trips", 0, "warning", "Faltan vehículos, conductores o rutas")
            return

        # 2. Generación de combinaciones válidas
        start_date = datetime.now() - timedelta(days=730)
        start_ts = pd.to_datetime(start_date)

        chosen_routes = np.random.choice(route_ids, size=n)
        chosen_vehicles = []
        for r in chosen_routes:
            dist = route_to_distance[r]
            # Moto solo si la ruta es corta (<100 km)
            valid_vehicles = [
                v for v in vehicle_to_type.keys()
                if not (vehicle_to_type[v] == "Motocicleta" and dist > 500) # moto no va en viajes de más de 500 km
            ]
            chosen_vehicles.append(random.choice(valid_vehicles))
        chosen_vehicles = np.array(chosen_vehicles)

        chosen_drivers = np.random.choice(drivers, size=n)

        # 3. Generación de tiempos
        day_offsets = np.random.randint(0, 730, size=n)
        hourly_probs = get_hourly_distribution()
        chosen_hours = np.random.choice(np.arange(24), size=n, p=hourly_probs)
        chosen_minutes = np.random.randint(0, 59, size=n)

        dep_datetimes = (
            start_ts
            + pd.to_timedelta(day_offsets, unit="d")
            + pd.to_timedelta(chosen_hours, unit="h")
            + pd.to_timedelta(chosen_minutes, unit="m")
        )

        durations = np.array([route_to_duration[r] for r in chosen_routes])
        max_extra_minutes = (durations * 15).astype(int)
        extra_minutes = np.random.randint(0, max_extra_minutes + 1)
        fixed_delivery_time_minutes = 40

        total_duration = (
            pd.to_timedelta(durations, unit="h")
            + pd.to_timedelta(extra_minutes, unit="m")
            + pd.to_timedelta(fixed_delivery_time_minutes, unit="m")
        )

        arr_datetimes = dep_datetimes + total_duration

        # 4. Cálculo de combustible y peso
        distances = np.array([route_to_distance[r] for r in chosen_routes])
        capacities = np.array([vehicle_to_capacity[v] for v in chosen_vehicles])

        # consumo promedio 8–15 L cada 100 km
        fuel_consumed = np.round(distances * np.random.uniform(0.08, 0.15), 2)
        # carga entre 40% y 90% de la capacidad del vehículo
        total_weight = np.round(capacities * np.random.uniform(0.4, 0.9, size=n), 2)

        # 5. Status
        status_choices = np.array(["completed", "cancelled", "in_progress"])
        status_probs = np.array([0.85, 0.05, 0.10])
        statuses = np.random.choice(status_choices, size=n, p=status_probs)

        # 6. DataFrame final
        df_trips = pd.DataFrame({
            "vehicle_id": chosen_vehicles,
            "driver_id": chosen_drivers,
            "route_id": chosen_routes,
            "departure_datetime": dep_datetimes,
            "arrival_datetime": arr_datetimes,
            "fuel_consumed_liters": fuel_consumed,
            "total_weight_kg": total_weight,
            "status": statuses,
        })

        # 7. Inserción por lotes
        for i in range(0, n, batch_size):
            chunk = df_trips.iloc[i:i + batch_size]
            trip_tuples = [tuple(row) for row in chunk.to_numpy()]
            cursor.executemany("""
                INSERT INTO trips (vehicle_id, driver_id, route_id, departure_datetime, arrival_datetime, fuel_consumed_liters, total_weight_kg, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, trip_tuples)
            conn.commit()
            logging.info(f"Insertados {i + len(chunk)} trips...")

        log_load("trips", n, "success")
        logging.info(f"{n} trips insertados en total.")

    except Exception as e:
        conn.rollback()
        log_load("trips", 0, "error", str(e))
        logging.error(f"Error insertando trips: {e}")


# --- Deliveries (USANDO PANDAS PARA EFICIENCIA EN EL BATCH) ---
def generate_deliveries(cursor, conn, n=400000, batch_size=50):
    try:
        logging.info("Iniciando generación de DELIVERIES con estructura optimizada...")

        # 1. Traer datos de viajes (incluye timestamps para coherencia)
        cursor.execute(
            "SELECT trip_id, departure_datetime, arrival_datetime FROM trips"
        )
        trips_data = cursor.fetchall()

        if not trips_data:
            logging.warning(
                "No se encontraron trips_data. Asegúrese de ejecutar generate_trips primero."
            )
            log_load("deliveries", 0, "warning", "No hay viajes para asociar entregas")
            return

        trips_df = pd.DataFrame(trips_data, columns=["trip_id", "departure", "arrival"])

        delivery_counts = np.array([2, 3, 4, 5, 6])
        delivery_weights = np.array([0.1, 0.2, 0.4, 0.2, 0.1])
        
        # Pool global de clientes con pesos (reutilizable)
        names = [fake.name() for _ in range(500)]
        weights_client = np.random.uniform(0.05, 0.95, size=len(names))
        weights_client /= weights_client.sum()

        all_deliveries_data = []

        for _, trip in trips_df.iterrows():
            num_deliveries = np.random.choice(delivery_counts, p=delivery_weights)

            for delivery_num in range(num_deliveries):
                if len(all_deliveries_data) >= n:
                    break

                # --- Mantener lógica original de scheduled_datetime aleatorio ---
                # Generamos un porcentaje aleatorio del viaje para scheduled
                fraction = np.random.uniform(0.05, 0.95)  # entre 5% y 95% del viaje
                scheduled_datetime = trip["departure"] + fraction * (trip["arrival"] - trip["departure"])
                customer_name = np.random.choice(names, p=weights_client)
                recipient_signature = random.random() < 0.8  # True el 80% de las veces

                # Datos de la entrega
                tracking_number = fake.unique.bothify(text="TN-##########")
                delivery_address = fake.street_address()
                package_weight_kg = round(random.uniform(5, 500), 2)

                # Estado y hora de entrega
                if random.random() < 0.90:
                    status = "delivered"
                    # Añadimos un posible retraso sobre scheduled_datetime
                    delivered_datetime = scheduled_datetime + timedelta(
                        minutes=random.randint(0, 50)
                    )
                else:
                    status = random.choice(["pending", "failed"])
                    delivered_datetime = None

                all_deliveries_data.append(
                    (
                        trip["trip_id"],
                        tracking_number,
                        customer_name,
                        delivery_address,
                        package_weight_kg,
                        scheduled_datetime,
                        delivered_datetime,
                        status,
                        recipient_signature
                    )
                )
            if len(all_deliveries_data) >= n:
                break

        df_deliveries = pd.DataFrame(
            all_deliveries_data,
            columns=[
                "trip_id",
                "tracking_number",
                "customer_name",
                "delivery_address",
                "package_weight_kg",
                "scheduled_datetime",
                "delivered_datetime",
                "delivery_status",
                "recipient_signature"
            ],
        )

        deliveries_inserted = len(df_deliveries)

        for i in range(0, deliveries_inserted, batch_size):
            chunk = df_deliveries.iloc[i : i + batch_size]
            deliveries_array = chunk.to_numpy()

            for row in deliveries_array:
                if pd.isna(row[6]):
                    row[6] = None

            deliveries_tuples = [tuple(row) for row in deliveries_array]

            cursor.executemany(
                """
                INSERT INTO deliveries (trip_id, tracking_number, customer_name, 
                delivery_address, package_weight_kg, scheduled_datetime, delivered_datetime, delivery_status, recipient_signature)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
                deliveries_tuples,
            )

            conn.commit()
            logging.info(f"Insertados {i + len(chunk)} deliveries...")

        log_load("deliveries", deliveries_inserted, "success")
        logging.info(f"{deliveries_inserted} deliveries insertados en total")

    except Exception as e:
        conn.rollback()
        log_load("deliveries", 0, "error", str(e))
        logging.error(f"Error insertando deliveries: {e}")

        
        
# --Entregas:

#def generate_deliveries(cursor, conn, n=400000, batch_size=5000):
#    try:
#        logging.info("Iniciando generación de DELIVERIES con mejoras realistas...")
#
#        cursor.execute("SELECT trip_id, departure_datetime, arrival_datetime FROM trips")
#        trips_data = cursor.fetchall()
#        if not trips_data:
#            logging.warning("No hay trips para generar deliveries.")
#            log_load("deliveries", 0, "warning", "Sin trips base")
#            return
#
#        trips_df = pd.DataFrame(trips_data, columns=["trip_id", "departure_datetime", "arrival_datetime"])
#
#        delivery_counts = np.array([2, 3, 4, 5, 6])
#        delivery_weights = np.array([0.1, 0.2, 0.4, 0.2, 0.1])
#
#        # Pool global de clientes con pesos (para que se repitan pero no siempre los mismos)
#        names = [fake.name() for _ in range(500)]
#        weights_client = np.random.uniform(0.05, 0.95, size=len(names))
#        weights_client /= weights_client.sum()
#
#        all_deliveries_data = []
#
#        for _, trip in trips_df.iterrows():
#            num_deliveries = np.random.choice(delivery_counts, p=delivery_weights)
#
#            for _ in range(num_deliveries):
#                if len(all_deliveries_data) >= n:
#                    break
#
#                fraction = np.random.uniform(0.05, 0.95)
#                scheduled_datetime = trip["departure_datetime"] + fraction * (trip["arrival_datetime"] - trip["departure_datetime"])
#
#                customer_name = np.random.choice(names, p=weights_client)
#                recipient_signature = random.random() < 0.8  # True el 80% de las veces
#
#                tracking_number = fake.unique.bothify(text="TN-##########")
#                delivery_address = fake.street_address()
#                package_weight_kg = round(random.uniform(5, 500), 2)
#
#                if random.random() < 0.9:
#                    status = "delivered"
#                    delivered_datetime = scheduled_datetime + timedelta(minutes=random.randint(0, 30))
#                    # asegurar que no exceda la llegada del trip
#                    if delivered_datetime > trip["arrival_datetime"]:
#                        delivered_datetime = trip["arrival_datetime"]
#                else:
#                    status = random.choice(["pending", "failed"])
#                    delivered_datetime = None  # Si no se entregó, no hay hora de entrega
#
#                # Estado y hora de entrega
#                if random.random() < 0.90:
#                    status = "delivered"
#                    # Añadimos un pequeño retraso sobre scheduled_datetime
#                    delivered_datetime = scheduled_datetime + timedelta(
#                        minutes=random.randint(0, 30)
#                    )
#                else:
#                    status = random.choice(["pending", "failed"])
#                    delivered_datetime = N  # Si no se entregó, no hay hora de entrega
#                
#                all_deliveries_data.append((
#                    trip["trip_id"],
#                    tracking_number,
#                    customer_name,
#                    delivery_address,
#                    package_weight_kg,
#                    scheduled_datetime,
#                    delivered_datetime,
#                    status,
#                    recipient_signature if status == "delivered" else None
#                ))
#
#            if len(all_deliveries_data) >= n:
#                break
#
#        df_deliveries = pd.DataFrame(
#            all_deliveries_data,
#            columns=[
#                "trip_id", "tracking_number", "customer_name", "delivery_address",
#                "package_weight_kg", "scheduled_datetime", "delivered_datetime",
#                "delivery_status", "recipient_signature"
#            ],
#        )
#
#        for i in range(0, len(df_deliveries), batch_size):
#            chunk = df_deliveries.iloc[i:i + batch_size]
#            deliveries_tuples = [tuple(row) for row in chunk.to_numpy()]
#            cursor.executemany("""
#                INSERT INTO deliveries (trip_id, tracking_number, customer_name, delivery_address, package_weight_kg, scheduled_datetime, delivered_datetime, delivery_status, recipient_signature)
#                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#            """, deliveries_tuples)
#            conn.commit()
#            logging.info(f"Insertados {i + len(chunk)} deliveries...")
#
#        log_load("deliveries", len(df_deliveries), "success")
#        logging.info(f"{len(df_deliveries)} deliveries insertados en total.")
#
#    except Exception as e:
#        conn.rollback()
#        log_load("deliveries", 0, "error", str(e))
#        logging.error(f"Error insertando deliveries: {e}")

 # Mantenimiento: (Se mantiene con el bucle for porque N es pequeña y la lógica de fechas es compleja de vectorizar)
def generate_maintenance(cursor, conn, n=5000): 
    try:
        # 1. Traer IDs de vehículos Y SUS FECHAS DE ADQUISICIÓN
        cursor.execute("SELECT vehicle_id, acquisition_date FROM vehicles")
        # vehicles_with_dates será una lista de tuplas: [(id1, fecha1), (id2, fecha2), ...]
        vehicles_with_dates = cursor.fetchall()

        # Diccionario para fácil acceso: {id: fecha_adquisicion}
        vehicle_acquisition_dates = {v[0]: v[1] for v in vehicles_with_dates}
        vehicle_ids = list(
            vehicle_acquisition_dates.keys()
        )  # Lista simple de IDs para elegir al azar

        types = [
            "Cambio de aceite",
            "Revisión de frenos",
            "Cambio de llantas",
            "Mantenimiento general",
            "Revisión de motor",
            "Alineación y balanceo",
        ]

        maintenance_batch = []
        batch_size = 500  # Uso de batch_size para inserción eficiente

        for i in range(n):
            # Selecciono un vehículo
            selected_vehicle_id = random.choice(vehicle_ids)
            # Obtengo su fecha de ingreso (acquisition_date)
            acquisition_date = vehicle_acquisition_dates[selected_vehicle_id]

            # --- GENERACIÓN DE FECHAS CLAVE: Asegurando que maintenance_date > acquisition_date ---

            # Calculo un rango de días desde la adquisición hasta hoy.
            # Convertimos a date para hacer la resta más limpia.
            days_since_acquisition = (datetime.now().date() - acquisition_date).days

            # Si el vehículo se adquirió hoy, evito error estableciendo el mínimo en 0 días.
            min_days_offset = max(0, days_since_acquisition)

            # Genero un número aleatorio de días DENTRO de ese rango (0 a días desde adquisición).
            maintenance_offset_days = random.randint(0, min_days_offset)

            # Calculo la fecha de mantenimiento real.
            # Nota: la fecha de adquisición es un objeto date, necesitamos convertirlo a datetime para sumar timedelta.
            maintenance_date = datetime.combine(
                acquisition_date, datetime.min.time()
            ) + timedelta(days=maintenance_offset_days)

            # 2. NEXT MAINTENANCE DATE: Debe ser MAYOR a la maintenance_date.
            #     Le sumo un offset de 30 a 365 días a la fecha de mantenimiento.
            next_maintenance_date = maintenance_date + timedelta(
                days=random.randint(30, 365)
            )

            maintenance_batch.append(
                (
                    selected_vehicle_id,
                    maintenance_date.date(),  # Usar solo la fecha
                    random.choice(types),
                    "",  # dejo vacío porque fake.sentence() generaba oraciones sin lógica
                    round(random.uniform(1000, 20000), 2),
                    next_maintenance_date.date(),  # Usar solo la fecha
                    fake.company(),
                )
            )

            # Inserto por lotes
            if (i + 1) % batch_size == 0:
                cursor.executemany(
                    """
                    INSERT INTO maintenance (vehicle_id, maintenance_date, maintenance_type, description, cost, next_maintenance_date, performed_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                    maintenance_batch,
                )
                conn.commit()
                logging.info(f"Insertados {i+1} records de maintenance...")
                maintenance_batch = []

        # Inserto lo que quedó
        if maintenance_batch:
            cursor.executemany(
                """
                INSERT INTO maintenance (vehicle_id, maintenance_date, maintenance_type, description, cost, next_maintenance_date, performed_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
                maintenance_batch,
            )
            conn.commit()

        log_load("maintenance", n, "success")
        logging.info(f"{n} maintenance insertados en total")

    except Exception as e:
        conn.rollback()
        log_load("maintenance", 0, "error", str(e))
        logging.error(f"Error insertando maintenance: {e}")


# ========================
# MAIN
# ========================
if __name__ == "__main__":
    logging.info("Iniciando la generación de datos sintéticos...")

    # Ejecución de las funciones:
    generate_vehicles(cursor, conn)
    generate_drivers(cursor, conn)
    generate_routes(cursor, conn)
    generate_trips(cursor, conn)
    generate_deliveries(cursor, conn)
    generate_maintenance(cursor, conn)

    cursor.close()
    conn.close()
    logging.info("Carga finalizada. Conexión a la base de datos cerrada.")
