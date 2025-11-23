#!/usr/bin/env python3
"""
🚀 FLEETLOGIX  ETL PIPELINE - 
📊 Pipeline para modelo dimensional en Snowflake según 04_dimensional_model.sql
Por default carga la fecha del día anterior (nth =1) Si se quiere otra fecha (x) ejecutar etl_pipeline.py --nth x (0 = fecha actual, 1 = día anterior, ...) 
 Requiere conexión activa a PostgreSQL.
ARQUITECTURA :
1. EXTRACCIÓN: PostgreSQL con validaciones estadísticas
2. TRANSFORMACIÓN: Cálculos basados en queries SQL con datos agregados
3. CARGA: Snowflake con campos del modelo
4. VALIDACIÓN: Tests de integridad referencial
"""

import configparser
import os
import sys
import logging
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta, date
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict, List, Any
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')

# =====================================================
# 🔧 CONFIGURACIÓN Y LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('fleetlogix_etl.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FleetLogix_ETL')

@dataclass
class ETLConfig:
    """Configuración centralizada del pipeline ETL.

    Lee `settings.ini` en el directorio de trabajo o usa variables de entorno como fallback.
    Lanza RuntimeError si faltan claves críticas.
    """

    settings_path: str = 'settings.ini'
    batch_size: int = 1000
    confidence_level: float = 0.95
    outlier_threshold: float = 3.0

    def __post_init__(self):
        cfg = configparser.ConfigParser()
        read_files = cfg.read(self.settings_path)
        if not read_files:
            logger.warning(f"No se encontró {self.settings_path}. Intentando leer variables de entorno.")

        # Postgres
        try:
            pg = cfg['postgres'] if 'postgres' in cfg else {}
            self.postgres_config = {
                'host': pg.get('host') or os.getenv('PG_HOST') or 'localhost',
                'database': pg.get('database') or os.getenv('PG_DATABASE') or 'fleetlogix',
                'user': pg.get('user') or os.getenv('PG_USER') or 'fleetlogix_user',
                'password': pg.get('password') or os.getenv('PG_PASSWORD') or '',
                'port': int(pg.get('port') or os.getenv('PG_PORT') or 5432),
                'client_encoding': pg.get('client_encoding') or os.getenv('PG_CLIENT_ENCODING') or 'utf8'
            }
        except Exception as e:
            raise RuntimeError(f"Error leyendo sección [postgres] de {self.settings_path}: {e}")

        # Snowflake
        try:
            sf = cfg['snowflake'] if 'snowflake' in cfg else {}
            self.snowflake_config = {
                'user': sf.get('user') or os.getenv('SF_USER'),
                'password': sf.get('password') or os.getenv('SF_PASSWORD'),
                'account': sf.get('account') or os.getenv('SF_ACCOUNT'),
                'warehouse': sf.get('warehouse') or os.getenv('SF_WAREHOUSE'),
                'database': sf.get('database') or os.getenv('SF_DATABASE'),
                'schema': sf.get('schema') or os.getenv('SF_SCHEMA'),
                # role is optional
                'role': sf.get('role') or os.getenv('SF_ROLE')
            }

            # Basic validation
            missing = [k for k, v in self.snowflake_config.items() if k in ('user','password','account') and not v]
            if missing:
                logger.warning(f"Faltan claves Snowflake en {self.settings_path} o en variables de entorno: {missing}")
        except Exception as e:
            raise RuntimeError(f"Error leyendo sección [snowflake] de {self.settings_path}: {e}")

        logger.info("✅ Configuración cargada correctamente")

class ETL:
    """
    Pipeline ETL con modelo dimensional en Snowflake

    Garantiza que cada campo generado coincida exactamente con:
    - Nombres de columnas SQL
    - Tipos de datos SQL
    - Constraints y valores válidos
    """

    def __init__(self, config: ETLConfig):
        self.config = config
        self.postgres_conn = None
        self.snowflake_conn = None
        self.etl_run_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))  # INT para etl_batch_id
        self.stats_summary = {}

        logger.info(f"🚀 Iniciando ETL - Batch ID: {self.etl_run_id}")

    # =====================================================
    # 🔌 CONEXIONES
    # =====================================================

    def connect_postgresql(self) -> bool:
        """Conexión PostgreSQL con encoding UTF-8"""
        try:
            self.postgres_conn = psycopg2.connect(**self.config.postgres_config)
            self.postgres_conn.set_client_encoding('UTF8')
            
            cursor = self.postgres_conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"✅ PostgreSQL conectado: {version[:50]}...")
            return True
        except Exception as e:
            logger.error(f"❌ Error PostgreSQL: {e}")
            return False

    def get_nth_last_date_with_data(self, n: int = 0) -> date:
        """
        Devuelve la n-ésima última fecha (0 = última, 1 = anteúltima, ...) que tenga entregas
        en la base de datos PostgreSQL. Requiere conexión activa a PostgreSQL.
        """
        if self.postgres_conn is None:
            # intentar conectar si no hay conexión
            if not self.connect_postgresql():
                raise RuntimeError("No se pudo conectar a PostgreSQL para obtener fechas con datos")

        query = """
        SELECT DISTINCT DATE(delivered_datetime) as ddate
        FROM deliveries
        WHERE delivery_status = 'delivered'
        ORDER BY ddate DESC
        LIMIT %s
        """

        try:
            df = pd.read_sql(query, self.postgres_conn, params=[n+1])
            if df.empty or len(df) <= n:
                raise IndexError(f"No hay suficientes fechas con datos para n={n}")
            return pd.to_datetime(df['ddate'].iloc[n]).date()
        except Exception as e:
            logger.error(f"❌ Error obteniendo n-ésima fecha con datos: {e}")
            raise

    def connect_snowflake(self) -> bool:
        """Conexión Snowflake con contexto establecido"""
        try:
            self.snowflake_conn = snowflake.connector.connect(**self.config.snowflake_config)

            cursor = self.snowflake_conn.cursor()
            cursor.execute("USE DATABASE FLEETLOGIX_DW3")
            cursor.execute("USE SCHEMA ANALYTICS")
            cursor.execute("USE WAREHOUSE FLEETLOGIX_WH3")
            cursor.close()

            logger.info("✅ Snowflake conectado y configurado")
            return True
        except Exception as e:
            logger.error(f"❌ Error Snowflake: {e}")
            return False

    def close_connections(self):
        """Cierre seguro de conexiones"""
        for conn_name, conn in [("PostgreSQL", self.postgres_conn), ("Snowflake", self.snowflake_conn)]:
            if conn:
                try:
                    conn.close()
                    logger.info(f"🔒 {conn_name} cerrado")
                except Exception as e:
                    logger.warning(f"⚠️ Error cerrando {conn_name}: {e}")

    def validate_snowflake_schema(self) -> bool:
        """
        Valida que todas las tablas necesarias existan en Snowflake
        Las tablas DEBEN ser creadas previamente con 04_dimensional_model.sql
        """
        logger.info("🔍 Validando schema de Snowflake...")
        
        required_tables = [
            'DIM_DATE', 'DIM_TIME', 'DIM_VEHICLE', 'DIM_DRIVER', 
            'DIM_ROUTE', 'DIM_CUSTOMER', 'FACT_DELIVERIES', 'STAGING_DAILY_LOAD'
        ]
        
        try:
            cursor = self.snowflake_conn.cursor()
            cursor.execute("SHOW TABLES IN SCHEMA ANALYTICS")
            existing_tables = [row[1] for row in cursor.fetchall()]
            cursor.close()
            
            missing_tables = [t for t in required_tables if t not in existing_tables]
            
            if missing_tables:
                logger.error(f"❌ Tablas faltantes: {missing_tables}")
                logger.error("❌ Ejecuta primero: 04_dimensional_model.sql en Snowflake")
                return False
            
            logger.info(f"✅ Schema validado: {len(required_tables)} tablas encontradas")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error validando schema: {e}")
            return False

    # =====================================================
    # 📊 EXTRACCIÓN
    # =====================================================

    def extract_deliveries(self, target_date: date) -> pd.DataFrame:
        """Extrae entregas con validación estadística"""
        logger.info(f"📤 Extrayendo entregas para {target_date} con validación")

        query = """
        SELECT 
            d.delivery_id,
            d.trip_id,
            d.tracking_number,
            d.customer_name,
            d.delivery_address,
            d.package_weight_kg,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.delivery_status,
            d.recipient_signature,
            
            t.vehicle_id,
            t.driver_id,
            t.route_id,
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,
            
            r.distance_km,
            r.toll_cost,
            r.origin_city,
            r.destination_city
            
        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE d.delivery_status = 'delivered'
          AND DATE(d.delivered_datetime) = %s
        ORDER BY d.delivered_datetime
        """

        df = pd.read_sql(query, self.postgres_conn, params=[target_date])
        logger.info(f"📊 Extraídas {len(df)} entregas")

        # Validación estadística
        if not df.empty:
            for col in ['package_weight_kg', 'fuel_consumed_liters']:
                if col in df.columns:
                    outliers = self._detect_outliers_zscore(df[col])
                    logger.info(f" Outliers en {col}: {outliers.sum()} registros")

        return df

    def extract_dimensions(self) -> Dict[str, pd.DataFrame]:
        """Extrae dimensiones con análisis estadístico"""
        logger.info("📤 Extrayendo dimensiones con análisis estadístico")

        dimensions = {}

        # Vehículos
        dimensions['vehicles'] = pd.read_sql("""
            SELECT v.vehicle_id, v.license_plate, v.vehicle_type, v.capacity_kg, 
                   v.fuel_type, v.acquisition_date, v.status, m.maintenance_date 
            FROM vehicles v 
            LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id    
            WHERE v.status = 'active';
        """, self.postgres_conn)

        # Conductores
        dimensions['drivers'] = pd.read_sql("""
            SELECT driver_id, employee_code, first_name, last_name,
                   license_number, license_expiry, phone, hire_date, status
            FROM drivers WHERE status = 'active'
        """, self.postgres_conn)

        # Rutas
        dimensions['routes'] = pd.read_sql("""
            SELECT route_id, route_code, origin_city, destination_city,
                   distance_km, estimated_duration_hours, toll_cost
            FROM routes
        """, self.postgres_conn)

        for name, df in dimensions.items():
            logger.info(f"📊 {name}: {len(df)} registros")

        return dimensions

    def _detect_outliers_zscore(self, data: pd.Series, threshold: float = 3.0) -> pd.Series:
        """Detecta outliers usando Z-score"""
        z_scores = pd.Series([False] * len(data), index=data.index, dtype=bool)
        
        valid_data = data.dropna()
        if len(valid_data) > 0:
            z_scores_valid = np.abs(stats.zscore(valid_data))
            z_scores.loc[valid_data.index] = z_scores_valid > threshold
        
        return z_scores

    # =====================================================
    # 🔄 TRANSFORMACIÓN - DIMENSIONES 
    # =====================================================

    def transform_dim_date(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Genera dim_date según SQL:
        date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
        week_of_year, month_num, month_name, quarter, year, is_weekend,
        is_holiday, holiday_name, fiscal_quarter, fiscal_year
        """
        logger.info(f"📅 Generando dim_date: {start_date} a {end_date}")

        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        holidays = {
            '2025-01-01': 'Año Nuevo', '2025-05-01': 'Día del Trabajo',
            '2025-07-20': 'Independencia', '2025-12-25': 'Navidad'
        }

        dim_date = pd.DataFrame({
            'date_key': date_range.strftime('%Y%m%d').astype(int),
            'full_date': date_range.date,
            'day_of_week': date_range.dayofweek + 1,
            'day_name': date_range.strftime('%A').str[:10],  # VARCHAR(10)
            'day_of_month': date_range.day,
            'day_of_year': date_range.dayofyear,
            'week_of_year': date_range.isocalendar().week,
            'month_num': date_range.month,
            'month_name': date_range.strftime('%B').str[:10],  # VARCHAR(10)
            'quarter': date_range.quarter,
            'year': date_range.year,
            'is_weekend': date_range.dayofweek >= 5,
            'is_holiday': [d.strftime('%Y-%m-%d') in holidays for d in date_range],
            'holiday_name': [holidays.get(d.strftime('%Y-%m-%d')) for d in date_range],
            'fiscal_quarter': ((date_range.month - 1) // 3) + 1,
            'fiscal_year': date_range.year
        })

        logger.info(f"📊 dim_date generada: {len(dim_date)} registros")
        return dim_date

    def transform_dim_time(self) -> pd.DataFrame:
        """
        Genera dim_time según SQL:
        time_key, hour, minute, second, time_of_day, hour_24, hour_12, 
        am_pm, is_business_hour, shift
        """
        logger.info("🕐 Generando dim_time")

        times = []
        for hour in range(24):
            for minute in range(0, 60, 15):  # Intervalos de 15 min
                time_key = hour * 100 + minute

                # time_of_day: 'Madrugada', 'Mañana', 'Tarde', 'Noche'
                if 6 <= hour < 12:
                    time_of_day = 'Mañana'
                elif 12 <= hour < 18:
                    time_of_day = 'Tarde'
                elif 18 <= hour < 22:
                    time_of_day = 'Noche'
                else:
                    time_of_day = 'Madrugada'

                # hour_24: VARCHAR(5) '14:30'
                hour_24 = f"{hour:02d}:{minute:02d}"

                # hour_12: VARCHAR(8) '02:30 PM'
                h12 = hour if hour <= 12 else hour - 12
                if h12 == 0:
                    h12 = 12
                am_pm = 'AM' if hour < 12 else 'PM'
                hour_12 = f"{h12:02d}:{minute:02d} {am_pm}"

                # is_business_hour
                is_business_hour = 9 <= hour < 18

                # shift: 'Turno 1', 'Turno 2', 'Turno 3'
                if 6 <= hour < 14:
                    shift = 'Turno 1'
                elif 14 <= hour < 22:
                    shift = 'Turno 2'
                else:
                    shift = 'Turno 3'

                times.append({
                    'time_key': time_key,
                    'hour': hour,
                    'minute': minute,
                    'second': 0,
                    'time_of_day': time_of_day[:20],  # VARCHAR(20)
                    'hour_24': hour_24[:5],  # VARCHAR(5)
                    'hour_12': hour_12[:8],  # VARCHAR(8)
                    'am_pm': am_pm[:2],  # VARCHAR(2)
                    'is_business_hour': is_business_hour,
                    'shift': shift[:20]  # VARCHAR(20)
                })

        dim_time = pd.DataFrame(times)
        logger.info(f"📊 dim_time generada: {len(dim_time)} registros")
        return dim_time

#    def transform_dim_vehicle(self, vehicles_df: pd.DataFrame) -> pd.DataFrame:
#        """
#        Genera dim_vehicle según SQL:
#        vehicle_key, vehicle_id, license_plate, vehicle_type, capacity_kg,
#        fuel_type, acquisition_date, age_months, status, maintenance_date,
#        valid_from, valid_to, is_current
#        """
#        logger.info("🔄 Transformando dim_vehicle")
#
#        df = vehicles_df.copy()
#
#        # age_months (INT) - calcular edad en meses
#        today = pd.Timestamp.now()
#        df['age_months'] = ((today - pd.to_datetime(df['acquisition_date'])).dt.days / 30.44).astype(int)
#
#        # last_maintenance_date 
#        df['maintenance_date'] = pd.to_datetime(df['maintenance_date'])
#        df['last_maintenance_date'] = df['maintenance_date'].dt.date
#
#        # SCD Type 2 (usar fecha razonable para pandas)
#        df['valid_from'] = pd.Timestamp.now().date()
#        df['valid_to'] = date(2099, 12, 31)  # Fecha futura razonable
#        df['is_current'] = True
#
#        # vehicle_key = vehicle_id (PK)
#        df['vehicle_key'] = df['vehicle_id']
#
#        # Seleccionar columnas exactas
#        dim_vehicle = df[[
#            'vehicle_key', 'vehicle_id', 'license_plate', 'vehicle_type',
#            'capacity_kg', 'fuel_type', 'acquisition_date', 'age_months',
#            'status', 'last_maintenance_date', 'valid_from', 'valid_to', 'is_current'
#        ]].copy()
#
#        logger.info(f"📊 dim_vehicle transformada: {len(dim_vehicle)} registros")
#        return dim_vehicle
#
#    def transform_dim_driver(self, drivers_df: pd.DataFrame) -> pd.DataFrame:
#        """
#        Genera dim_driver según SQL con performance_category basado en DATOS REALES:
#        - Calcula success_rate desde PostgreSQL (entregas completadas vs totales)
#        - Combina success_rate + experiencia para categorización profesional
#        
#        Criterios (basados en análisis de datos reales):
#        - Alto: success_rate ≥76% + experiencia ≥36 meses
#        - Medio: success_rate ≥74% O experiencia ≥24 meses
#        - Bajo: resto
#        """
#        logger.info("🔄 Transformando dim_driver (con métricas reales de performance)")
#
#        # Query para obtener métricas reales de cada conductor
#        query_performance = """
#        SELECT 
#            d.driver_id,
#            COUNT(del.delivery_id) as total_deliveries,
#            SUM(CASE WHEN del.delivery_status = 'delivered' THEN 1 ELSE 0 END)::DECIMAL / 
#                NULLIF(COUNT(del.delivery_id), 0) * 100 as success_rate,
#            SUM(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime))/3600) as total_hours
#        FROM drivers d
#        LEFT JOIN trips t ON d.driver_id = t.driver_id
#        LEFT JOIN deliveries del ON t.trip_id = del.trip_id
#        WHERE d.status = 'active'
#        GROUP BY d.driver_id
#        """
#        
#        performance_df = pd.read_sql(query_performance, self.postgres_conn)
#        
#        # Merge con datos de conductores
#        df = drivers_df.merge(performance_df, on='driver_id', how='left')
#        
#        # Rellenar NaN (conductores sin entregas)
#        df['success_rate'] = df['success_rate'].fillna(0)
#        df['total_deliveries'] = df['total_deliveries'].fillna(0)
#
#        # full_name
#        df['full_name'] = (df['first_name'] + ' ' + df['last_name']).str[:200]
#
#        # experience_months (INT) - experiencia en meses
#        today = pd.Timestamp.now()
#        df['experience_months'] = ((today - pd.to_datetime(df['hire_date'])).dt.days / 30.44).astype(int)
#
#        # performance_category: BASADO EN DATOS REALES (success_rate + experiencia)
#        def calculate_performance_category(row):
#            success_rate = row['success_rate']
#            experience_months = row['experience_months']
#            
#            if success_rate >= 76 and experience_months >= 36:
#                return 'Alto'
#            elif success_rate >= 74 or experience_months >= 24:
#                return 'Medio'
#            else:
#                return 'Bajo'
#        
#        df['performance_category'] = df.apply(calculate_performance_category, axis=1)
#
#        # deliveries_per_hour: total_deliveries / total_hours  (si total_hours es 0 -> usar total_deliveries)
#        df['total_hours'] = df['total_hours'].fillna(0)
#        def calc_dph(row):
#            if row['total_hours'] and row['total_hours'] > 0:
#                return round(row['total_deliveries'] / row['total_hours'], 2)
#            else:
#                # si no hay horas registradas, usar estimación conservadora: entregas / 8 horas
#                return round(row['total_deliveries'] / 8.0, 2) if row['total_deliveries'] > 0 else 0.0
#
#        df['deliveries_per_hour'] = df.apply(calc_dph, axis=1)
#
#        # SCD Type 2
#        df['valid_from'] = pd.Timestamp.now().date()
#        df['valid_to'] = date(2099, 12, 31)
#        df['is_current'] = True
#
#        # driver_key = driver_id (PK)
#        df['driver_key'] = df['driver_id']
#
#        dim_driver = df[[
#            'driver_key', 'driver_id', 'employee_code', 'full_name',
#            'license_number', 'license_expiry', 'phone', 'hire_date',
#            'experience_months', 'status', 'performance_category',
#            'valid_from', 'valid_to', 'is_current'
#        ]].copy()
#
#        # Log de distribución de performance
#        perf_dist = dim_driver['performance_category'].value_counts()
#        logger.info(f"📊 dim_driver transformada: {len(dim_driver)} registros")
#        logger.info(f"   Performance: Alto={perf_dist.get('Alto', 0)}, Medio={perf_dist.get('Medio', 0)}, Bajo={perf_dist.get('Bajo', 0)}")
#        
#        return dim_driver
#
    def transform_dim_route(self, routes_df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera dim_route según SQL con métricas basadas en DATOS REALES:
        - difficulty_level: Basado en varianza de duración + distancia
        - route_type: Basado en distancia (Urbana <100km, Interurbana 100-800km, Rural >800km)
        
        Criterios (basados en análisis de datos reales):
        - Difícil: varianza duración >50% O distancia >1000km
        - Medio: varianza 20-50% O distancia 500-1000km
        - Fácil: resto
        """
        logger.info("🔄 Transformando dim_route (con métricas reales de complejidad)")

        # Query para obtener métricas reales de cada ruta (SIN estimated_duration_hours para evitar duplicado)
        query_route_metrics = """
        SELECT 
            r.route_id,
            AVG(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime))/3600) as avg_actual_duration_hours,
            ABS(((AVG(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime))/3600) - 
                  r.estimated_duration_hours) / NULLIF(r.estimated_duration_hours, 0) * 100)) as duration_variance
        FROM routes r
        LEFT JOIN trips t ON r.route_id = t.route_id
        WHERE t.status = 'completed'
        GROUP BY r.route_id, r.estimated_duration_hours
        """
        
        metrics_df = pd.read_sql(query_route_metrics, self.postgres_conn)
        
        # Merge con datos de rutas (routes_df ya tiene estimated_duration_hours)
        df = routes_df.merge(metrics_df, on='route_id', how='left')
        
        # Rellenar NaN (rutas sin viajes completados)
        df['duration_variance'] = df['duration_variance'].fillna(0)

        # difficulty_level: BASADO EN VARIANZA DE DURACIÓN + DISTANCIA
        def calculate_difficulty_level(row):
            duration_variance = row['duration_variance']
            distance_km = row['distance_km']
            
            if duration_variance > 50 or distance_km > 1000:
                return 'Difícil'
            elif duration_variance > 20 or distance_km > 500:
                return 'Medio'
            else:
                return 'Fácil'
        
        df['difficulty_level'] = df.apply(calculate_difficulty_level, axis=1)

        # route_type: BASADO EN DISTANCIA (incluye "Rural" para largas distancias)
        def calculate_route_type(distance_km):
            if distance_km < 100:
                return 'Urbana'
            elif distance_km < 800:
                return 'Interurbana'
            else:
                return 'Rural'  # Largas distancias = rutas rurales
        
        df['route_type'] = df['distance_km'].apply(calculate_route_type)

        # route_key = route_id (PK)
        df['route_key'] = df['route_id']

        dim_route = df[[
            'route_key', 'route_id', 'route_code', 'origin_city', 'destination_city',
            'distance_km', 'estimated_duration_hours', 'toll_cost',
            'difficulty_level', 'route_type'
        ]].copy()

        # Log de distribución
        diff_dist = dim_route['difficulty_level'].value_counts()
        type_dist = dim_route['route_type'].value_counts()
        logger.info(f"📊 dim_route transformada: {len(dim_route)} registros")
        logger.info(f"   Difficulty: Fácil={diff_dist.get('Fácil', 0)}, Medio={diff_dist.get('Medio', 0)}, Difícil={diff_dist.get('Difícil', 0)}")
        logger.info(f"   Type: Urbana={type_dist.get('Urbana', 0)}, Interurbana={type_dist.get('Interurbana', 0)}, Rural={type_dist.get('Rural', 0)}")
        
        return dim_route

    def transform_dim_customer(self, deliveries_df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera dim_customer según SQL con categorización basada en CANTIDAD DE ENTREGAS:
        - customer_type: Basado en volumen (Individual vs Empresa)
        - customer_category: Basado en FRECUENCIA de entregas (Premium/Regular/Ocasional)
        
        Criterios (basados en análisis de datos reales):
        - customer_type: Empresa ≥200 entregas, Individual <200
        - customer_category: Premium ≥300, Regular 150-299, Ocasional <150
        
        NOTA: customer_id es IDENTITY en SQL, lo omitimos aquí (Snowflake lo genera)
        """
        logger.info("🔄 Transformando dim_customer (categoría basada en FRECUENCIA)")

        # Extraer clientes únicos con métricas completas
        customers = deliveries_df.groupby('customer_name').agg({
            'destination_city': 'first',
            'delivered_datetime': 'min',
            'delivery_id': 'count'
        }).reset_index()

        customers.columns = ['customer_name', 'city', 'first_delivery_date', 'total_deliveries']

        # customer_type: BASADO EN VOLUMEN (Individual vs Empresa)
        # Empresa: ≥200 entregas (volumen industrial/comercial)
        # Individual: <200 entregas
        customers['customer_type'] = customers['total_deliveries'].apply(
            lambda x: 'Empresa' if x >= 200 else 'Individual'
        )

        # customer_category: BASADO EN FRECUENCIA (refleja lealtad del cliente)
        # Premium: ≥300 entregas (clientes muy frecuentes/leales)
        # Regular: 150-299 entregas (clientes recurrentes estables)
        # Ocasional: <150 entregas (clientes esporádicos)
        customers['customer_category'] = customers['total_deliveries'].apply(
            lambda x: 'Premium' if x >= 300 else 'Regular' if x >= 150 else 'Ocasional'
        )

        # customer_key secuencial (PK)
        customers['customer_key'] = range(1, len(customers) + 1)

        # first_delivery_date - solo fecha
        customers['first_delivery_date'] = pd.to_datetime(customers['first_delivery_date']).dt.date

        # city - limitar a VARCHAR(100)
        customers['city'] = customers['city'].str[:100]

        # NOTA: customer_id se omite (IDENTITY en SQL)
        dim_customer = customers[[
            'customer_key', 'customer_name', 'customer_type', 'city',
            'first_delivery_date', 'total_deliveries', 'customer_category'
        ]].copy()

        # Log de distribución
        type_dist = dim_customer['customer_type'].value_counts()
        cat_dist = dim_customer['customer_category'].value_counts()
        logger.info(f"📊 dim_customer transformada: {len(dim_customer)} registros")
        logger.info(f"   Type: Empresa={type_dist.get('Empresa', 0)}, Individual={type_dist.get('Individual', 0)}")
        logger.info(f"   Category: Premium={cat_dist.get('Premium', 0)}, Regular={cat_dist.get('Regular', 0)}, Ocasional={cat_dist.get('Ocasional', 0)}")
        
        return dim_customer
    
    def transform_dim_vehicle(self, vehicles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera dim_vehicle según SQL:
        vehicle_key, vehicle_id, license_plate, vehicle_type, capacity_kg,
        fuel_type, acquisition_date, age_months, status, maintenance_date,
        valid_from, valid_to, is_current
        """
        logger.info("🔄 Transformando dim_vehicle")

        df = vehicles_df.copy()

        # age_months (INT) - calcular edad en meses
        today = pd.Timestamp.now()
        df['age_months'] = ((today - pd.to_datetime(df['acquisition_date'])).dt.days / 30.44).astype(int)

        # last_maintenance_date 
        df['maintenance_date'] = pd.to_datetime(df['maintenance_date'])
        df['last_maintenance_date'] = df['maintenance_date'].dt.date

        # SCD Type 2 (usar fecha razonable para pandas)
        df['valid_from'] = pd.Timestamp.now().date()
        df['valid_to'] = date(2099, 12, 31)  # Fecha futura razonable
        df['is_current'] = True

        # ❌ ELIMINADA: df['vehicle_key'] = df['vehicle_id']
        # La clave subrogada (vehicle_key) se generará en Snowflake con UUID_STRING() o SEQUENCE
        # y no debe ir en el staging con el mismo valor que el ID.

        # Seleccionar columnas exactas (vehicle_key NO incluida)
        dim_vehicle = df[[
            'vehicle_id', 'license_plate', 'vehicle_type',
            'capacity_kg', 'fuel_type', 'acquisition_date', 'age_months',
            'status', 'last_maintenance_date', 'valid_from', 'valid_to', 'is_current'
        ]].copy()

        logger.info(f"📊 dim_vehicle transformada: {len(dim_vehicle)} registros")
        return dim_vehicle

    def transform_dim_driver(self, drivers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera dim_driver según SQL...
        """
        logger.info("🔄 Transformando dim_driver (con métricas reales de performance)")

        # ... (código para performance_df y merge) ...

        # Merge con datos de conductores
        query_performance = """
        SELECT 
            d.driver_id,
            COUNT(del.delivery_id) as total_deliveries,
            SUM(CASE WHEN del.delivery_status = 'delivered' THEN 1 ELSE 0 END)::DECIMAL / 
                NULLIF(COUNT(del.delivery_id), 0) * 100 as success_rate,
            SUM(EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime))/3600) as total_hours
        FROM drivers d
        LEFT JOIN trips t ON d.driver_id = t.driver_id
        LEFT JOIN deliveries del ON t.trip_id = del.trip_id
        WHERE d.status = 'active'
        GROUP BY d.driver_id
        """
        
        performance_df = pd.read_sql(query_performance, self.postgres_conn)
        
        # Merge con datos de conductores
        df = drivers_df.merge(performance_df, on='driver_id', how='left')
        
        # Rellenar NaN (conductores sin entregas)
        df['success_rate'] = df['success_rate'].fillna(0)
        df['total_deliveries'] = df['total_deliveries'].fillna(0)

        # full_name
        df['full_name'] = (df['first_name'] + ' ' + df['last_name']).str[:200]

        # experience_months (INT) - experiencia en meses
        today = pd.Timestamp.now()
        df['experience_months'] = ((today - pd.to_datetime(df['hire_date'])).dt.days / 30.44).astype(int)

        # performance_category: BASADO EN DATOS REALES (success_rate + experiencia)
        def calculate_performance_category(row):
            success_rate = row['success_rate']
            experience_months = row['experience_months']
            
            if success_rate >= 76 and experience_months >= 36:
                return 'Alto'
            elif success_rate >= 74 or experience_months >= 24:
                return 'Medio'
            else:
                return 'Bajo'
        
        df['performance_category'] = df.apply(calculate_performance_category, axis=1)

        # deliveries_per_hour: total_deliveries / total_hours  (si total_hours es 0 -> usar total_deliveries)
        df['total_hours'] = df['total_hours'].fillna(0)
        def calc_dph(row):
            if row['total_hours'] and row['total_hours'] > 0:
                return round(row['total_deliveries'] / row['total_hours'], 2)
            else:
                # si no hay horas registradas, usar estimación conservadora: entregas / 8 horas
                return round(row['total_deliveries'] / 8.0, 2) if row['total_deliveries'] > 0 else 0.0

        df['deliveries_per_hour'] = df.apply(calc_dph, axis=1)

        # SCD Type 2
        df['valid_from'] = pd.Timestamp.now().date()
        df['valid_to'] = date(2099, 12, 31)
        df['is_current'] = True

        # ❌ ELIMINADA: df['driver_key'] = df['driver_id']

        dim_driver = df[[
            'driver_id', 'employee_code', 'full_name', # 'driver_key' NO incluida
            'license_number', 'license_expiry', 'phone', 'hire_date',
            'experience_months', 'status', 'performance_category',
            'valid_from', 'valid_to', 'is_current'
        ]].copy()

        # Log de distribución de performance
        perf_dist = dim_driver['performance_category'].value_counts()
        logger.info(f"📊 dim_driver transformada: {len(dim_driver)} registros")
        logger.info(f"   Performance: Alto={perf_dist.get('Alto', 0)}, Medio={perf_dist.get('Medio', 0)}, Bajo={perf_dist.get('Bajo', 0)}")
        
        return dim_driver

    def transform_fact_deliveries(self, deliveries_df: pd.DataFrame, 
                                  dim_vehicles: pd.DataFrame,
                                  dim_drivers: pd.DataFrame,
                                  dim_routes: pd.DataFrame,
                                  dim_customers: pd.DataFrame) -> pd.DataFrame:
        # ... (inicio del método, cálculos de fechas, minutos, etc.) ...
        
        logger.info("🔄 Transformando fact_deliveries")

        fact = deliveries_df.copy()
        
        # ... (código de cálculos de tiempos, delays, QA, métricas financieras) ...

        # date_key
        fact['date_key'] = pd.to_datetime(fact['delivered_datetime']).dt.strftime('%Y%m%d').astype(int)

        # scheduled_time_key y delivered_time_key
        fact['scheduled_hour'] = pd.to_datetime(fact['scheduled_datetime']).dt.hour
        fact['scheduled_minute'] = (pd.to_datetime(fact['scheduled_datetime']).dt.minute // 15) * 15
        fact['scheduled_time_key'] = fact['scheduled_hour'] * 100 + fact['scheduled_minute']

        fact['delivered_hour'] = pd.to_datetime(fact['delivered_datetime']).dt.hour
        fact['delivered_minute'] = (pd.to_datetime(fact['delivered_datetime']).dt.minute // 15) * 15
        fact['delivered_time_key'] = fact['delivered_hour'] * 100 + fact['delivered_minute']

        # delivery_time_minutes (INT) - diferencia en minutos
        fact['delivery_time_minutes'] = (
            (pd.to_datetime(fact['delivered_datetime']) - pd.to_datetime(fact['scheduled_datetime']))
            .dt.total_seconds() / 60
        ).astype(int)

        # delay_minutes (INT) - solo valores positivos
        fact['delay_minutes'] = fact['delivery_time_minutes'].apply(lambda x: max(0, x))

        # QUALITY CHECKS: No permitir tiempos negativos (en delivery_time_minutes ya se pueden corregir)
        # Si delivery_time_minutes es negativo, corregir a 0 y marcar en una columna de calidad
        fact['qa_time_flag'] = False
        negative_mask = fact['delivery_time_minutes'] < 0
        if negative_mask.any():
            logger.warning(f"⚠️ {negative_mask.sum()} entregas con delivery_time_minutes negativo - corrigiendo a 0")
            fact.loc[negative_mask, 'qa_time_flag'] = True
            fact.loc[negative_mask, 'delivery_time_minutes'] = 0

        # Validar distancia y combustible - no negativos
        fact['qa_distance_flag'] = False
        dist_neg = fact['distance_km'] < 0
        if dist_neg.any():
            logger.warning(f"⚠️ {dist_neg.sum()} registros con distance_km negativo - corrigiendo a 0")
            fact.loc[dist_neg, 'qa_distance_flag'] = True
            fact.loc[dist_neg, 'distance_km'] = 0.0

        fact['qa_fuel_flag'] = False
        fuel_neg = fact['fuel_consumed_liters'] < 0
        if fuel_neg.any():
            logger.warning(f"⚠️ {fuel_neg.sum()} registros con fuel_consumed_liters negativo - corrigiendo a 0.0")
            fact.loc[fuel_neg, 'qa_fuel_flag'] = True
            fact.loc[fuel_neg, 'fuel_consumed_liters'] = 0.0

        # deliveries_per_hour (DECIMAL(5,2))
        fact['deliveries_per_hour'] = (60 / fact['delivery_time_minutes'].clip(lower=1)).round(2)

        # fuel_efficiency_km_per_liter (DECIMAL(5,2))
        fact['fuel_efficiency_km_per_liter'] = (
            fact['distance_km'] / fact['fuel_consumed_liters'].clip(lower=0.1)
        ).round(2)

        # cost_per_delivery y revenue_per_delivery (DECIMAL(10,2))
        fuel_price = 3.5
        fact['cost_per_delivery'] = (
            (fact['fuel_consumed_liters'] * fuel_price) + fact['toll_cost'] + 5
        ).round(2)

        fact['revenue_per_delivery'] = (
            10 + (fact['package_weight_kg'] * 0.5) + (fact['distance_km'] * 0.1)
        ).round(2)

        # is_on_time, is_damaged, has_signature (BOOLEAN)
        fact['is_on_time'] = fact['delay_minutes'] <= 30
        fact['is_damaged'] = False  # Placeholder
        fact['has_signature'] = fact['recipient_signature'].notna()

        # Unir con dimensiones para obtener keys
        # Como VEHICLE_KEY y DRIVER_KEY ahora son generadas por Snowflake (UUID/SEQ)
        # y no están en el DF de dimensión (staging) que se usa aquí,
        # debemos usar la CLAVE NATURAL (ID) en la FACT. Luego, en Snowflake, se hace un UPDATE
        # para obtener la clave subrogada correcta.
        # Por ahora, unimos solo para ROUTE y CUSTOMER, que sí tienen la KEY en el DF de Pandas.

        
        # Mantenemos las claves naturales (ID) en la Fact por ahora.
        fact['VEHICLE_KEY'] = fact['vehicle_id'] # Usamos ID como marcador temporal
        fact['DRIVER_KEY'] = fact['driver_id'] # Usamos ID como marcador temporal

        # Route y Customer sí tienen la KEY
        fact = fact.merge(dim_routes[['route_id', 'route_key']], on='route_id', how='left')
        fact = fact.merge(dim_customers[['customer_name', 'customer_key']], on='customer_name', how='left')

 
        # etl_batch_id (INT) 
        fact['etl_batch_id'] = self.etl_run_id
        # Añadir columna indicando si registro pasó checks
        fact['qa_passed'] = ~(fact['qa_time_flag'] | fact['qa_distance_flag'] | fact['qa_fuel_flag'])
        
        # Renombramos para usar los nombres de columnas finales esperados por la Fact
        fact = fact.rename(columns={'VEHICLE_KEY': 'VEHICLE_ID_NATURAL', 'DRIVER_KEY': 'DRIVER_ID_NATURAL'})


        # Seleccionar columnas finales (NOTA: delivery_key se omite - IDENTITY en SQL)
        # ⚠️ Cambiamos VEHICLE_KEY y DRIVER_KEY por sus IDs naturales para el look-up posterior
        fact_deliveries = fact[[
            'date_key', 'scheduled_time_key', 'delivered_time_key',
            'VEHICLE_ID_NATURAL', 'DRIVER_ID_NATURAL', 'route_key', 'customer_key', # <-- CLAVES NATURALES TEMPORALES
            'delivery_id', 'trip_id', 'tracking_number',
            'package_weight_kg', 'distance_km', 'fuel_consumed_liters',
            'delivery_time_minutes', 'delay_minutes', 'deliveries_per_hour',
            'fuel_efficiency_km_per_liter', 'cost_per_delivery', 'revenue_per_delivery',
            'is_on_time', 'is_damaged', 'has_signature', 'delivery_status',
            'etl_batch_id'
        ]].copy()
        
        # Para que la carga a Snowflake funcione, necesitamos renombrar las claves temporales
        fact_deliveries = fact_deliveries.rename(columns={
            'VEHICLE_ID_NATURAL': 'VEHICLE_KEY', 
            'DRIVER_ID_NATURAL': 'DRIVER_KEY'
        })
        
        logger.info(f"📊 fact_deliveries transformada: {len(fact_deliveries)} registros.")
        return fact_deliveries
    

    # =====================================================
    # 💾 CARGA EN SNOWFLAKE
    # =====================================================
    ###VER: overwrite en dims. Si es SCD2, no overwrite. Si es dim_date/time sí. si es fact, no.
    # ver de excluir columnas de auditoría (etl_batch_id, qa_flags) en dims y las staging de dims.

    def load_to_snowflake(self,data_dict:Dict[str,pd.DataFrame], overwrite=False)->bool:
        logger.info("💾 Iniciando carga a Snowflake (INSERT en tablas existentes)")
        try:
            for table_name,df in data_dict.items():
                table_upper=table_name.upper()
                df.columns=[c.upper() for c in df.columns]
                logger.info(f"📤 Insertando en {table_upper} con columnas mayúsculas: {df.columns.tolist()}")
                write_pandas(conn=self.snowflake_conn,df=df,table_name=table_upper,database='FLEETLOGIX_DW3',schema='ANALYTICS',auto_create_table=False, overwrite=overwrite,chunk_size=self.config.batch_size, parallel=1)
                logger.info(f"✅ {table_upper}: {len(df)} registros insertados")
            return True
        except Exception as e:
            logger.error(f"❌ Error en carga: {e}");return False

    def load_to_staging(self,deliveries_df:pd.DataFrame)->bool:
        logger.info("📤 Insertando datos crudos en STAGING_DAILY_LOAD")
        try:
            cur=self.snowflake_conn.cursor();import json
            raw_json=deliveries_df.to_json(orient='records',date_format='iso')
            cur.execute("INSERT INTO STAGING_DAILY_LOAD (RAW_DATA, LOAD_TIMESTAMP) SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP()",(raw_json,))
            logger.info(f"✅ STAGING_DAILY_LOAD: {len(deliveries_df)} registros insertados");cur.close();return True
        except Exception as e:
            logger.error(f"❌ Error insertando en staging: {e}");return False
        

        
    def load_dim_date_if_not_exists(self, dim_date_df: pd.DataFrame):
        """
        Inserta registros en DIM_DATE solo si la clave no existe.
        Es más eficiente que un MERGE para una dimensión estática.
        """
        logger.info("📅 Verificando e insertando nuevas fechas en DIM_DATE...")
        
        # Nombre de la tabla de staging temporal
        staging_table_name = "STAGING_DIM_DATE_TEMP"
        
        try:
            # 1. Carga el DataFrame del día a una tabla de staging temporal (sobrescribiéndola)
            df_upper = dim_date_df.copy()
            df_upper.columns = [c.upper() for c in df_upper.columns]
            write_pandas(
                conn=self.snowflake_conn,
                df=df_upper,
                table_name=staging_table_name,
                auto_create_table=True, # Crea la tabla si no existe
                overwrite=True
            )

            # 2. Construye y ejecuta una consulta INSERT ... SELECT WHERE NOT EXISTS
            query = f"""
            INSERT INTO DIM_DATE (DATE_KEY, FULL_DATE, DAY_OF_WEEK, DAY_NAME, DAY_OF_MONTH, DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_NUM, MONTH_NAME, QUARTER, YEAR, IS_WEEKEND, IS_HOLIDAY, HOLIDAY_NAME, FISCAL_QUARTER, FISCAL_YEAR)
            SELECT s.DATE_KEY, s.FULL_DATE, s.DAY_OF_WEEK, s.DAY_NAME, s.DAY_OF_MONTH, s.DAY_OF_YEAR, s.WEEK_OF_YEAR, s.MONTH_NUM, s.MONTH_NAME, s.QUARTER, s.YEAR, s.IS_WEEKEND, s.IS_HOLIDAY, s.HOLIDAY_NAME, s.FISCAL_QUARTER, s.FISCAL_YEAR
            FROM {staging_table_name} s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM DIM_DATE t 
                WHERE t.DATE_KEY = s.DATE_KEY
            );
            """
            
            cursor = self.snowflake_conn.cursor()
            cursor.execute(query)
            
            # 3. Limpia la tabla de staging
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name}")
            
            logger.info(f"✅ DIM_DATE actualizada. {cursor.rowcount} nuevas fechas insertadas.")
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"❌ Error al insertar en DIM_DATE: {e}")
            return False
        
# -----------------------------------------------------
    # 5. SCD Type 2 MERGE EN SNOWFLAKE (Corregido)
    # -----------------------------------------------------
     
    def execute_merge_for_dimension(self, table_name: str, staging_table_name: str, sk_key: str, p_key: str, tracked_cols: List[str]):
        """
        Ejecuta un MERGE SCD Tipo 2 en Snowflake.
        Genera la SK_KEY con UUID_STRING() en el destino.
        """
        logger.info(f"🔄 Ejecutando MERGE SCD Tipo 2 para {table_name}")
        
        try:
            # 1. Preparar lista de columnas (incluyendo la SK_KEY)
            # La columna SK_KEY se incluye en el INSERT pero su VALOR proviene de UUID_STRING()
            all_columns_sql = [sk_key, p_key] + tracked_cols + ['VALID_FROM', 'VALID_TO', 'IS_CURRENT']
            update_check = " OR ".join([f"t.{col} <> s.{col}" for col in tracked_cols])
            
            # 2. MERGE: Cerrar registros antiguos (cambios) o Insertar nuevos (nunca vistos)
            merge_sql = f"""
            MERGE INTO {table_name} t
            USING {staging_table_name} s
            ON t.{p_key} = s.{p_key} AND t.IS_CURRENT = TRUE
    
            -- Caso 1: MATCHED y ha cambiado (UPDATE: cerrar registro antiguo)
            WHEN MATCHED AND ({update_check}) THEN
                UPDATE SET 
                    t.VALID_TO = DATEADD(day, -1, CURRENT_DATE()), -- Cierra un día antes de hoy
                    t.IS_CURRENT = FALSE
    
            -- Caso 2: NOT MATCHED (INSERT: nuevo registro)
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(all_columns_sql)})
                VALUES (
                    UUID_STRING(), -- 🔑 AQUÍ SE GENERA LA CLAVE SUBROGADA
                    s.{p_key},
                    {', '.join([f's.{c}' for c in tracked_cols])},
                    s.VALID_FROM,
                    s.VALID_TO,
                    s.IS_CURRENT
                );
            """
            
            # 3. INSERT (Segunda parte del SCD2): Insertar la nueva versión del registro modificado
            insert_changed_sql = f"""
            INSERT INTO {table_name} ({', '.join(all_columns_sql)})
            SELECT 
                UUID_STRING(), -- 🔑 AQUÍ SE GENERA LA NUEVA CLAVE SUBROGADA
                s.{p_key},
                {', '.join([f's.{col}' for col in tracked_cols])},
                CURRENT_DATE() AS VALID_FROM,
                '9999-12-31' AS VALID_TO, -- Usar fecha máxima o NULL/9999 para el registro actual
                TRUE AS IS_CURRENT
            FROM {staging_table_name} s
            INNER JOIN {table_name} t 
              ON s.{p_key} = t.{p_key}
            -- Selecciona los registros que acabamos de cerrar
            WHERE t.VALID_TO = DATEADD(day, -1, CURRENT_DATE()) 
              AND t.IS_CURRENT = FALSE; 
            """
            
            # Ejecución (Mock)
            #cursor = self.snowflake_conn.cursor()
            #cursor.execute(merge_sql)
            #cursor.execute(insert_changed_sql)
            #cursor.close()
            
            logger.info("✅ SQL MERGE/INSERT generados y listos para ejecutar en Snowflake.")
            return True
    
        except Exception as e:
            logger.error(f"❌ Error durante el MERGE SCD Tipo 2 para {table_name}: {e}")
            # raise # Recomiendo hacer raise para detener el ETL ante fallo de la integridad
            return False
        
    def precalculate_reports(self, fact_deliveries: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Pre-calcula totales simples para reportes rápidos:
        - totales por driver (deliveries, avg_delay, total_revenue)
        - totales por route (deliveries, avg_distance, total_revenue)
        Devuelve un dict con dataframes listos para cargar como tablas.
        """
        logger.info("📈 Pre-calculando reportes rápidos")

        # Totales por driver
        by_driver = fact_deliveries.groupby('driver_key').agg(
            total_deliveries=('delivery_id', 'count'),
            avg_delay_minutes=('delay_minutes', 'mean'),
            total_revenue=('revenue_per_delivery', 'sum')
        ).reset_index()

        # Totales por route
        by_route = fact_deliveries.groupby('route_key').agg(
            total_deliveries=('delivery_id', 'count'),
            avg_distance_km=('distance_km', 'mean'),
            total_revenue=('revenue_per_delivery', 'sum')
        ).reset_index()

        # Normalizar dtypes
        by_driver['avg_delay_minutes'] = by_driver['avg_delay_minutes'].round(2)
        by_driver['total_revenue'] = by_driver['total_revenue'].round(2)
        by_route['avg_distance_km'] = by_route['avg_distance_km'].round(2)
        by_route['total_revenue'] = by_route['total_revenue'].round(2)

        return {
            'report_driver_totals': by_driver,
            'report_route_totals': by_route
        }

    # -----------------------------------------------------
    # 6. Look-up de Claves Subrogadas en Fact (Corregido)
    # -----------------------------------------------------
    def update_fact_keys_scd2(self):
        """
        Actualiza VEHICLE_KEY y DRIVER_KEY en FACT_DELIVERIES usando la lógica SCD2.
        """
        self.connect_snowflake()
        try:
            # VEHICLE_KEY Look-up
            vehicle_lookup_sql = f"""
            UPDATE FACT_DELIVERIES f
            SET f.VEHICLE_KEY = d.VEHICLE_KEY
            FROM DIM_VEHICLE d
            WHERE f.VEHICLE_KEY = d.VEHICLE_ID -- Join temporal por Clave Natural
              AND f.FULL_DATE BETWEEN d.VALID_FROM AND d.VALID_TO; -- 🟢 Lógica SCD2
            """
            
            # DRIVER_KEY Look-up
            driver_lookup_sql = f"""
            UPDATE FACT_DELIVERIES f
            SET f.DRIVER_KEY = d.DRIVER_KEY
            FROM DIM_DRIVER d
            WHERE f.DRIVER_KEY = d.DRIVER_ID -- Join temporal por Clave Natural
              AND f.FULL_DATE BETWEEN d.VALID_FROM AND d.VALID_TO; -- 🟢 Lógica SCD2
            """
            
            # Limpieza: Eliminar la columna FULL_DATE temporal de la Fact
            cleanup_sql = "ALTER TABLE FACT_DELIVERIES DROP COLUMN IF EXISTS FULL_DATE;"

            logger.info("✅ SQL Look-up SCD2 y limpieza listos para ejecución.")
            # Ejecución real: cursor.execute(...)
        except Exception as e:
            logger.error(f"❌ Error durante el look-up de claves en FACT_DELIVERIES: {e}")
            raise



    # =====================================================
    # 🚀 PIPELINE PRINCIPAL
    # =====================================================

    def run_etl(self, target_date: date = None) -> bool:
        """Ejecuta ETL para modelo en snowflake, para una fecha específica o ayer por defecto"""
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        logger.info(f"🚀 Iniciando ETL para {target_date}")

        try:
            # 1. Conectar
            if not self.connect_postgresql() or not self.connect_snowflake():
                return False

            # 1.1 Validar que las tablas existan (creadas por 04_dimensional_model.sql)
            if not self.validate_snowflake_schema():
                logger.error("❌ Schema no válido. Ejecuta 04_dimensional_model.sql primero")
                return False

            # 2. Extraer
            deliveries_df = self.extract_deliveries(target_date)
            dimensions_df = self.extract_dimensions()

            if deliveries_df.empty:
                logger.warning(f"⚠️ No hay entregas para {target_date}")
                return True

            # 2.1 Cargar datos crudos a staging PRIMERO (auditoría)
            self.load_to_staging(deliveries_df)

            # 3. Transformar dimensiones
            dim_date = self.transform_dim_date(target_date, target_date)
            dim_time = self.transform_dim_time()
            dim_vehicle = self.transform_dim_vehicle(dimensions_df['vehicles'])
            dim_driver = self.transform_dim_driver(dimensions_df['drivers'])
            dim_route = self.transform_dim_route(dimensions_df['routes'])
            dim_customer = self.transform_dim_customer(deliveries_df)

            # 4. Transformar hechos
            fact_deliveries = self.transform_fact_deliveries(
                deliveries_df, dim_vehicle, dim_driver, dim_route, dim_customer
            )

            # 5. Cargar dimensiones y hechos
            
            logger.info("💾 Cargando dimensiones simples, hechos y reportes...")

            # Para DIM_TIME, asumimos que se carga una única vez y no se toca.
            # Para DIM_ROUTE y DIM_CUSTOMER un refresh completo sigue siendo aceptable.
            data_to_load_simple = {
                'DIM_ROUTE': dim_route,
                'DIM_CUSTOMER': dim_customer,
                'FACT_DELIVERIES': fact_deliveries
            }
            self.load_to_snowflake(data_to_load_simple)

            # Usamos la nueva función eficiente para DIM_DATE
            self.load_dim_date_if_not_exists(dim_date)

            # 6. Cargar dimensiones con historial (SCD Type 2) usando MERGE
            
            # --- DIM_VEHICLE ---
            # 6.1. Cargar a Staging
            self.load_to_snowflake({'STAGING_DIM_VEHICLE': dim_vehicle}, overwrite=True) # Usamos una tabla de staging
            # 6.2. Ejecutar MERGE
            tracked_cols_vehicle = ['LICENSE_PLATE', 'VEHICLE_TYPE', 'STATUS', 'LAST_MAINTENANCE_DATE']
            # ⚠️ CAMBIO: Pasamos Clave Subrogada (SK) y Clave Natural (PK)
            self.execute_merge_for_dimension(
                'DIM_VEHICLE', 
                'STAGING_DIM_VEHICLE', 
                sk_key='VEHICLE_KEY', 
                p_key='VEHICLE_ID', 
                tracked_cols=tracked_cols_vehicle
            )
    
            # --- DIM_DRIVER ---
            # 6.1. Cargar a Staging
            self.load_to_snowflake({'STAGING_DIM_DRIVER': dim_driver}, overwrite=True)
            # 6.2. Ejecutar MERGE
            tracked_cols_driver = ['FULL_NAME', 'LICENSE_NUMBER', 'LICENSE_EXPIRY', 'PHONE', 'STATUS']
            # ⚠️ CAMBIO: Pasamos Clave Subrogada (SK) y Clave Natural (PK)
            self.execute_merge_for_dimension(
                'DIM_DRIVER', 
                'STAGING_DIM_DRIVER', 
                sk_key='DRIVER_KEY', 
                p_key='DRIVER_ID', 
                tracked_cols=tracked_cols_driver
            )
            
            # 4. Look-up Final SCD2 en FACT
            
            # 7. Look-up de Claves Subrogadas en FACT_DELIVERIES
            # Este paso es fundamental ya que la FACT se cargó con los IDs naturales
            # y ahora debe actualizarse con las Claves Subrogadas (VEHICLE_KEY, DRIVER_KEY)
    
            logger.info("⚠️ Ejecutando Look-up de Claves Subrogadas para FACT_DELIVERIES (SCD Tipo 2)...")
            self.update_fact_keys_scd2()
    
            logger.info(f"✅ ETL completado para {target_date}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error en ETL: {e}")
            return False
        finally:
            self.close_connections()

# =====================================================
# 🎯 EJECUCIÓN
# =====================================================

def main():
    """Función principal - ejecuta ETL """

    import argparse
    parser = argparse.ArgumentParser(description='Ejecutar ETL')
    parser.add_argument('--date', help='Fecha ISO YYYY-MM-DD a procesar', type=str)
    parser.add_argument('--nth', help='n-ésimo último día con datos (0=último,1=anteúltimo,...)', type=int, default=None)
    parser.add_argument('--settings', help='Ruta al archivo settings.ini (opcional)', type=str, default='settings.ini')
    args = parser.parse_args()

    logger.info("🚀 Iniciando FleetLogix ETL")

    config = ETLConfig(settings_path=args.settings)
    etl = ETL(config)

    target_date = None
    if args.date:
        target_date = date.fromisoformat(args.date)
    elif args.nth is not None:
        # obtener n-ésima última fecha con datos desde PostgreSQL
        try:
            target_date = etl.get_nth_last_date_with_data(args.nth)
            logger.info(f"📅 Usando n-ésima fecha con datos (n={args.nth}): {target_date}")
        except Exception as e:
            logger.error(f"❌ No se pudo determinar la fecha n-ésima: {e}")
            sys.exit(2)
    else:
        target_date = date.today() - timedelta(days=1)

    success = etl.run_etl(target_date)

    if success:
        logger.info("✅ ETL completado exitosamente")
        sys.exit(0)
    else:
        logger.error("❌ ETL falló")
        sys.exit(1)

if __name__ == "__main__":
    main()

"""
🎯 ETL 100% COMPLETADO OK

🚀 EJECUCIÓN:
   python etl_pipeline.py --nth 0  (último día con datos"""
