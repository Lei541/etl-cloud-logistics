-- =====================================================
-- FLEETLOGIX - ÍNDICES DE OPTIMIZACIÓN
-- Basados en las 12 queries analizadas
-- Objetivo: Mejorar performance en 20%+
-- =====================================================

-- Análisis de performance ANTES de crear índices
-- Ejecutar cada query con EXPLAIN ANALYZE y guardar tiempos

-- =====================================================
-- ÍNDICE 1: Optimización para JOINs frecuentes en trips
-- =====================================================
-- Justificación: Las queries 4-12 hacen JOIN intensivo entre trips y otras tablas
-- Queries beneficiadas: 4, 5, 6, 7, 9, 10, 11
CREATE INDEX idx_trips_composite_joins ON trips(vehicle_id, driver_id, route_id, departure_datetime)
WHERE status = 'completed';

-- =====================================================
-- ÍNDICE 2: Optimización para análisis temporal de deliveries
-- =====================================================
-- Justificación: Queries 8, 12 filtran y agrupan por scheduled_datetime
-- Queries beneficiadas: 4, 8, 12
CREATE INDEX idx_deliveries_scheduled_datetime ON deliveries(scheduled_datetime, delivery_status)
WHERE delivery_status = 'delivered';

-- =====================================================
-- ÍNDICE 3: Optimización para mantenimiento por vehículo
-- =====================================================
-- Justificación: Query 9 necesita acceso rápido a mantenimientos por vehículo
-- Queries beneficiadas: 9
CREATE INDEX idx_maintenance_vehicle_cost ON maintenance(vehicle_id, cost);

-- =====================================================
-- ÍNDICE 4: Optimización para análisis de conductores
-- =====================================================
-- Justificación: Queries 5, 6, 10 filtran por conductores activos
-- Queries beneficiadas: 2, 5, 6, 10
CREATE INDEX idx_drivers_status_license ON drivers(status, license_expiry)
WHERE status = 'active';

-- =====================================================
-- ÍNDICE 5: Optimización para métricas de rutas
-- =====================================================
-- Justificación: Query 7 calcula consumo por ruta
-- Queries beneficiadas: 4, 7, 9, 10
CREATE INDEX idx_routes_metrics ON routes(route_id, distance_km, destination_city);

-- =====================================================
-- COMANDOS PARA VERIFICAR ÍNDICES CREADOS
-- =====================================================
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
    AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- =====================================================
-- MANTENIMIENTO DE ÍNDICES
-- =====================================================
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;