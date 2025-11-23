-- =====================================================
-- FLEETLOGIX - 12 QUERIES SQL
-- Organizadas por nivel de complejidad
-- =====================================================

-- =====================================================
-- QUERIES BÁSICAS (3 queries)
-- =====================================================

-- Query 1: Contar vehículos por tipo
-- Problema de negocio: Conocer la composición de la flota
SELECT 
    vehicle_type,
    COUNT(*) as cantidad
FROM vehicles
GROUP BY vehicle_type
ORDER BY cantidad DESC;

--La composición de la flota es relativamente pareja con mayor predominancia de Camión Grande.

-- Query 2: Listar conductores con licencia próxima a vencer
-- Problema de negocio: Prevenir problemas legales por licencias vencidas
SELECT 
    first_name,
    last_name,
    license_number,
    license_expiry
FROM drivers
WHERE license_expiry < CURRENT_DATE + INTERVAL '30 days'
ORDER BY license_expiry;

-- 11 conductores tienen la licencia próxima a vencer. 

-- Query 3: Total de viajes por estado
-- Problema de negocio: Monitorear operaciones en curso
SELECT 
    status,
    COUNT(*) as total_viajes
FROM trips
GROUP BY status;

-- 10% de los viajes están en progreso. 85% fueron completados y 5% cancelados.

-- =====================================================
-- QUERIES INTERMEDIAS (5 queries)
-- =====================================================

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
-- Problema de negocio: Identificar demanda por ciudad para planificación de recursos
SELECT 
    r.destination_city,
    COUNT(DISTINCT t.trip_id) as total_viajes,
    COUNT(d.delivery_id) as total_entregas,
    SUM(d.package_weight_kg) as peso_total_kg
FROM routes r
INNER JOIN trips t ON r.route_id = t.route_id
INNER JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY r.destination_city
ORDER BY total_entregas DESC;

--La mayor proporción de demanda se encuentra en Mar del Plata y Mendoza. 

-- Query 5: Conductores activos con cantidad de viajes completados
-- Problema de negocio: Evaluar carga de trabajo que tuvo cada conductor y cancelación de viajes-
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name as nombre_completo,
    d.license_expiry,
    COUNT(t.trip_id) as viajes_totales,
    SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as viajes_completados 
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id
WHERE d.status = 'active'
GROUP BY d.driver_id, d.first_name, d.last_name, d.license_expiry
HAVING COUNT(t.trip_id) > 0
ORDER BY viajes_completados DESC;

-- Query 6: Promedio de entregas por conductor en los últimos 6 meses
-- Problema de negocio: Medir productividad individual de conductores
SELECT 
    dr.driver_id,
    dr.first_name || ' ' || dr.last_name as conductor,
    COUNT(DISTINCT t.trip_id) as total_viajes,
    COUNT(d.delivery_id) as total_entregas,
    ROUND(COUNT(d.delivery_id)::NUMERIC / NULLIF(COUNT(DISTINCT t.trip_id), 0), 2) as promedio_entregas_por_viaje,
    ROUND(COUNT(d.delivery_id)::NUMERIC / 180, 2) as promedio_entregas_diarias
FROM drivers dr
INNER JOIN trips t ON dr.driver_id = t.driver_id
INNER JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '6 months'
    AND t.status = 'completed'
GROUP BY dr.driver_id, dr.first_name, dr.last_name
HAVING COUNT(DISTINCT t.trip_id) >= 10
ORDER BY promedio_entregas_por_viaje DESC;

-- Query 7: Rutas con mayor consumo de combustible por kilómetro
-- Problema de negocio: Identificar rutas ineficientes para optimización

SELECT 
    r.route_code,
    r.origin_city || ' -> ' || r.destination_city as ruta,
    r.distance_km,
    COUNT(t.trip_id) as viajes_realizados,
    AVG(t.fuel_consumed_liters) as promedio_combustible_litros,
    ROUND(AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100, 2) as litros_por_100km,
    SUM(t.fuel_consumed_liters) as combustible_total
FROM routes r
INNER JOIN trips t ON r.route_id = t.route_id
WHERE t.fuel_consumed_liters IS NOT NULL 
    AND r.distance_km > 0
    AND t.status = 'completed'
GROUP BY r.route_id, r.route_code, r.origin_city, r.destination_city, r.distance_km
HAVING COUNT(t.trip_id) >= 50
ORDER BY litros_por_100km desc
limit 10;

-- Porporción de consumo pareja

-- Query 8: Análisis de entregas retrasadas por día de la semana
-- Problema de negocio: Identificar patrones de retraso para mejorar planificación
SELECT 
    TO_CHAR(d.scheduled_datetime, 'Day') as dia_semana,
    EXTRACT(DOW FROM d.scheduled_datetime) as num_dia,
    COUNT(*) as total_entregas,
    COUNT(CASE 
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 
    END) as entregas_retrasadas,
    ROUND(100.0 * COUNT(CASE 
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 
    END) / COUNT(*), 2) as porcentaje_retrasos,
    AVG(EXTRACT(EPOCH FROM (d.delivered_datetime - d.scheduled_datetime)) / 60) as minutos_promedio_diferencia
FROM deliveries d
WHERE d.delivery_status = 'delivered'
    AND d.scheduled_datetime >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY dia_semana, num_dia
ORDER BY num_dia;

-- Retraso parejo, pero algo mayor los Martes. 

-- =====================================================
-- QUERIES COMPLEJAS (4 queries)
-- =====================================================

-- Query 9: Costo de mantenimiento por kilómetro recorrido
-- Problema de negocio: Evaluar costo-beneficio de cada tipo de vehículo
WITH vehicle_metrics AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_type,
        v.license_plate,
        COUNT(DISTINCT t.trip_id) as total_viajes,
        SUM(r.distance_km) as km_totales,
        SUM(m.cost) as costo_mantenimiento_total,
        COUNT(DISTINCT m.maintenance_id) as cantidad_mantenimientos
    FROM vehicles v
    LEFT JOIN trips t ON v.vehicle_id = t.vehicle_id
    LEFT JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
    WHERE t.status = 'completed'
    GROUP BY v.vehicle_id, v.vehicle_type, v.license_plate
)
SELECT 
    vehicle_type,
    COUNT(vehicle_id) as cantidad_vehiculos,
    SUM(total_viajes) as viajes_totales,
    SUM(km_totales) as kilometros_totales,
    SUM(costo_mantenimiento_total) as costo_total_mantenimiento,
    ROUND(SUM(costo_mantenimiento_total) / NULLIF(SUM(km_totales), 0), 2) as costo_por_km,
    ROUND(AVG(costo_mantenimiento_total / NULLIF(cantidad_mantenimientos, 0)), 2) as costo_promedio_por_mantenimiento
FROM vehicle_metrics
WHERE km_totales > 0 AND costo_mantenimiento_total > 0
GROUP BY vehicle_type
ORDER BY costo_por_km DESC;

--Los costos de mantenimiento son parejos, pero algo mayor en las Van.

-- Query 10: Ranking de conductores por eficiencia usando Window Functions
-- Problema de negocio: Identificar top performers para incentivos
WITH conductor_metricas AS (
    SELECT 
        d.driver_id,
        d.first_name || ' ' || d.last_name as nombre,
        COUNT(DISTINCT t.trip_id) as viajes,
        COUNT(DISTINCT del.delivery_id) as entregas,
        AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100 as consumo_100km,
        COUNT(CASE WHEN del.delivered_datetime <= del.scheduled_datetime THEN 1 END)::NUMERIC / 
            NULLIF(COUNT(del.delivery_id), 0) * 100 as puntualidad_pct
    FROM drivers d
    JOIN trips t ON d.driver_id = t.driver_id
    JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN deliveries del ON t.trip_id = del.trip_id
    WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY d.driver_id, d.first_name, d.last_name
    HAVING COUNT(DISTINCT t.trip_id) >= 20
)
SELECT 
    nombre,
    viajes,
    entregas,
    ROUND(consumo_100km, 2) as consumo_100km,
    ROUND(puntualidad_pct, 2) as puntualidad_pct,
    RANK() OVER (ORDER BY puntualidad_pct DESC) as rank_puntualidad,
    RANK() OVER (ORDER BY consumo_100km ASC) as rank_eficiencia,
    RANK() OVER (ORDER BY entregas DESC) as rank_productividad,
    (RANK() OVER (ORDER BY puntualidad_pct DESC) + 
     RANK() OVER (ORDER BY consumo_100km ASC) + 
     RANK() OVER (ORDER BY entregas DESC)) / 3.0 as score_promedio
FROM conductor_metricas
ORDER BY score_promedio ASC
LIMIT 20;

--El score promedio más bajo es de 34, estando el siguiente 11 puntos arriba y el tercero 5 puntos más. 



-- Query 11: Análisis de tendencia de viajes con LAG y LEAD
-- Problema de negocio: Proyectar necesidades futuras basadas en tendencias
WITH viajes_mensuales AS (
    SELECT 
        DATE_TRUNC('month', departure_datetime) as mes,
        COUNT(*) as total_viajes,
        SUM(total_weight_kg) as peso_total,
        AVG(fuel_consumed_liters) as combustible_promedio
    FROM trips
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('month', departure_datetime)
)
SELECT 
    TO_CHAR(mes, 'YYYY-MM') as periodo,
    total_viajes,
    LAG(total_viajes, 1) OVER (ORDER BY mes) as viajes_mes_anterior,
    LEAD(total_viajes, 1) OVER (ORDER BY mes) as viajes_mes_siguiente,
    total_viajes - LAG(total_viajes, 1) OVER (ORDER BY mes) as cambio_absoluto,
    ROUND((total_viajes - LAG(total_viajes, 1) OVER (ORDER BY mes))::NUMERIC / 
          NULLIF(LAG(total_viajes, 1) OVER (ORDER BY mes), 0) * 100, 2) as cambio_porcentual,
    ROUND(peso_total / 1000, 2) as toneladas_transportadas,
    ROUND(combustible_promedio, 2) as combustible_promedio_viaje,
    AVG(total_viajes) OVER (ORDER BY mes ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as promedio_movil_3m
FROM viajes_mensuales
ORDER BY mes DESC
LIMIT 12;

--La evolución temporal ha sido estable, aunque hubo algo mayor demanda en enero de 2025, considerando el promedio móvil de 3 meses.

-- Query 12: Pivot de entregas por hora y día de la semana
-- Problema de negocio: Optimizar horarios de operación y personal
WITH entregas_por_hora_dia AS (
    SELECT 
        EXTRACT(DOW FROM scheduled_datetime) as dia_semana,
        EXTRACT(HOUR FROM scheduled_datetime) as hora,
        COUNT(*) as cantidad_entregas
    FROM deliveries
    WHERE scheduled_datetime >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY EXTRACT(DOW FROM scheduled_datetime), EXTRACT(HOUR FROM scheduled_datetime)
)
SELECT 
    hora,
    SUM(CASE WHEN dia_semana = 0 THEN cantidad_entregas ELSE 0 END) as domingo,
    SUM(CASE WHEN dia_semana = 1 THEN cantidad_entregas ELSE 0 END) as lunes,
    SUM(CASE WHEN dia_semana = 2 THEN cantidad_entregas ELSE 0 END) as martes,
    SUM(CASE WHEN dia_semana = 3 THEN cantidad_entregas ELSE 0 END) as miercoles,
    SUM(CASE WHEN dia_semana = 4 THEN cantidad_entregas ELSE 0 END) as jueves,
    SUM(CASE WHEN dia_semana = 5 THEN cantidad_entregas ELSE 0 END) as viernes,
    SUM(CASE WHEN dia_semana = 6 THEN cantidad_entregas ELSE 0 END) as sabado,
    SUM(cantidad_entregas) as total_semana
FROM entregas_por_hora_dia
WHERE hora BETWEEN 6 AND 22
GROUP BY hora
ORDER BY hora;

-- El momento de menos entrega es a las 6 am (124) del domingo y el de mayor entregas los lunes a las 12 (301). En el total de la semana las mayores entregas se dan a las 11 y 12 hs. 