SELECT 
    actual.nameCountry AS Pais,
    actual.name AS Ciudad,
    -- Comparación de Personas
    pasada.numPeople AS personas_dic_2025,
    actual.numPeople AS personas_actual,
    (actual.numPeople - pasada.numPeople) AS crecimiento_personas,
    
    -- Comparación de Actividades
    pasada.totalActivities AS actividades_dic_2025,
    actual.totalActivities AS actividades_actual,
    (actual.totalActivities - pasada.totalActivities) AS nuevas_actividades

FROM destinos actual
JOIN destinos pasada ON actual.id = pasada.id
WHERE pasada.snapshot_date = '2025-12-31' 
  AND actual.snapshot_date = (SELECT MAX(snapshot_date) FROM destinos)
  AND actual.nameCountry IN ('Chile', 'Argentina', 'Brasil', 'Colombia', 'México', 'Perú')
ORDER BY actual.nameCountry ASC, crecimiento_personas DESC;

### por pais:

SELECT 
    actual.nameCountry AS Pais,
    -- Sumamos los valores de la fecha antigua
    SUM(pasada.numPeople) AS personas_dic_2025,
    -- Sumamos los valores de la fecha actual
    SUM(actual.numPeople) AS personas_actual,
    -- Calculamos la diferencia total por país
    (SUM(actual.numPeople) - SUM(pasada.numPeople)) AS crecimiento_total_personas,
    
    -- Sumamos las actividades
    SUM(pasada.totalActivities) AS actividades_dic_2025,
    SUM(actual.totalActivities) AS actividades_actual,
    (SUM(actual.totalActivities) - SUM(pasada.totalActivities)) AS nuevas_actividades_totales

FROM destinos actual
JOIN destinos pasada ON actual.id = pasada.id
WHERE pasada.snapshot_date = '2025-12-31' 
  AND actual.snapshot_date = (SELECT MAX(snapshot_date) FROM destinos)
  AND actual.nameCountry IN ('Chile', 'Argentina', 'Brasil', 'Colombia', 'México', 'Perú')
GROUP BY actual.nameCountry
ORDER BY crecimiento_total_personas DESC;