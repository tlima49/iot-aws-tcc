-- Descrição: Query otimizada para visualização em tempo real dos sensores
-- Dashboard: Time Series Panel no Grafana Cloud
-- Data Source: Amazon Athena via plugin grafana-athena-datasource
-- Tabela: biorreator_db.sensor_data (formato Parquet, particionada)

-- CTE (Common Table Expression) para processamento inicial dos dados
WITH parsed_data AS (
    SELECT
        -- Conversão do timestamp string para TIMESTAMP nativo do Athena
        -- Necessário para funções temporais e ordenação correta
        CAST(timestamp AS TIMESTAMP) as time,
        
        -- ID do equipamento (usado para multi-tenancy e filtros)
        equipment,
        
        -- Conversão explícita dos sensores para tipos corretos
        -- Garante consistência e evita erros de tipo em cálculos
        CAST(ph AS DOUBLE) as pH,           		-- pH
        CAST(rpm AS INTEGER) as RPM,       	-- Agitacao
        CAST(tcd AS DOUBLE) as TCD,         	-- Total Cell Density (densidade celular)
        CAST(temperatura AS DOUBLE) as Temperatura  -- Temperatura
    FROM sensor_data
    WHERE
        -- OTIMIZAÇÃO DE PARTIÇÕES 
        -- Usa partições year/month/day para scan eficiente no S3
        -- Partition Projection resolve paths automaticamente
        
        -- Ano atual (formato string para compatibilidade com partição)
        year = CAST(year(current_timestamp) AS VARCHAR)
        
        -- Mês atual com zero-padding (ex: '08' não '8')
        AND month = lpad(CAST(month(current_timestamp) AS VARCHAR), 2, '0')
        
        -- Últimos 5 dias para cobrir timezone e delays de processamento
        -- Necessário pois dados podem ter delay de até 30min (Firehose buffer)
        AND day IN (
            lpad(CAST(day(current_timestamp) AS VARCHAR), 2, '0'),
            lpad(CAST(day(current_timestamp - interval '1' day) AS VARCHAR), 2, '0'),
            lpad(CAST(day(current_timestamp - interval '2' day) AS VARCHAR), 2, '0'),
            lpad(CAST(day(current_timestamp - interval '3' day) AS VARCHAR), 2, '0'),
            lpad(CAST(day(current_timestamp - interval '4' day) AS VARCHAR), 2, '0'),
            lpad(CAST(day(current_timestamp - interval '5' day) AS VARCHAR), 2, '0')        )
        
        -- FILTRO MULTI-TENANT POR EQUIPAMENTO
        -- Variável Grafana substituída automaticamente pelo plugin
        -- Formato: '25080001,25080002' → ('25080001','25080002')
        AND equipment IN ('${equipment_filter:csv}')
)

-- QUERY PRINCIPAL - DADOS PARA TIME SERIES
SELECT
    time,           -- Eixo X: timestamp para série temporal
    equipment,      -- Series grouping: uma linha por equipamento
    pH,             -- Série 1: pH do biorreator
    RPM,            -- Série 2: Velocidade do agitador
    TCD,            -- Série 3: Densidade celular
    Temperatura     -- Série 4: Temperatura do processo
FROM parsed_data
WHERE
    -- Remove registros com timestamp inválido/nulo
    time IS NOT NULL
    
    -- Últimas 120 horas de dados (independente de partições)
    -- Cobre delays de processamento e permite visualização histórica
    AND time >= current_timestamp - interval '120' hour
