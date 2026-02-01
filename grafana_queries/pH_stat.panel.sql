-- Descrição: Query otimizada para exibir valor atual do pH em painel Stat
-- Dashboard: Stat Panel no Grafana Cloud (formato single value)
-- Função: Monitoramento em tempo real do pH mais recente por equipamento
-- Range Crítico: pH 4.0-9.0 (fora dessa faixa = alarme no sistema)
-- Data Source: Amazon Athena via plugin grafana-athena-datasource

-- CTE para identificar o registro mais recente de cada equipamento
WITH latest_data AS (
    SELECT
        -- VALOR PRINCIPAL: pH CONVERTIDO PARA DOUBLE
        -- Conversão explícita garante precisão decimal (ex: 7.25)
        -- Tipo DOUBLE suporta até 15 dígitos de precisão
        CAST(pH AS DOUBLE) as value,
        
        -- ID do equipamento para agrupamento
        equipment,
        
        -- Timestamp original (string) usado para ordenação temporal
        -- Mantido como string para compatibilidade com particionamento
        timestamp,

        -- ROW_NUMBER() garante apenas 1 registro por equipamento
        -- PARTITION BY equipment: um ranking independente por biorreator
        -- ORDER BY timestamp DESC: mais recente primeiro (rn=1)
        ROW_NUMBER() OVER (
            PARTITION BY equipment 
            ORDER BY timestamp DESC
        ) as rn
        
    FROM sensor_data
    WHERE
        -- Limita scan apenas às partições necessárias
        -- Evita scan completo da tabela (TB → MB)
        
        -- Ano atual (partição year)
        year = CAST(year(current_timestamp) AS VARCHAR)
        
        -- Mês atual com zero-padding (partição month)
        AND month = lpad(CAST(month(current_timestamp) AS VARCHAR), 2, '0')
        
        -- Últimos 2 dias (partição day)
        -- Cobre timezone differences e delays de processamento
        AND day IN (
            lpad(CAST(day(current_timestamp) AS VARCHAR), 2, '0'),                    -- Hoje
            lpad(CAST(day(current_timestamp - interval '1' day) AS VARCHAR), 2, '0')  -- Ontem
        )
        
        -- FILTROS DE QUALIDADE DE DADOS
        -- Multi-tenancy: usuário vê apenas seus equipamentos
        -- Variável Grafana: ${equipment_filter:csv} → '25080001,25080002'
        AND equipment IN ('${equipment_filter:csv}')
        
        -- Elimina registros com pH nulo/inválido
        -- Crítico para Stat panels (não podem exibir NULL)
        AND ph IS NOT NULL
)

-- QUERY PRINCIPAL: RETORNA APENAS O VALOR MAIS RECENTE
SELECT
    -- Valor único para Stat Panel
    -- Grafana renderiza como gauge/single stat automaticamente
    value
FROM latest_data
WHERE 
    -- Filtro final: apenas o registro mais recente (rn=1)
    -- Se múltiplos equipamentos, retorna o mais recente de cada um
    rn = 1