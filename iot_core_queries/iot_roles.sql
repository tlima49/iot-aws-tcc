-- Instrução SQL para os dados recebidos:
SELECT *, topic(2) as equipment FROM ‘PRO/+/data’

-- Instrução SQL para os alarmes recebidos
SELECT *, topic(2) as equipment FROM ‘PRO/+/alarm’
