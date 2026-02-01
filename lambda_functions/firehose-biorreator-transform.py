import json
import base64
import boto3
from datetime import datetime
import logging

# Configuração do sistema de logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
"""
Transformar dados do Kinesis Firehose
Recebe dados dos biorreatores pelo MQTT e transforma para armazenamento no S3
Args: event: Evento do Kinesis Firehose contendo os registros para processamento
Returns: Dict: Resposta com registros processados para o Firehose
"""
#Lista para armazenar registros processados
output = []

#Processa cada registro enviado pelo Firehose
for record in event['records']:
try:
# Decodificação dos dados que chegam em base64 para utf-8
payload = base64.b64decode(record['data']).decode('utf-8')
logger.info(f"Raw payload: {payload}")

# Converte a string decodificada em um dicionário python
data = json.loads(payload)

# Extrai a ID do equipamento com a função extract_equipment_id
equipment_id = extract_equipment_id(data, record)

# Transforma os dados dos sensores incluindo chaves do particionamento dinâmico
transformed_data = transform_sensor_data_with_partitioning(data, equipment_id)

# Cria um dicionário para o registro de saída, que será retornado ao Firehose.
# Mantém a ID, Define resultado como Ok, adiciona uma quebra de linha e converte novamente para string de base64
output_record = {
'recordId': record['recordId'],
'result': 'Ok',
'data': base64.b64encode(
 (json.dumps(transformed_data) + '\n').encode('utf-8')
).decode('utf-8')
}

# Adiciona que o registro foi processo com sucesso a lista de saída e registra no log
output.append(output_record)
logger.info(f"Processed record for equipment: {equipment_id} at {transformed_data.get('timestamp')}")

#Em caso de erro. Registra uma mensagem detalhando qual registro falhou e o motivo.
except Exception as e:
logger.error(f"Error processing record {record['recordId']}: {str(e)}")
output.append({
'recordId': record['recordId'],
'result': 'ProcessingFailed'
})

# Registra uma mensagem informando quantos registros foram processados com sucesso e falhas
logger.info(f"Successfully processed {len(output)} records")
return {'records': output}

# Função para extrair o ID do equipamento de diferentes possíveis locais dentro dos dados
def extract_equipment_id(data, record):
if 'equipment' in data:
return str(data['equipment'])
elif 'topic' in data:
topic_parts = data['topic'].split('/')
if len(topic_parts) >= 2:
return topic_parts[1]
elif 'deviceId' in data:
return str(data['deviceId'])
else:
logger.warning("No equipment ID found, using UNKNOWN")
return "UNKNOWN"

# Função que transforma os dados do sensor e adiciona campos de particionamento.
def transform_sensor_data_with_partitioning(data, equipment_id):
try:
sensor_data = data.get('d', {})
timestamp = data.get('ts', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

# Tenta extrair os componentes de data e hora do timestamp fornecido no payload
try:
dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
year = dt.strftime('%Y')
month = dt.strftime('%m')
day = dt.strftime('%d')
hour = dt.strftime('%H')
logger.info(f"Extracted partitioning: {year}/{month}/{day}/{hour} from timestamp: {timestamp}")
# Se a conversão do timestamp falhar registra o erro e extrai data e hora atuais.
except Exception as parse_error:
logger.error(f"Error parsing timestamp {timestamp}: {parse_error}")
dt = datetime.utcnow()
year = dt.strftime('%Y')
month = dt.strftime('%m')
day = dt.strftime('%d')
hour = dt.strftime('%H')

# Dicionário para armazenar os dados transformados na mesma ordem do schema Glue
transformed = {}

# Tenta obter o valor do sensor. Se não encontrar, o padrão é uma lista com [None]
# Se o valor é uma lista, não está vazia e o não é nulo converte e armazena
# Se o valor não é nulo e não é uma lista converte e armazena
# Se nenhuma das condições armazena “None” para o campo do sensor
# 1. ph (position 1)
ph_value = sensor_data.get('pH', sensor_data.get('ph', [None]))
if isinstance(ph_value, list) and len(ph_value) > 0 and ph_value[0] is not None:
transformed['ph'] = float(ph_value[0])
elif ph_value is not None and not isinstance(ph_value, list):
transformed['ph'] = float(ph_value)
else:
transformed['ph'] = None

# 2. rpm (position 2)
rpm_value = sensor_data.get('rpm', [None])
if isinstance(rpm_value, list) and len(rpm_value) > 0 and rpm_value[0] is not None:
transformed['rpm'] = int(rpm_value[0])
elif rpm_value is not None and not isinstance(rpm_value, list):
transformed['rpm'] = int(rpm_value)
else:
transformed['rpm'] = None

# 3. tcd (position 3)
tcd_value = sensor_data.get('tcd', [None])
if isinstance(tcd_value, list) and len(tcd_value) > 0 and tcd_value[0] is not None:
transformed['tcd'] = float(tcd_value[0])
elif tcd_value is not None and not isinstance(tcd_value, list):
transformed['tcd'] = float(tcd_value)
else:
transformed['tcd'] = None

# 4. temperatura (position 4)
temp_value = sensor_data.get('temperatura', [None])
if isinstance(temp_value, list) and len(temp_value) > 0 and temp_value[0] is not None:
transformed['temperatura'] = float(temp_value[0])
elif temp_value is not None and not isinstance(temp_value, list):
transformed['temperatura'] = float(temp_value)
else:
transformed['temperatura'] = None

# Adiciona o timestamp original aos dados transfomados
# 5. timestamp (position 5)
transformed['timestamp'] = timestamp

# Adiciona o ID do equipamento aos dados transformados
# 6. equipment (position 6)
transformed['equipment'] = equipment_id

# Adiciona os campos de particionamento dinâmico ao registro final. O Firehose usa esses campos para criar a estrutura de pastas no S3
transformed['partition_year'] = year
transformed['partition_month'] = month
transformed['partition_day'] = day
transformed['partition_hour'] = hour

# Adiciona o dicionário final para depuração
logger.info(f"Transformed with partitioning: {transformed}")
return transformed

# Se ocorrer qualquer erro durante o bloco de transformação, registra o erro e retorna um dicionário com valores nulos para todos os campos do sensor
except Exception as e:
logger.error(f"Error transforming sensor data: {str(e)}")
dt = datetime.utcnow()
return {
'ph': None,
'rpm': None,
'tcd': None,
'temperatura': None,
'timestamp': data.get('ts', dt.strftime('%Y-%m-%d %H:%M:%S')),
'equipment': equipment_id,
'partition_year': dt.strftime('%Y'),
'partition_month': dt.strftime('%m'),
'partition_day': dt.strftime('%d'),
'partition_hour': dt.strftime('%H')
}
