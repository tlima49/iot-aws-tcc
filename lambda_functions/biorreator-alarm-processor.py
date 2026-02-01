# ====================================================================
# AWS LAMBDA FUNCTION - PROCESSAMENTO DE ALARMES BIORREATORES
# ====================================================================
# Descrição: Processa alarmes dos biorreatores e envia notificações
# Trigger: AWS IoT Core Rule (tópico: PRO/+/alarm)
# Funcionalidades:
#   - Extração de dados do payload MQTT
#   - Envio de emails via Amazon SES
#   - Auditoria de alarmes no S3
#   - Tratamento de erros e logging
# ====================================================================

import json
import boto3
import os
from datetime import datetime

# CONFIGURAÇÃO DOS CLIENTES AWS
# Inicialização dos clientes AWS com credenciais da role da Lambda
s3 = boto3.client('s3')    # Para armazenamento de auditoria
ses = boto3.client('ses')  # Para envio de emails
sns = boto3.client('sns')  # Para notificações futuras (opcional)

# VARIÁVEIS DE AMBIENTE - CONFIGURAÇÃO EXTERNA
# Bucket S3 para armazenar logs de alarmes
S3_BUCKET = os.environ.get('S3_BUCKET', 'biorreator-data-tcc')

# Prefixo no S3 para organização dos alarmes
S3_PREFIX = os.environ.get('S3_PREFIX', 'alarms/')

# Email remetente verificado no Amazon SES
SES_FROM = os.environ.get('SES_FROM', 'biorreator.xxx@gmail.com')

# Tópico SNS para notificações futuras (opcional)
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')

def lambda_handler(event, context):
    """
    Handler principal da função Lambda
    
    Args:
        event: Payload do alarme vindo do IoT Core
        context: Contexto de execução da Lambda
    
    Returns:
        dict: Response com status da operação
    """
    try:
        # LOGGING INICIAL - DEBUG E AUDITORIA
        print(f"Processing alarm: {json.dumps(event)}")
        
        # EXTRAÇÃO DE DADOS DO PAYLOAD MQTT
        # Estrutura esperada do payload:
        # {
        #   "d": {
        #     "alarm": ["Mensagem do alarme"],
        #     "email": ["email1@domain.com", "email2@domain.com"]
        #   },
        #   "ts": "2025-08-31 15:30:00",
        #   "equipment": "25080001"  # Adicionado pela IoT Rule
        # }
        
        # Extrai seção "d" que contém dados dos sensores/alarmes
        d_data = event.get('d', {})
        
        # Extrai arrays de alarme e email (formato IHM Delta)
        alarm_array = d_data.get('alarm', [])
        email_array = d_data.get('email', [])
        
        # PROCESSAMENTO DOS ARRAYS - CONVERSÃO PARA VALORES ÚNICOS
        # Extrai primeira mensagem do array de alarmes
        # IHM Delta envia arrays mesmo para valores únicos
        alarm_message = alarm_array[0] if alarm_array and len(alarm_array) > 0 else 'Unknown alarm'
        
        # Processa lista de emails destinatários
        email_list = email_array if isinstance(email_array, list) and email_array else []
        
        # Extrai timestamp do evento (formato: YYYY-MM-DD HH:MM:SS)
        timestamp = event.get('ts', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Equipment ID extraído do tópico MQTT pela IoT Rule
        equipment = event.get('equipment', 'unknown')
        
        # VALIDAÇÃO E TRATAMENTO DE EMAILS
        # Se não há emails especificados, usa email de teste padrão
        if not email_list:
            print("No email recipients specified, using default test email")
            email_list = ['biorreator.xxx@gmail.com']
        
        print(f"Extracted - Alarm: {alarm_message}, Emails: {email_list}, Equipment: {equipment}")
        
        # PREPARAÇÃO DO REGISTRO DE AUDITORIA
        # Estrutura para armazenamento no S3
        alarm_record = {
            'alarm_message': alarm_message,
            'email_recipients': email_list,
            'timestamp': timestamp,
            'equipment': equipment,
            'processed_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'email_sent': False,  # Flag para controle de envio
            'test_mode': True,    # Indica modo de teste
            'raw_payload': event  # Payload original para debugging
        }
        
        # GERAÇÃO DA CHAVE S3 COM PARTICIONAMENTO
        # Parse do timestamp para criar estrutura de partições
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        
        # Estrutura: alarms/year=2025/month=08/day=31/equipment=25080001/timestamp_alarm.json
        s3_key = f"{S3_PREFIX}year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/equipment={equipment}/{dt.strftime('%Y%m%d%H%M%S')}_alarm.json"
        
        # CONFIGURAÇÃO DO EMAIL
        recipients = email_list
        subject = f"Alerta Biorreator {equipment}"
        
        # TEMPLATE HTML DO EMAIL - FORMATAÇÃO PROFISSIONAL
        html_body = f"""
        <html>
            <body>
                <!-- Cabeçalho do sistema -->
                <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; border-radius: 5px;">
                    <h3 style="color: #856404;">Sistema Biorreator</h3>
                </div>
                <br>
                
                <!-- Título do alerta -->
                <h2 style="color: #d32f2f;">⚠️ ALERTA BIORREATOR</h2>
                
                <!-- Tabela com informações do alarme -->
                <table style="border-collapse: collapse; width: 100%;">
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">Equipamento:</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{equipment}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">Data/Hora:</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{timestamp}</td>
                    </tr>
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">Mensagem:</td>
                        <td style="border: 1px solid #ddd; padding: 8px; color: #d32f2f; font-weight: bold;">{alarm_message}</td>
                    </tr>
                </table>
                <br>
                
                <!-- Informações dos destinatários -->
                <div style="background-color: #e3f2fd; border-left: 4px solid #2196f3; padding: 10px;">
                    <p style="margin: 0; color: #1976d2;">
                        📧 <strong>Destinatários:</strong> {', '.join(recipients)}
                    </p>
                </div>
                <br>
                
                <!-- Rodapé com informações técnicas -->
                <p style="color: #666; font-size: 12px;">
                    Este é um <strong>teste</strong> do sistema de monitoramento de biorreatores.
                    <br>Remetente: {SES_FROM}
                    <br>Processado: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
                </p>
            </body>
        </html>
        """
        
        # TEMPLATE TEXTO PLANO - FALLBACK PARA CLIENTES SEM HTML
        text_body = f"""
        ALERTA BIORREATOR
        
        Equipamento: {equipment}
        Data/Hora: {timestamp}
        Mensagem: {alarm_message}
        
        Destinatários: {', '.join(recipients)}
        
        Este é um TESTE do sistema de monitoramento.
        Remetente: {SES_FROM}
        Processado: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
        """
        
        try:
            # ENVIO DO EMAIL VIA AMAZON SES
            response = ses.send_email(
                Source=SES_FROM,                    # Remetente verificado
                Destination={'ToAddresses': recipients},  # Lista de destinatários
                Message={
                    'Subject': {'Data': subject},   # Assunto do email
                    'Body': {
                        'Text': {'Data': text_body}, # Versão texto plano
                        'Html': {'Data': html_body}  # Versão HTML formatada
                    }
                }
            )
            
            # Logging de sucesso
            print(f"Email sent successfully to: {recipients}")
            print(f"SES MessageId: {response['MessageId']}")
            
            # Atualiza registro de auditoria
            alarm_record['email_sent'] = True
            alarm_record['ses_message_id'] = response['MessageId']
            
        except Exception as e:
            # TRATAMENTO DE ERROS DE ENVIO DE EMAIL
            error_msg = str(e)
            print(f"❌ Error sending email: {error_msg}")
            alarm_record['email_error'] = error_msg
            
            # Diagnósticos específicos para erros comuns
            if "Email address not verified" in error_msg:
                print(f"SOLUÇÃO: Verificar email {SES_FROM} no Amazon SES")
            elif "MessageRejected" in error_msg:
                print("SOLUÇÃO: Verificar configurações SES")
        
        # SALVAMENTO DO REGISTRO DE AUDITORIA NO S3
        # Armazena todas as informações do alarme para auditoria
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(alarm_record, indent=2),
            ContentType='application/json'
        )
        
        # RESPOSTA DE SUCESSO
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'mode': 'TEST',
                's3_key': s3_key,
                'equipment': equipment,
                'alarm_message': alarm_message,
                'recipients': recipients,
                'email_sent': alarm_record.get('email_sent', False),
                'sender': SES_FROM
            })
        }
        
    except Exception as e:
        # TRATAMENTO DE ERROS GERAIS
        print(f" Error processing alarm: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e), 'mode': 'TEST'})
        }
