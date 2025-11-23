"""
FleetLogix - Configuración AWS
Script para configurar servicios AWS básicos
"""

import boto3
import json
import psycopg2
from datetime import datetime

# Configuración
AWS_REGION = 'us-east-1'
RDS_INSTANCE_ID = 'fleetlogix-db'
S3_BUCKET_NAME = 'fleetlogix-data'

# Clientes AWS
rds = boto3.client('rds', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
iam = boto3.client('iam')

def crear_rds_postgresql():
    """Crear instancia RDS PostgreSQL"""
    print(" Creando RDS PostgreSQL...")
    
    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier=RDS_INSTANCE_ID,
            DBInstanceClass='db.t3.micro',  # Free tier
            Engine='postgres',
            EngineVersion='15.4',
            MasterUsername='fleetlogix_admin',
            MasterUserPassword='FleetLogix2024!',  # Cambiar en producción
            AllocatedStorage=20,
            StorageType='gp2',
            BackupRetentionPeriod=7,  # Backups automáticos 7 días
            PreferredBackupWindow='03:00-04:00',
            PreferredMaintenanceWindow='sun:04:00-sun:05:00',
            PubliclyAccessible=True,
            Tags=[
                {'Key': 'Project', 'Value': 'FleetLogix'},
                {'Key': 'Environment', 'Value': 'Development'}
            ]
        )
        print(f" RDS creado: {response['DBInstance']['DBInstanceIdentifier']}")
        
    except rds.exceptions.DBInstanceAlreadyExistsFault:
        print("RDS ya existe")
    except Exception as e:
        print(f" Error creando RDS: {e}")

def crear_s3_bucket():
    """Crear bucket S3 para datos históricos"""
    print("\nCreando S3 Bucket...")
    
    try:
        # Crear bucket
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        
        # Configurar estructura de carpetas
        folders = [
            'raw-data/',
            'processed-data/',
            'backups/',
            'logs/'
        ]
        
        for folder in folders:
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"{folder}",
                Body=b''
            )
        
        # Configurar lifecycle para organizar por fecha
        lifecycle_config = {
            'Rules': [{
                'ID': 'archive-old-data',
                'Status': 'Enabled',
                'Transitions': [{
                    'Days': 90,
                    'StorageClass': 'GLACIER'
                }],
                'Prefix': 'raw-data/'
            }]
        }
        
        s3.put_bucket_lifecycle_configuration(
            Bucket=S3_BUCKET_NAME,
            LifecycleConfiguration=lifecycle_config
        )
        
        print(f" S3 Bucket creado: {S3_BUCKET_NAME}")
        
    except s3.exceptions.BucketAlreadyExists:
        print(" S3 Bucket ya existe")
    except Exception as e:
        print(f" Error creando S3: {e}")

def crear_tablas_dynamodb():
    """Crear tablas DynamoDB para estado actual"""
    print("\n Creando tablas DynamoDB...")
    
    tablas = [
        {
            'TableName': 'deliveries_status',
            'KeySchema': [
                {'AttributeName': 'delivery_id', 'KeyType': 'HASH'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'delivery_id', 'AttributeType': 'S'}
            ]
        },
        {
            'TableName': 'vehicle_tracking',
            'KeySchema': [
                {'AttributeName': 'vehicle_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'vehicle_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ]
        },
        {
            'TableName': 'routes_waypoints',
            'KeySchema': [
                {'AttributeName': 'route_id', 'KeyType': 'HASH'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'route_id', 'AttributeType': 'S'}
            ]
        },
        {
            'TableName': 'alerts_history',
            'KeySchema': [
                {'AttributeName': 'vehicle_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'vehicle_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ]
        }
    ]
    
    for tabla in tablas:
        try:
            response = dynamodb.create_table(
                TableName=tabla['TableName'],
                KeySchema=tabla['KeySchema'],
                AttributeDefinitions=tabla['AttributeDefinitions'],
                BillingMode='PAY_PER_REQUEST',  # On-demand
                Tags=[
                    {'Key': 'Project', 'Value': 'FleetLogix'}
                ]
            )
            print(f" Tabla creada: {tabla['TableName']}")
            
        except dynamodb.exceptions.ResourceInUseException:
            print(f" Tabla ya existe: {tabla['TableName']}")
        except Exception as e:
            print(f" Error creando tabla {tabla['TableName']}: {e}")

def configurar_backups_automaticos():
    """Configurar backups automáticos para RDS"""
    print("\n⚙️ Configurando backups automáticos...")
    
    try:
        # Los backups ya están configurados en create_db_instance
        # Aquí podríamos agregar configuración adicional
        
        # Crear snapshot manual inicial
        snapshot_id = f"fleetlogix-initial-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=RDS_INSTANCE_ID,
            Tags=[
                {'Key': 'Type', 'Value': 'Manual'},
                {'Key': 'Project', 'Value': 'FleetLogix'}
            ]
        )
        
        print(f" Snapshot inicial creado: {snapshot_id}")
        print(" Backups automáticos configurados (retención: 7 días)")
        
    except Exception as e:
        print(f" Error configurando backups: {e}")

def migrar_datos_postgresql():
    """Script para migrar datos de PostgreSQL local a RDS"""
    print("\n Preparando migración de PostgreSQL local a RDS...")
    
    migration_script = """
#!/bin/bash
# Script de migración PostgreSQL local -> RDS

# Variables
LOCAL_DB="fleetlogix"
LOCAL_USER="postgres"
RDS_ENDPOINT="fleetlogix-db.xxxx.us-east-1.rds.amazonaws.com"
RDS_USER="fleetlogix_admin"
RDS_DB="fleetlogix"

echo " Iniciando migración de base de datos..."

# 1. Hacer dump de la base local
echo " Exportando base de datos local..."
pg_dump -h localhost -U $LOCAL_USER -d $LOCAL_DB -f fleetlogix_dump.sql

# 2. Crear base de datos en RDS
echo " Creando base de datos en RDS..."
psql -h $RDS_ENDPOINT -U $RDS_USER -c "CREATE DATABASE $RDS_DB;"

# 3. Restaurar en RDS
echo " Importando datos en RDS..."
psql -h $RDS_ENDPOINT -U $RDS_USER -d $RDS_DB -f fleetlogix_dump.sql

echo " Migración completada"
"""
    
    with open('migrate_to_rds.sh', 'w') as f:
        f.write(migration_script)
    
    print(" Script de migración creado: migrate_to_rds.sh")
    print("   Ejecutar con: bash migrate_to_rds.sh")

def crear_rol_iam_lambda():
    """Crear rol IAM para funciones Lambda"""
    print("\n Creando rol IAM para Lambda...")
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    
    try:
        # Crear rol
        role_response = iam.create_role(
            RoleName='FleetLogixLambdaRole',
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Rol para funciones Lambda de FleetLogix'
        )
        
        # Adjuntar políticas
        policies = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess',
            'arn:aws:iam::aws:policy/AmazonSNSFullAccess'
        ]
        
        for policy in policies:
            iam.attach_role_policy(
                RoleName='FleetLogixLambdaRole',
                PolicyArn=policy
            )
        
        print(" Rol IAM creado: FleetLogixLambdaRole")
        return role_response['Role']['Arn']
        
    except iam.exceptions.EntityAlreadyExistsException:
        print(" Rol IAM ya existe")
        return f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/FleetLogixLambdaRole"
    except Exception as e:
        print(f" Error creando rol: {e}")
        return None

def main():
    """Ejecutar configuración completa"""
    print("FLEETLOGIX - Configuración AWS")
    print("="*50)
    
    # 1. Crear servicios
    crear_rds_postgresql()
    crear_s3_bucket()
    crear_tablas_dynamodb()
    
    # 2. Configurar
    configurar_backups_automaticos()
    migrar_datos_postgresql()
    
    # 3. Crear rol para Lambda
    rol_arn = crear_rol_iam_lambda()
    
    print("\nCONFIGURACIÓN BÁSICA COMPLETADA")
    print("\nPróximos pasos:")
    print("1. Esperar ~10 min para que RDS esté disponible")
    print("2. Ejecutar script de migración: bash migrate_to_rds.sh")
    print("3. Desplegar funciones Lambda con el rol:", rol_arn)
    print("4. Configurar API Gateway")
    print("5. Configurar triggers automáticos")
    
    # Guardar configuración
    config = {
        'rds_instance': RDS_INSTANCE_ID,
        's3_bucket': S3_BUCKET_NAME,
        'dynamodb_tables': [
            'deliveries_status',
            'vehicle_tracking', 
            'routes_waypoints',
            'alerts_history'
        ],
        'lambda_role_arn': rol_arn,
        'region': AWS_REGION,
        'timestamp': datetime.now().isoformat()
    }
    
    with open('aws_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("\n Configuración guardada en: aws_config.json")

if __name__ == "__main__":
    main()