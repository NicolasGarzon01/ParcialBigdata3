import json
import urllib.request
import urllib.error
from datetime import datetime
import boto3

def lambda_handler(event, context):
    """
    Función Lambda que descarga contenido de El Tiempo y El Espectador
    y lo guarda en S3 siguiendo la estructura requerida
    """
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Diccionario con medios y URLs
    medios = {
        "eltiempo": "https://www.eltiempo.com/",
        "elespectador": "https://www.elespectador.com/"
    }
    
    # Configurar S3 client
    s3 = boto3.client('s3')
    bucket = 'mi-parcial-bucket-1749009073'  # Cambia por tu bucket real
    
    resultados = []
    
    for nombre, url in medios.items():
        try:
            # Headers para evitar bloqueos
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Crear request con headers
            req = urllib.request.Request(url, headers=headers)
            
            # Descargar contenido con timeout
            with urllib.request.urlopen(req, timeout=30) as response:
                content = response.read()
                
                # Verificar que la respuesta sea exitosa (200)
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
            
            # Estructura S3 según especificaciones
            s3_key = f"headlines/raw/contenido-{today}.html"
            
            # Subir a S3
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=content,
                ContentType='text/html',
                Metadata={
                    'source': nombre,
                    'url': url,
                    'scrape_date': today,
                    'content_length': str(len(content))
                }
            )
            
            resultados.append({
                'medio': nombre,
                'status': 'success',
                's3_key': s3_key,
                'content_size': len(content)
            })
            
            print(f"✅ {nombre}: guardado en {s3_key}")
            
        except urllib.error.HTTPError as e:
            error_msg = f"❌ {nombre}: HTTP error {e.code} - {e.reason}"
            print(error_msg)
            
            resultados.append({
                'medio': nombre,
                'status': 'error',
                'error': f"HTTP {e.code}: {e.reason}"
            })
            
        except urllib.error.URLError as e:
            error_msg = f"❌ {nombre}: URL error - {str(e.reason)}"
            print(error_msg)
            
            resultados.append({
                'medio': nombre,
                'status': 'error',
                'error': f"URL error: {str(e.reason)}"
            })
            
        except Exception as e:
            error_msg = f"❌ {nombre}: error > {str(e)}"
            print(error_msg)
            
            resultados.append({
                'medio': nombre,
                'status': 'error',
                'error': str(e)
            })
    
    # Respuesta JSON para Lambda
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': 'Web scraping completed',
            'date': today,
            'results': resultados
        }, indent=2)
    }