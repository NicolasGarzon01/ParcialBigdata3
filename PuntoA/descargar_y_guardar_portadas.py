import json
import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'zappa-headlines-noticiass'

def descargar_y_guardar_portadas():
    fecha_actual = datetime.utcnow().strftime('%Y-%m-%d')
    
    medios_noticias = {
        'eltiempo': 'https://www.eltiempo.com',
        'publimetro': 'https://www.publimetro.co'
    }

    for nombre_medio, url in medios_noticias.items():
        try:
            print(f"[DESCARGA] Solicitando HTML de {nombre_medio}: {url}")
            respuesta = requests.get(url, timeout=10)
            respuesta.raise_for_status()
            contenido_html = respuesta.text

            ruta_archivo_s3 = f"headlines/raw/{nombre_medio}-contenido-{fecha_actual}.html"
            print(f"[S3] Guardando HTML en: {ruta_archivo_s3}")

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=ruta_archivo_s3,
                Body=contenido_html,
                ContentType='text/html'
            )
            print(f"✅ HTML de '{nombre_medio}' guardado exitosamente en S3")

        except Exception as error:
            print(f"❌ Error descargando '{nombre_medio}' desde {url}: {str(error)}")

    return {
        'statusCode': 200,
        'body': json.dumps('✔️ Descarga completada y archivos subidos.')
    }