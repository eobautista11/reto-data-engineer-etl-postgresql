📘 Proyecto ETL – Data Engineer
1. Descripción del proyecto

Este proyecto implementa un proceso ETL para integrar, limpiar, transformar y cargar datos operativos provenientes de archivos JSON del dominio e-commerce.

El resultado final se almacena en una base de datos PostgreSQL para análisis posterior, permitiendo extraer métricas como:

total de ventas por cliente

número de órdenes por país

ticket promedio

El objetivo principal fue transformar datos crudos en información estructurada, trazable y analíticamente útil.

2. Flujo ETL
2.1 Extract

lectura de archivos JSON

normalización de estructuras

validación de schema

manejo de rutas dinámicas

2.2 Transform

limpieza de valores nulos

conversión de fechas a formato estándar ISO 8601

conversión de montos a tipo numérico

normalización de correos electrónicos (lowercase, trim)

eliminación de registros inválidos documentados en el log

2.3 Load

inserción incremental en PostgreSQL

control de duplicados mediante llaves naturales

manejo de errores SQL

uso de transacciones

3. Modelo de datos

Se diseñó un esquema simple orientado al análisis:

📌 Tabla principal – customers

Campos:

customer_pk (PK)

customer_id

full_name

email

country

language

birth_date

registration_date

📌 Tabla fact – orders

Campos:

order_pk (PK)

order_id

customer_pk (FK)

total_amount

currency

order_date

status

Relación uno-a-muchos
customers.customer_pk → orders.customer_pk

4. SQL del proyecto

El archivo sql/queries.sql incluye tres consultas de negocio:

1️⃣ Total de ventas por cliente
2️⃣ Total de órdenes por país
3️⃣ Ticket promedio

5. Requerimientos

Instalar dependencias con:

pip install -r requirements.txt


Base de datos requerida: PostgreSQL 13+

Configurar credenciales en:

config/db_config.yaml

6. Ejecución

1️⃣ Colocar los archivos JSON en /data/json/

2️⃣ Crear las tablas ejecutando:

psql -f sql/ddl.sql


3️⃣ Ejecutar el ETL:

python main_etl.py


4️⃣ Validar resultados cargados en PostgreSQL

7. Logging y control de calidad

El proyecto incluye:

trazabilidad completa por archivo procesado

identificación de registros descartados

control de errores

auditoría de inserciones

validaciones automáticas

Salida generada en consola.

8. Supuestos

los JSON tienen estructura consistente por entidad

el email identifica al cliente de forma única

las fechas requieren normalización

órdenes sin cliente se descartan

9. Mejoras futuras

automatizar despliegue en Airflow

particionamiento por fechas

pruebas unitarias

nuevos métodos de ingesta

reglas dinámicas de calidad de datos

10. Autor

Eddy Oliva Bautista
