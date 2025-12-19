1. Descripción del proyecto

Este proyecto implementa un proceso ETL para integrar, limpiar, transformar y cargar datos operativos provenientes de archivos JSON del dominio e-commerce. El resultado final se almacena en una base de datos PostgreSQL para análisis posterior, permitiendo extraer métricas como:

total de ventas por cliente

número de órdenes por país

ticket promedio

El objetivo principal fue transformar datos crudos en información estructurada, trazable y analíticamente útil.

2. Arquitectura del sistema

El proyecto se estructura en capas independientes para mantener claridad, mantenibilidad y separación de responsabilidades:

RETO_DATA_ENGINEER/
│
├── config/
│   └── db_config.yaml
│
├── data/
│   ├── json/         (input original)
│   └── csv/          (output opcional)
│
├── etl/
│   ├── extract.py    (lectura de datos)
│   ├── transform.py  (limpieza y estandarización)
│   └── load.py       (carga a PostgreSQL)
│
├── sql/
│   ├── ddl.sql       (modelo de base de datos)
│   └── queries.sql   (consultas analíticas)
│
├── utils/
│   ├── logger.py     (logging centralizado)
│   └── validators.py (verificación de calidad de datos)
│
├── main_etl.py       (pipeline ejecutable)
├── requirements.txt
└── README.md

3. Flujo ETL
3.1 Extract

lectura de archivos JSON

normalización de estructuras

validación de schema

manejo de rutas dinámicas

3.2 Transform

limpieza de valores nulos

conversión de fechas al formato estándar ISO 8601

conversión de montos a tipo numérico

normalización de correos electrónicos (lowercase, trim)

eliminación de registros inválidos documentados en el log

3.3 Load

inserción incremental en PostgreSQL

control de duplicados mediante llaves naturales

manejo de errores SQL

uso de transacciones

4. Modelo de datos

Se diseñó un esquema simple orientado al análisis:

Tabla principal – customers

customer_pk (PK)

customer_id

full_name

email

country

language

birth_date

registration_date

Tabla fact – orders

order_pk (PK)

order_id

customer_pk (FK)

total_amount

currency

order_date

status

Relación de uno-a-muchos:
customers.customer_pk → orders.customer_pk

5. SQL del proyecto

El archivo sql/queries.sql incluye tres consultas de negocio:

Total de ventas por cliente

Total de órdenes por país

Ticket promedio

6. Requerimientos

Instalar dependencias con:

pip install -r requirements.txt


Base de datos requerida: PostgreSQL 13+

Configurar credenciales en:
config/db_config.yaml

7. Ejecución

colocar los archivos JSON en /data/json/

crear las tablas ejecutando:
psql -f sql/ddl.sql

ejecutar el proceso ETL:

python main_etl.py


validar que los datos hayan sido cargados correctamente

8. Logging y control de calidad

El proyecto incluye:

trazabilidad completa por archivo procesado

identificación de registros descartados

control de errores

auditoría de inserciones

validaciones automáticas de contenido

Se genera salida en consola y archivo.

9. Supuestos

los JSON tienen estructura consistente por entidad

el email identifica al cliente de forma única

las fechas son válidas pero requieren normalización

si una orden no puede vincularse a un cliente, se descarta

10. Mejoras futuras

automatizar despliegue en Airflow

implementar particionamiento por fechas

añadir pruebas unitarias

soporte para nuevos medios de ingesta

control de calidad basado en reglas dinámicas

11. Autor

Eddy Oliva Bautista
