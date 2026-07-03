# Auditoría — dag_tbInicioSolAutPag_diario

> **DAG:** `dag_tbInicioSolAutPag_diario`
> **Schedule:** Lunes a Viernes 5:30am
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03

---

## Flujo de verificación (seguir en orden)

```
1. etl_audit_log  → estado OK para los 3 SPs
2. dag_run        → state = success
3. Correo         → ETL Notification OK
4. Log .txt       → tblInicioSolicitados/Autorizados/Pagados OK
5. Auditoría SQL  → diff_cnt=0 en los 3 destinos
```

---

## Consultas de auditoría SQL

### Verificar sincronización tblInicioSolicitados
```sql
SELECT origen.anio, origen.mes, origen.NomMes
     , origen.cant AS cnt_origen, COALESCE(destino.cant,0) AS cnt_destino
     , origen.cant - COALESCE(destino.cant,0) AS diferencia
FROM (
    SELECT YEAR(B.fCorreo) AS anio, MONTH(B.fCorreo) AS mes
         , ELT(MONTH(B.fCorreo),'Enero','Febrero','Marzo','Abril','Mayo','Junio',
               'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
         , COUNT(1) AS cant
    FROM customers.redeems B
    LEFT JOIN customers.fb_clients C ON C.clientid = B.clientid
    WHERE NOT ISNULL(B.fCorreo) AND IFNULL(B.status_r,0) <> 0
    AND B.fCorreo < CURDATE()
    GROUP BY anio, mes, NomMes
) origen
LEFT JOIN (
    SELECT anio, mes, COUNT(1) AS cant
    FROM flybackDW.tblInicioSolicitados
    WHERE fecha < CURDATE()
    GROUP BY anio, mes
) destino ON destino.anio = origen.anio AND destino.mes = origen.mes
ORDER BY origen.anio, origen.mes;
```

---

## Registro de ejecuciones

### YYYY-MM-DD — Ejecución diaria

#### 1. etl_audit_log
```
paquete         :
max_id_inicio   :
filas_insertadas:
estado          :
fecha_inicio    :
fecha_fin       :
mensaje_error   :
```

#### 2. dag_run
```
state     :
queued_at :
end_date  :
```

#### 3. Correo
```
Estado  :
Fecha   :
```

#### 4. Log .txt
```
(pegar contenido del log)
```

#### 5. Auditoría SQL
```
tblInicioSolicitados → diferencia: 0  ✅ / ⚠️
tblInicioAutorizados → diferencia: 0  ✅ / ⚠️
tblInicioPagados     → diferencia: 0  ✅ / ⚠️
```

**Resultado:** ✅ / ⚠️ (descripción si hay problema)

---

<!-- Copiar el bloque anterior para cada nueva ejecución -->
