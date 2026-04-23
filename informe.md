# Informe de resolución del Trabajo Práctico

- **Alumno**: Agustín Altamirano
- **Padrón**: 110237

---

## 1. Middlewares y tipos de mensaje

Todas las interacciones entre filtros usan RabbitMQ a través de estas dos clases:

- `MessageMiddlewareQueueRabbitMQ`: interfaz utilizada para interactuar con una cola de RabbitMQ.
- `MessageMiddlewareExchangeRabbitMQ`: interfaz utilizada para interactuar con un exchange de tipo direct de RabbitMQ.

Para la serialización interna (entre filtros), se utiliza JSON.

### 1.1 Middlewares usados por SumFilter

Este tipo de filtro produce y consume mensajes de control y datos de los siguientes middlewares:

| Rol              | Nombre                                              | Tipo            |
| ---------------- | --------------------------------------------------- | --------------- |
| Entrada de datos | `INPUT_QUEUE`                                       | Cola            |
| Salida de datos  | `aggregation_0`, `aggregation_1`, …                 | Cola            |
| Control          | `SUM_CONTROL_EXCHANGE` con routing key `SUM_PREFIX` | Exchange direct |

**Mensajes que recibe por `INPUT_QUEUE` (publicados por el Gateway):**

- Dato de cantidad de una fruta: `[client_id, fruit, amount]`
- EOF del cliente: `[client_id, message_count]`. Contiene la cantidad total de mensajes de datos que el gateway envió para ese cliente.

**Mensajes que publica hacia cada `aggregation_i`:**

- Dato sumado: `[client_id, fruit, amount_total]`. Posee la cantidad total acumulada localmente para esa fruta y cliente.
- EOF del sum: `[client_id]`. Indica que esa instancia de `SumFilter` ya emitió todos los datos que le correspondían para ese cliente. Cada instancia de Sum envía un único EOF a todas las colas `aggregation_i` al finalizar la barrera del cliente.

**Mensajes que publica/consume por `SUM_CONTROL_EXCHANGE` (ambos producer/consumer son los propios Sum):**

- `["EOF_RECEIVED", client_id, message_count_total]`: la instancia que leyó el EOF del cliente lo anuncia al resto.
- `["PROCESSED_MESSAGE_COUNT", client_id, instance_id, message_count_local]`: Contiene la información de cuántos mensajes procesó una instancia para ese cliente hasta el momento.

Como el exchange es de tipo direct con una sola routing key y cada instancia declara una cola exclusiva bindeada a esa key, los mensajes hacen fan‑out a todas las instancias de Sum.

### 1.2 AggregationFilter

| Rol     | Nombre                      | Tipo |
| ------- | --------------------------- | ---- |
| Entrada | `{AGGREGATION_PREFIX}_{ID}` | Cola |
| Salida  | `OUTPUT_QUEUE`              | Cola |

**Mensajes que recibe (emitidos por Sum):**

- Dato sumado: `[client_id, fruit, amount]`. Es un mensaje por cada fruta que, según el hash, le corresponde a esta instancia.
- EOF: `[client_id]`. Mensaje enviada por cada instancia de Sum.

**Mensaje que publica hacia el Join:**

- Top parcial: `[client_id, partial_fruit_top]`, donde `partial_fruit_top` tiene como máximo `TOP_SIZE` elementos.

### 1.3 JoinFilter

| Rol     | Nombre         | Tipo |
| ------- | -------------- | ---- |
| Entrada | `INPUT_QUEUE`  | Cola |
| Salida  | `OUTPUT_QUEUE` | Cola |

**Mensaje que recibe (emitido por cada Aggregation):**

- Top parcial: `[client_id, partial_fruit_top]`.

**Mensaje que publica hacia el Gateway:**

- Top final: `[client_id, final_fruit_top]`

---

## 2. Flujo de manejo del EOF de un cliente

Uno de los desafíos de la implementación consiste en manejar los EOF del cliente enviados por el Gateway. Este mensaje es encolado en la cola `INPUT_QUEUE`, y solo una instancia de `SumFilter` lo recibe. Para que todas las instancias de `SumFilter` puedan saber cuándo llega este mensaje, se implementa un protocolo de coordinación basado en una **barrera por conteo total de mensajes**, utilizando el exchange `SUM_CONTROL_EXCHANGE`.

### Paso 1: Una instancia de `SumFilter` recibe el EOF por `INPUT_QUEUE`

El Gateway publica el EOF como `[client_id, message_count_total]`. RabbitMQ entrega ese mensaje a **una sola** instancia de Sum.

Esa instancia ejecuta el método `_process_eof`, cuya única responsabilidad es anunciar a todas las instancias de `SumFilter` (incluida ella misma) que el EOF ya llegó. Para eso publica en `SUM_CONTROL_EXCHANGE` el mensaje:

```
["EOF_RECEIVED", client_id, message_count_total]
```

La instancia no modifica todavía su estado local. Para ello, espera a que ese mismo mensaje le llegue por el exchange y lo procese en el thread de control. Esto se diseñó así con el objetivo de mantener la lógica "simétrica" entre instancias.

### Paso 2: Fan‑out del `EOF_RECEIVED` a todas las instancias de `SumFilter`

Cuando una instancia procesa `EOF_RECEIVED`, se realizan los siguientes pasos:

1. Llama a `set_global_expected_message_count(client_id, message_count_total)`:
   - Guarda `message_count_total` en `__global_expected_message_count[client_id]`.
   - Marca al cliente en `__clients_with_eof`. Esta marca es la que habilita dos cosas: primero, que el thread de datos empiece a re‑publicar su contador cada vez que procese un dato nuevo de ese cliente _(ver Paso 3)_. En segundo lugar, permite que al recibir `PROCESSED_MESSAGE_COUNT` del resto, se evalúe la condición de barrera _(ver Paso 4)_.
   - Devuelve el contador local actual de mensajes procesados por esta instancia para ese cliente.
2. Con ese contador local, publica en `SUM_CONTROL_EXCHANGE` el mensaje:
   ```
   ["PROCESSED_MESSAGE_COUNT", client_id, ID, instance_message_count_actual]
   ```
   (donde `ID` es el id de esa instancia de `SumFilter`).

### Paso 3: Consumo del resto de datos de `INPUT_QUEUE` y republicación de conteos

El EOF puede llegar a una de las instancias de `SumFilter` antes de que las demás instancias hayan terminado de procesar los datos que RabbitMQ les repartió a cada una. Esto puede ocurrir debido a que algunas instancias procesaron más rápido que otras. Debido a esto, puede pasar que, cuando les llegue el mensaje `EOF_RECEIVED` a todas las instancias, algunas de ellas todavía estén procesando datos del cliente.

Para solucionar este problema, a partir del momento en el que recibe el `EOF_RECEIVED` de un cliente determinado la instancia de `SumFilter` empieza a republicar su contador actualizado cada vez que procesa un nuevo dato de ese cliente.

### Paso 4: Acumulación global de conteos y evaluación de la barrera

Cada instancia de `SumFilter` recibe todos los `PROCESSED_MESSAGE_COUNT` publicados por las instancias (incluida ella misma). El thread de control los procesa en `_process_control_processed_message_count`:

1. Actualiza `__global_message_count[client_id][sum_instance_id]` (un diccionario que posee la cantidad de mensajes procesados por cada instancia para cada cliente) con `max(anterior, nuevo)` (`message_count_controller.py:37-43`). Usar `max` es necesario porque los mensajes de una misma instancia pueden llegar desordenados y los valores sólo crecen en el tiempo.
2. Evalúa `client_has_received_all_messages(client_id)` (`message_count_controller.py:45-51`), que es verdadera cuando la suma de los últimos contadores reportados por cada instancia alcanza o supera el esperado.
3. Si la barrera se cumple, esta instancia ejecuta `_flush_client_fruits(client_id)`, lo cual envía los resultados acumulados para ese cliente hacia las instancias de `AggregationFilter`.
4. Luego se ejecuta `reset_client_count(client_id)`, que borra todo el estado asociado a ese cliente en esta instancia.

### Análisis de cantidad de mensajes de control

Como este algoritmo de sincronización basa su lógica en la publicación en broadcast de mensajes de control, es importante analizar cuántos mensajes se publican en total por cliente:

- `EOF_RECEIVED`: un mensaje por cliente, publicado por la instancia que recibió el EOF.
- `PROCESSED_MESSAGE_COUNT`: en el mejor caso (todas las instancias de `SumFilter` han procesado todos los mensajes del cliente cuando llega el EOF), se publica un mensaje por instancia. Si bien ese escenario no sucede siempre, la cantidad de mensajes de control no se eleva mucho más, ya que al llegar el EOF tenemos la certeza de que el procesamiento global ya se encuentra en los últimos mensajes del cliente.

---

## 3. Envío de resultados desde `SumFilter` hacia `AggregationFilter`

La distribución se hace por **sharding determinístico** sobre el par `(client_id, fruit)`:

```python
def _client_fruit_hash(self, client_id, fruit):
    return zlib.crc32(f"{client_id}_{fruit.fruit}".encode("utf-8")) % AGGREGATION_AMOUNT
```

En el flush, se utiliza este método para hallar el ID de la instancia de `AggregationFilter` al que se eviará el resultado acumulado de una fruta para un cliente determinado. Algunas observaciones sobre esta estrategia:

- **Determinismo por fruta y cliente**: todas las apariciones de `(client_id, fruit_name)`,aunque hayan sido procesadas por instancias distintas de `SumFilter`, terminan en la misma instancia de `AggregationFilter`. Esto es lo que garantiza la correctitud del algorimto top‑K ejecutado en el siguiente paso de procesamiento (la suma total de cada fruta debe completarse en un único nodo).
- **Aprovechamiento de las instancias disponibles**: al aplicar una función de hashing de esta forma, se garantiza que los distintos tipos de frutas enviados por un mismo cliente sean distriibuidos de forma bastante uniforme entre todas las instancias disponibles de `AggregationFilter`. Es decir, potencialmente se puede distribuir el procesamiento del cáluclo del top-K de un mismo cliente entre varias instancias de `AggregationFilter`.

El envío de los mensajes de los resultados se hace mediante colas, una por `ID` de `AggregationFilter`, con nombre `{AGGREGATION_PREFIX}_{ID}`. Después de publicar los datos, el Sum publica `[client_id]` en todas las colas de los `AggregationFilter`. Por lo tanto, cada una de estas instancias recibirá exactamente `SUM_AMOUNT` barreras por cliente, una por instancia de Sum.

---

## 4. Manejo de EOF en `AggregationFilter`

La lógica del manejo del EOF en estas instancias se basa en la invariante de que cada `SumFilter` publica exactamente un EOF `[client_id]` a cada `AggregationFilter`. Por lo tanto el número esperado de EOF por cliente es exactamente `SUM_AMOUNT`, independientemente de si esa partición recibió datos o no.

Eso es importante: una instancia de `AggregationFilter` que no recibió datos de un cliente aún así recibe los `SUM_AMOUNT` EOF y emite un top parcial (vacío) al `JoinFilter`, porque este último espera siempre `AGGREGATION_AMOUNT` mensajes por cliente.

---

## 5. Manejo de datos en `JoinFilter`

Esta instancia recibe `AGGREGATION_AMOUNT` mensajes por cliente, cada uno con un top parcial. El `JoinFilter` acumula los resultados parciales y mantiene un contador de tops parciales recibidos. Cuando el contador alcanza `AGGREGATION_AMOUNT`, se calcula el top final sumando las cantidades de cada fruta y tomando las `TOP_SIZE` frutas con mayor cantidad. Luego se publica el resultado hacia el Gateway.

---

## 6. Escalabilidad

El sistema se parametriza con las variables de entorno `SUM_AMOUNT` y `AGGREGATION_AMOUNT`. Estas determinan cuántas instancias se levantan de cada una y se propagan por variables de entorno al resto. Todo el estado en los filtros se indexa por `client_id`, por lo que el avance de cada cliente es independiente.

### a. Un cliente, una instancia de cada filtro

- **Uso de recursos**: el procesamiento es serial. Todo se resuelve en un único pipeline, y el paralelismo disponible se limita al solapamiento entre los tres filtros mientras procesan mensajes en vuelo por RabbitMQ.
- **Protocolo usado**: prácticamente sólo el flujo de datos. El protocolo de coordinación de EOF de `SumFilter` se ejecuta de todas formas, pero no es necesario ya que existe una sola instancia de `SumFilter`.
- **Cuellos de botella**: el throughput está limitado por la instancia más lenta del pipeline. No hay manera de paralelizar carga.

### b. Varios clientes, una instancia de cada filtro

- **Uso de recursos**: los clientes se procesan concurrentemente _dentro_ de cada filtro. El Gateway atiende múltiples conexiones en paralelo, pero a partir de `INPUT_QUEUE` todo vuelve a serializarse en la única instancia de cada filtro.
- **Protocolo usado**: el mismo que en (a), replicado por cliente. El estado por cliente garantiza que el avance de un cliente no bloquee al resto.
- **Cuellos de botella**: el throughput por filtro se reparte entre todos los clientes simultáneos; un cliente con volumen grande puede saturar el `SumFilter` y retrasar los demás. La memoria de las instancias de cada etapa crece con la cantidad de clientes activos (acumula todos sus datos hasta que termina de procesarlos y pasa a la siguiente etapa).

### c. Un cliente, varias instancias de cada filtro

- **Uso de recursos**: es el escenario donde el paralelismo es efectivo. RabbitMQ reparte los datos del cliente en `INPUT_QUEUE` entre las `SUM_AMOUNT` instancias de `SumFilter`, y el sharding distribuye las frutas del cliente entre las `AGGREGATION_AMOUNT` instancias de `AggregationFilter`.
- **Protocolo usado**: el protocolo de coordinación entre Sum se utiliza en plenitud y es indispensable. Hace falta `EOF_RECEIVED` para que la instancia que recibió el EOF lo propague a las demás, y hacen falta los `PROCESSED_MESSAGE_COUNT` (más su re‑publicación oportunista del Paso 3) para alcanzar la barrera. En `AggregationFilter` el conteo de `SUM_AMOUNT` EOF también se usa realmente; así como también en `JoinFilter` el conteo de `AGGREGATION_AMOUNT` tops parciales.
- **Cuellos de botella**:
  - El `JoinFilter` es siempre una única instancia y concentra los parciales de todas las Aggregation. En la práctica no es grave porque cada parcial ya viene reducido a `TOP_SIZE` elementos.
  - El tráfico de control crece con `SUM_AMOUNT` (cada instancia le responde al resto), aunque el volumen por cliente se mantiene acotado porque el EOF y la barrera ocurren una sola vez.

### d. Varios clientes, varias instancias de cada filtro

- **Uso de recursos**: es la composición de (b) y (c). El sistema aprovecha todas las instancias en paralelo; además, como el shard key incluye `client_id`, las frutas de clientes distintos se reparten entre distintas instancias de `AggregationFilter`, lo que aumenta aún más el balanceo.
- **Protocolo usado**: el mismo conjunto completo que en (c), pero con una conversación de coordinación por cliente. Las conversaciones no se mezclan porque cada mensaje de control lleva el `client_id` y el estado del `MessageCountController` está indexado por él.
- **Cuellos de botella**: a los de (c) se suman:
  - Contención del único `JoinFilter` si muchos clientes terminan simultáneamente.
  - Memoria proporcional al número de clientes activos x su volumen en Sum y Aggregation.
  - Tráfico en el exchange de control proporcional a `clientes_activos x SUM_AMOUNT`.

## Supuesto acerca de los ids de clientes

El sistema supone que cada `client_id` es único (UUID4 generado en el gateway por conexión). En el caso casi imposible de que dos clientes compartieran `client_id`, sus estados se mezclarían en todos los filtros.
