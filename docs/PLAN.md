---
PLAN: "fix(events/mock): map-free in-process Broker — zero map in the WASM tree (tinygo budget)"
TAG: v0.0.4
EXECUTOR: jules
REVIEWER: none
---

# PLAN — `events/mock` Broker sin `map`

Orquestador: [`DEMO_AGENDA_MASTER_PLAN.md`](../../../webtyp/docs/DEMO_AGENDA_MASTER_PLAN.md) — decisión abierta **O1**, resuelta: **no `map` en el árbol WASM**.

## Contexto

`webtyp/events/mock.Broker` es el broker in-proc de referencia (single-binary) y
lo usa la demo `app-demo` (`config` → `appointment_booking.Deps.Publisher`), que
**compila a WASM**. Su `Publish` usa `map[string][]events.Handler`
(`mock/broker.go`). La regla del ecosistema: cero `map` en código que compila a
WASM — tinygo agranda el binario con el runtime del `map`. Este es el único `map`
que la demo introduce en el árbol (el `fsm.go` de appointment_booking es un `map`
pre-existente fuera de este plan).

Antes de esto el plan maestro ofrecía "(a) aceptalo con nota" / "(b) paquete
`events/inproc`". La decisión final del dueño: **no debe existir ningún `map`** —
arreglar `mock.Broker` en su lugar. `mock` ya es el broker de referencia para
"módulo-a-módulo en un binario"; crear `inproc` sería duplicar con otra pieza.

## Cambio (`mock/broker.go`)

Reemplazar el `map[string][]events.Handler` por un slice de structs con scan
lineal — mismo patrón que `router/loopback/registry`:

```go
type Broker struct {
	mu   sync.Mutex
	// subs es un slice (no map): un broker de módulo→módulo tiene pocos topics
	// y pocos handlers por topic; un scan lineal no cuesta nada medible y
	// este paquete vive dentro del binario WASM de la demo.
	subs []subEntry
}

type subEntry struct {
	topic   string
	handlers []events.Handler
}
```

- `Subscribe(topic, h)`: append al slice (nada que cambiar semanticamente).
- `Publish(e)`: copiar bajo lock los handlers de CADA entry cuyo `topic == e.Topic`
  a un slice plano y ejecutarlos fuera del lock (preserva el snapshot de hoy).
- `var _ events.Broker = (*Broker)(nil)` se mantiene.
- El `sync.Mutex` se CONSERVA: mock es también el test double de consumers que
  disparan Publish desde goroutines (safe-for-concurrent era su contrato). `sync`
  no es `map`; esa regla es solo del tamaño del binario y una mutex no lo hincha.

## Tests

- Actualizar los tests existentes de `events` (`tests/consumer_test.go`,
  `tests/conformance_test.go`) — la API no cambia, así que deberían quedar
  verdes sin tocar el cuerpo; si alguno asume `map`, ajustarlo.
- Nuevo, en `events/tests/` (consumer-shaped):
  - `TestBroker_MultipleTopics_ScopedDelivery` — suscribir topic A y B; publicar
    A → solo recibe A (B intacto).
  - `TestBroker_OrderPreserved` — dos handlers en el mismo topic se ejecutan en
    orden de suscripción (regresión del slice).
  - `TestBroker_ConcurrentPublish_NoRace` — `-race` sobre Publish/Subscribe
    concurrentes (mantiene la garantía safe-for-concurrent del mock).
- `gotest ./...` verde.

## Criterios de aceptación

- `mock/broker.go` no contiene `map[`.
- `gotest ./...` verde en `events` (`-race` incluido).
- `GOOS=js GOARCH=wasm go build ./...` OK.
- En la demo: reconfirmar que el árbol WASM de `app-demo` no gana `map` desde
  `events` — `GOOS=js GOARCH=wasm go list -deps ./web/` no cambia (verificación
  manual del agente de la Etapa G).

## Fuera de alcance

- El `map` de `appointment_booking/fsm.go` (pre-existente, documentado en su
  AGENTS; plan separado).
- `webtyp/sse`: broker real wire, no in-proc.