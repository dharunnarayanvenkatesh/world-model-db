# Digital twins

World Model DB represents a digital twin as a normal world entity with four
state channels, temporal topology, events, commands, provenance, and conflicts.
The model is deliberately domain-neutral: a twin may represent a machine,
building, vehicle, supply chain, patient, biological system, city asset,
software service, organization, process, simulation, or composite system.

## State model

Twin state is stored through the existing immutable observation pipeline:

```text
adapter / sensor / agent / simulator
                 |
                 v
 timestamped observation (event time + ingestion time)
                 |
                 v
 deterministic conflict resolution and provenance
                 |
                 v
 reported | desired | configuration | derived
                 |
                 v
 bitemporal snapshot + drift + WHY + history
```

The four channels are:

- `reported`: measured or externally observed state;
- `desired`: control-plane intent or target state;
- `configuration`: relatively stable parameters and limits;
- `derived`: calculated health, predictions, classifications, or simulation
  outputs.

Internally they are ordinary facts with predicates such as
`twin.reported.temperature`. Every write retains source identity, event time,
ingestion time, validity, confidence, quality, unit, sequence, and raw payload.
Late and corrected telemetry therefore does not rewrite history.

## Register and update a twin

```http
POST /twins
Content-Type: application/json

{
  "kind": "manufacturing.centrifugal_pump",
  "name": "Cooling Pump 7",
  "model_id": "dtmi:example:pump;1",
  "schema_version": "1",
  "capabilities": ["telemetry", "commands", "simulation"],
  "external_ids": {
    "aas": "urn:example:asset:pump-7",
    "opcua": "ns=2;s=Pump7"
  }
}
```

```http
POST /twins/digital-twin%3Acooling-pump-7/telemetry
Content-Type: application/json

{
  "signal": "bearing.temperature",
  "value": 81.5,
  "value_type": "float",
  "unit": "Cel",
  "quality": "good",
  "sequence": "18442",
  "observed_at": "2026-10-11T09:00:00Z",
  "ingested_at": "2026-10-11T09:00:01Z",
  "adapter_id": "opcua-line-1",
  "protocol": "opcua"
}
```

Desired state uses the same value contract:

```http
POST /twins/digital-twin%3Acooling-pump-7/desired

{
  "signal": "bearing.temperature",
  "value": 75.0,
  "value_type": "float",
  "unit": "Cel",
  "observed_at": "2026-10-11T09:00:02Z",
  "adapter_id": "controller-1",
  "protocol": "agent"
}
```

## Reconstruct state and detect drift

```http
GET /twins/digital-twin%3Acooling-pump-7/state
GET /twins/digital-twin%3Acooling-pump-7/state?valid_at=2026-10-11T09%3A00%3A00Z&known_at=2026-10-11T09%3A05%3A00Z
```

The response separates the four channels and reports every desired property
whose reported value differs or is absent. `valid_at` asks what held in the
modeled world; `known_at` asks what the database believed at that time.

## Topology and composition

Any twin can be linked to another twin with a typed, temporal relationship:

```http
POST /twins/digital-twin%3Acooling-pump-7/relationships

{
  "type": "part_of",
  "target": "digital-twin:cooling-loop-a",
  "valid_from": "2026-01-01T00:00:00Z"
}
```

Typical relationships include `part_of`, `contains`, `depends_on`,
`connected_to`, `located_in`, `feeds`, `controls`, `represents`, and
`simulates`. They are not hard-coded; ontology definitions can add domain/range,
cardinality, acyclicity, or connectivity constraints. Existing temporal graph
and blast-radius queries operate on the same links.

## Commands

Commands are durable events rather than direct device I/O. An adapter claims a
request, performs the real-world operation, and posts lifecycle acknowledgments.
The database remains the audit/control plane and never pretends an emitted
command succeeded before acknowledgement.

```http
POST /twins/digital-twin%3Acooling-pump-7/commands

{
  "command_type": "set_speed",
  "requested_by": "agent:operator",
  "requested_at": "2026-10-11T09:01:00Z",
  "expires_at": "2026-10-11T09:02:00Z",
  "idempotency_key": "work-order-42-step-3",
  "parameters": { "rpm": 1500 }
}
```

```http
POST /twins/digital-twin%3Acooling-pump-7/commands/event%3A1/ack

{
  "status": "succeeded",
  "at": "2026-10-11T09:01:03Z",
  "message": "Drive acknowledged target speed"
}
```

Supported states are `accepted`, `running`, `succeeded`, `failed`, `rejected`,
and `cancelled`. Replaying an identical idempotency key returns the original
command. Reusing the key for a different command is rejected.

## Interoperability boundary

The core does not link protocol SDKs. Adapters translate external envelopes to
the REST or Rust twin contract while keeping protocol metadata:

| Ecosystem | Mapping |
|---|---|
| Azure DTDL | interface/model ID to `model_id`; telemetry/property to signal; command to twin command |
| Asset Administration Shell | asset/global IDs to `external_ids`; submodels to configuration and reported state |
| NGSI-LD | entity type/ID to kind/external ID; properties to signals; relationships to twin links |
| OPC UA | NodeId to external ID; variables to telemetry; methods to commands |
| MQTT/Sparkplug | topic/device identity to adapter and external ID; metrics to telemetry |
| OTLP | resource identity to twin; metric/log/span attributes to observations and events |
| simulation/FMU | model URI to `model_id`; parameters to configuration; outputs to derived state |

Adapters may be separate Apache-2.0/MIT processes. This avoids forcing all
protocol dependencies into the database and preserves the project's dependency
license policy.

## V0 boundary

The implementation supplies twin semantics, persistence, historical
reconstruction, topology, drift, and command auditability. It does not yet
provide a high-rate streaming transport, edge/offline synchronization, spatial
geometry engine, FMI runtime, automatic protocol discovery, tenant isolation,
or a device-security perimeter. Device commands must pass through an
authenticated, policy-enforcing adapter; do not expose this V0 server directly
to operational networks.

