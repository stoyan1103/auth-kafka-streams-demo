# Auth Kafka Streams Demo

A Java and Spring Boot learning project exploring **JWT authentication, role-based access control, event-driven session management, and Apache Kafka Streams**. The application exposes a documented REST API and uses Kafka-based components to explore how authentication state can be represented and queried in an event-driven system.

> **Project scope:** This is an educational demonstration, not a production-ready identity provider. Do not expose it publicly without reviewing security, persistence, credential handling, and deployment configuration.

## Highlights

- User registration and JWT-based login.
- Authenticated logout with token invalidation.
- `USER` and `ADMIN` authorization rules.
- User profile lookup: the account owner or an admin can access a profile.
- Admin-only paginated user listing.
- Kafka-based token-session events and Kafka Streams stateful processing as a learning exercise.
- Contract-first REST API using OpenAPI-generated interfaces and models.
- Multi-module Maven architecture with separate API, event contracts, infrastructure, and application modules.
- Local, three-broker Kafka cluster with AKHQ for topic and message inspection.

## Technology stack

| Area | Technology |
| --- | --- |
| Language | Java 25 |
| Framework | Spring Boot 3.5.14, Spring Security |
| Authentication | JWT (JJWT) |
| Streaming | Apache Kafka 3.7.0, Kafka Streams |
| Contracts | OpenAPI Generator, Protocol Buffers 3.25.9 |
| Build | Maven 3.9+ |
| Infrastructure | Docker Compose, Apache Kafka (KRaft), AKHQ |

## Project structure

```text
auth-kafka-streams-demo/
├── application/          # Executable Spring Boot application
├── rest-contract/        # OpenAPI specification and generated REST contracts
├── proto-contract/       # Protocol Buffers message contracts
├── kafka-contract/       # Shared Kafka-facing contracts
├── kafka-topic-creator/  # Separate Kafka topic-provisioning module
├── common/               # Common/shared code
├── scripts/              # Helper scripts
├── docker-compose.yml    # Three Kafka brokers and AKHQ
└── pom.xml               # Maven parent and dependency management
```

The module boundaries are intended to keep external contracts and infrastructure concerns separate from application behavior. In particular, the REST contract is generated from `rest-contract/src/main/resources/openapi/auth-api.yaml` during Maven's `generate-sources` phase, rather than being maintained as an unrelated hand-written API interface.

## How the system works

### HTTP and security flow

1. A client registers through the public registration endpoint.
2. A client logs in with a username and password and receives a bearer access token.
3. For protected endpoints, the client sends the token in the `Authorization: Bearer <token>` header.
4. Spring Security authenticates the request and applies role- and ownership-based authorization.
5. Logging out invalidates the currently active token.

### Event-driven session flow

The demo explores publishing token-session changes as Kafka events and building/querying a stateful view with Kafka Streams. This separates the *command side* (registration, login, logout) from the *derived session state* that can be reconstructed from the event stream. It is a learning-oriented example of event-driven state management, not a claim that signed JWTs alone are revocable without server-side coordination.

Conceptually:

```text
REST client
   |
   v
Spring Boot REST API + Spring Security
   |                     |
   | register/login      | authenticated requests
   v                     v
Authentication services / user model
   |
   v
Token-session events -> Kafka topics -> Kafka Streams processing
                                            |
                                            v
                                       Session view
```

**Implementation note:** The complete internal topology (processor names, store names, exact event schemas, and topic-provisioning command) should be checked against the Java sources before documenting those internals as a definitive execution trace.

## Prerequisites

- JDK **25 or later** (`java -version`).
- Apache Maven **3.9 or later** (`mvn -version`).
- Docker Desktop / Docker Engine with Docker Compose.
- `curl` (or an API client such as Postman).
- A locally generated strong secret for signing JWTs.

The root Maven build enforces the Java and Maven versions. Kafka uses three local external listeners at ports **19092**, **29092**, and **39092**; AKHQ is exposed on **8081**.

## Local setup

### 1. Clone the repository

```bash
git clone https://github.com/stoyan1103/auth-kafka-streams-demo.git
cd auth-kafka-streams-demo
```

### 2. Start the local Kafka infrastructure

```bash
docker compose up -d
```

Inspect the running containers:

```bash
docker compose ps
```

AKHQ: <http://localhost:8081>

Local Kafka bootstrap servers:

```text
localhost:19092,localhost:29092,localhost:39092
```

> Kafka automatic topic creation is disabled in `docker-compose.yml`. The repository includes a `kafka-topic-creator` module. **Before exercising event-driven flows**, ensure the application topics have been provisioned using that module. The exact command/entry point needs validation against its source code.

### 3. Configure the JWT signing secret

On macOS/Linux (Bash):

```bash
export JWT_SECRET="$(openssl rand -base64 48)"
```

On PowerShell, set the `JWT_SECRET` environment variable to a strong, locally generated value before launching the application. Do not commit the secret to the repository.

Optional configuration, as declared in `application/src/main/resources/application.properties`:

```text
JWT_EXPIRATION_SECONDS=3600
KAFKA_BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
```

### 4. Build the multi-module project

```bash
mvn clean install
```

This command resolves/builds module dependencies and runs the configured Maven lifecycle. Build success depends on having the required JDK and dependencies available.

### 5. Run the application

After installing the modules locally, launch the Spring Boot application from its module:

```bash
mvn -f application/pom.xml spring-boot:run
```

The OpenAPI specification declares a local API server at <http://localhost:8080>. If available in the running configuration, the Springdoc UI is usually served at <http://localhost:8080/swagger-ui/index.html>.

> **Run verification:** The steps above are derived from the checked Maven/Compose configuration; an end-to-end execution and topic-creator startup have not been verified in this documentation draft.

## REST API

The endpoint paths below come from the repository's OpenAPI contract.

| Method | Path | Purpose | Access |
| --- | --- | --- | --- |
| `POST` | `/api/v1/auth/register` | Register a user | Public |
| `POST` | `/api/v1/auth/login` | Authenticate and obtain a JWT | Public |
| `POST` | `/api/v1/auth/logout` | Invalidate the current JWT | Authenticated |
| `GET` | `/api/v1/users/{id}` | Retrieve a user profile | Owner or `ADMIN` |
| `GET` | `/api/v1/users?page=0&size=20` | List users with pagination | `ADMIN` |

### Try it with curl

**Register a test user**

```bash
curl -i -X POST http://localhost:8080/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo_user","email":"demo@example.com","password":"ChangeMe123!"}'
```

A successful response is documented as `201 Created` and includes the registered user's ID and account details.

**Log in**

```bash
curl -i -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo_user","password":"ChangeMe123!"}'
```

A successful response contains `accessToken`, `tokenType`, and `expirationInSeconds`. Copy the access token into a shell variable for subsequent requests:

```bash
TOKEN='<paste_access_token_here>'
USER_ID='<paste_user_id_from_registration>'
```

**Read your own profile**

```bash
curl -i "http://localhost:8080/api/v1/users/$USER_ID" \
  -H "Authorization: Bearer $TOKEN"
```

**Log out**

```bash
curl -i -X POST http://localhost:8080/api/v1/auth/logout \
  -H "Authorization: Bearer $TOKEN"
```

The OpenAPI contract documents `204 No Content` on successful logout. Check that the same token is no longer accepted on a protected endpoint.

### Authorization checks

- A `USER` can retrieve their own profile, but not an unrelated user's profile.
- An `ADMIN` can retrieve profiles and list users.
- A missing or invalid bearer token should fail on protected endpoints.
- The `GET /api/v1/users` endpoint supports `page` and `size` query parameters; the contract lists a maximum page size of 100.

The public registration endpoint is **not** documented as a way to grant yourself the `ADMIN` role. To test admin-only routes, use an administrator account created through an explicitly supported development mechanism rather than assuming a public privilege-elevation API exists.

## Kafka and AKHQ checks

1. Open <http://localhost:8081> after Docker Compose is running.
2. Confirm that all three brokers are connected.
3. Verify that the required application topics exist (topic creation is not automatic).
4. Register, log in, and log out through the REST API.
5. Inspect relevant session-event topics for the generated messages.
6. Restart the application and check that its stateful processing behaves as intended. When comparing results, distinguish Kafka changelog/state restoration from in-memory state.

Exact topic names and the state-store query interface should be added after checking the application source and topic creator configuration.

## Maintainability and design decisions

This project demonstrates several useful maintainability practices:

- **Module separation:** executable application, REST contracts, Kafka-related contracts, Protocol Buffers, topic provisioning, and shared utilities are kept in distinct modules.
- **Contract-first API:** OpenAPI is the source for generated Spring REST interfaces and models, reducing divergence between implemented endpoints and public API definitions.
- **Explicit data contracts:** Protocol Buffers are used for a typed serialization contract.
- **Configuration outside source:** the JWT secret and Kafka bootstrap servers are supplied through environment variables with documented defaults where appropriate.
- **Security boundaries:** protected resources are distinguished from public registration/login endpoints, with owner/admin restrictions documented at the API level.
- **Separation of concerns:** authentication commands and event-derived session state can be reasoned about independently.

These are architectural choices visible in the configuration and API contracts, **not** a substitute for an independent review of all implementation classes, tests, and error handling.

## Troubleshooting

| Problem | What to check |
| --- | --- |
| Build rejects the JDK | Root POM requires Java 25+ |
| Build rejects Maven | Root POM requires Maven 3.9+ |
| JWT configuration error on startup | `JWT_SECRET` must be defined |
| Cannot connect to Kafka | `docker compose ps`; ports 19092/29092/39092; bootstrap servers |
| No Kafka events or state updates | Topic provisioning, producer configuration, consumer/Streams startup logs |
| AKHQ unavailable | Container status and host port 8081 |
| `401 Unauthorized` | Bearer token present, valid, unexpired, and not revoked |
| `403 Forbidden` | Owner/ADMIN authorization requirements |

To shut down the local infrastructure:

```bash
docker compose down
```

The Compose setup maps broker data directories onto the host (`./docker-data/kafka/...`); `docker compose down` does not automatically delete those host directories.

## Further improvements

Potential future work for this learning project includes stronger automated tests for the Kafka Streams topology, contract tests, end-to-end integration tests, a dedicated development bootstrap command for topic provisioning, and deployment automation.

## Notes for reviewers

This repository is a personal technical demonstration. It is designed to explore authentication and event-driven design patterns, not to serve as an audited identity platform. Inspect the source and run the project locally before treating any behavior as a security guarantee.
