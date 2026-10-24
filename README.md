# Usługa przetwarzania wiadomości Kafka w partiach

Usługa oparta na Javie 21 i Spring Boot, która publikuje wersjonowane zdarzenia (Event) w formacie JSON przez REST, przetwarza rekordy Kafka w partiach i udostępnia poprawnie przetworzone wiadomości (Processed message) zapisane w PostgreSQL.

## Uruchomienie lokalne

Docker Compose uruchamia aplikację, Kafka i PostgreSQL:

```bash
docker compose up --build
```

API jest dostępne pod adresem `http://localhost:8080`. Ustaw `JWT_ISSUER_URI` i `JWT_JWK_SET_URI` zgodnie z konfiguracją dostawcy OAuth2/OIDC, który wystawia tokeny z uprawnieniami `messages.write` i `messages.read`. Zapisz odpowiedni token w zmiennej środowiskowej `TOKEN`, a następnie opublikuj zdarzenie z UUID wygenerowanym przez klienta:

```bash
curl -i -X POST http://localhost:8080/api/v1/events \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"d3d7d41a-6a9c-49d7-8f5e-4c6a2f2f0c01","type":"customer.created","schemaVersion":"1","occurredAt":"2026-10-23T12:00:00Z","payload":{"customerId":"cust-42"}}'
```

Publikacja zwraca `202 Accepted` po potwierdzeniu przyjęcia rekordu przez Kafka. Przetworzone zdarzenia można pobrać przez `GET /api/v1/messages?page=0&size=50`; wyniki są uporządkowane od najnowszych. Odczyt wymaga tokenu z uprawnieniem `messages.read`. Publiczne endpointy stanu aplikacji i sond są dostępne pod adresami `/actuator/health`, `/actuator/health/liveness` i `/actuator/health/readiness`.

Compose używa hasła PostgreSQL `local_dev_only`, przeznaczonego wyłącznie do lokalnego środowiska developerskiego. Możesz je zmienić za pomocą `DATABASE_PASSWORD`. Nie używaj przykładowego hasła w innych środowiskach.

### Lokalna atrapa JWT

Do ręcznego testowania API bez dostawcy OAuth2/OIDC włącz profil Springa `local`. W konfiguracji uruchomienia aplikacji w IntelliJ ustaw **Active profiles** na `local` lub dodaj zmienną środowiskową `SPRING_PROFILES_ACTIVE=local`. Ustaw `DATABASE_PASSWORD` na hasło lokalnej instancji PostgreSQL; jeśli korzystasz z Compose, użyj hasła skonfigurowanego dla tej instancji. Możesz też uruchomić aplikację poniższym poleceniem, korzystając z Javy 21 i ustawionej zmiennej środowiskowej `DATABASE_PASSWORD`:

```bash
SPRING_PROFILES_ACTIVE=local mvn spring-boot:run
```

Ten profil akceptuje stały token Bearer `local-dev-token` jako użytkownika `local-developer` z uprawnieniami `messages.read` i `messages.write`:

```bash
export TOKEN=local-dev-token
curl -i http://localhost:8080/api/v1/messages \
  -H "Authorization: Bearer $TOKEN"
```

Przykład publikacji podany powyżej działa z tym samym tokenem. Brak tokenu lub inny token powoduje odpowiedź `401 Unauthorized`. Lokalny token jest atrapą i nie zawiera podpisu JWT. Dekoder testowy nie łączy się ze skonfigurowanym dostawcą tożsamości ani endpointem JWK. Włączaj ten profil wyłącznie podczas lokalnego developmentu. Bez profilu `local` aplikacja korzysta ze standardowej walidacji JWT. Kafka i PostgreSQL muszą być uruchomione również przy korzystaniu z atrapy.

## Konfiguracja

Wartości domyślne znajdują się w `src/main/resources/application.yml`. Zmienne środowiskowe pozwalają zmienić adres brokera Kafka, temat, grupę konsumentów, rozmiar partii, limit czasu oczekiwania na potwierdzenie, ustawienia ponowień i DLQ, adres i dane dostępowe PostgreSQL, dostawcę JWT i endpoint JWK oraz okres retencji przetworzonych wiadomości. `DATABASE_PASSWORD` jest wymagane i nie ma wartości domyślnej w aplikacji: lokalnie dostarcza je Compose, a Kubernetes odczytuje je ze wskazanego zasobu Secret. Wartości są odczytywane podczas startu aplikacji, dzięki czemu konfigurację środowiska można zmieniać bez ponownego budowania obrazu.

PostgreSQL zaokrągla `occurredAt` do mikrosekund w modelu odczytu. Skróty identyfikujące treść zdarzeń zachowują oryginalne nanosekundy, dlatego identyczne ponowienia są akceptowane, a zmiana oryginalnego znacznika czasu nadal powoduje konflikt. Wartości Kafka, których nie można zdeserializować, trafiają bezpośrednio do tematu DLQ z `event: null`, oryginalnym kluczem w `rawRecord.key` i oryginalnymi bajtami zakodowanymi w Base64 w `rawRecord.value`. Metadane błędu zawierają źródłowy temat, partycję i offset. Rekord usunięcia Kafka (tombstone) ma surową wartość `null`. Pozostałe zdarzenia w partii są dalej przetwarzane; partia jest potwierdzana dopiero po trwałym zapisaniu wyniku każdego zdarzenia lub potwierdzeniu jego publikacji do DLQ.

## Wdrożenie na Kubernetes

Manifesty Kubernetes wdrażają wyłącznie aplikację. Kafka i PostgreSQL muszą być przygotowane osobno i dostępne z klastra. Przykładowy ConfigMap zawiera adresy usług i dostawcy tożsamości, które należy zastąpić właściwymi wartościami. Dostosuj `k8s/configmap.yaml` do docelowego środowiska, a następnie zbuduj obraz i opublikuj go w rejestrze lub załaduj do klastra:

```bash
docker build -t message-service:local .
```

Przed wdrożeniem aplikacji utwórz zasób Secret wskazany w Deployment, korzystając z wartości z menedżera sekretów. Nie zapisuj danych dostępowych w repozytorium:

```bash
kubectl create secret generic message-service-secrets \
  --from-literal=DATABASE_USERNAME='<database-user>' \
  --from-literal=DATABASE_PASSWORD='<database-password>'
```

Zastosuj konfigurację bez sekretów oraz zasoby aplikacji:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml -f k8s/service.yaml
kubectl rollout status deployment/message-service
```

Deployment korzysta z publicznych grup stanu readiness i liveness. Kafka i PostgreSQL są zewnętrznymi zależnościami aplikacji i nie mają manifestów wdrożeniowych w tym projekcie. Po zmianie wartości w ConfigMap zastosuj konfigurację i zrestartuj Deployment, aby aplikacja odczytała nowe zmienne środowiskowe:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl rollout restart deployment/message-service
```

## Testy i pokrycie kodu

Uruchom pełny smoke test lokalnego środowiska Compose:

```bash
bash scripts/smoke-test.sh
```

Skrypt buduje osobne środowisko, sprawdza stan aplikacji i sondy, uwierzytelnianie, publikację, przetwarzanie, idempotencję i konflikty, a następnie usuwa utworzone kontenery i wolumeny. Użyj `--keep`, aby zachować środowisko. [Instrukcja smoke testu](docs/smoke-test.md) opisuje wymagania, opcje, oczekiwane wyniki oraz pełną procedurę ręcznego wykonania wszystkich kroków.

Testy uruchomisz poleceniem `mvn test`. Zestaw obejmuje testy jednostkowe oraz testy integracyjne korzystające z Testcontainers, które wymagają Dockera. Pełna weryfikacja generuje również lokalny raport JaCoCo w formacie HTML:

```bash
mvn verify
```

Raport jest zapisywany w `target/site/jacoco/index.html`. Proces budowania nie wysyła go do zewnętrznych serwisów.
