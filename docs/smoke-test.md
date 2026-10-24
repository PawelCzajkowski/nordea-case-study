# Smoke test lokalnego środowiska

Ta procedura sprawdza gotowy obraz aplikacji uruchomiony razem z Kafka i PostgreSQL przez Docker Compose. Event przechodzi przez HTTP → Kafka → konsument batchowy → PostgreSQL → HTTP. Test obejmuje również publiczne sondy, odrzucanie niepoprawnego uwierzytelnienia, idempotencję i konflikt treści.

Profil `local` akceptuje token `local-dev-token` z uprawnieniami `messages.read` i `messages.write`. Służy do lokalnego testu bez zewnętrznego dostawcy OIDC. Weryfikację podpisów i konfiguracji rzeczywistego dostawcy tożsamości należy przeprowadzić oddzielnie. Ten smoke test nie wymusza awarii DLQ ani wygaśnięcia retencji; te scenariusze mają osobne testy automatyczne.

## Wymagania

- Uruchomiony Docker Desktop lub Docker Engine z Docker Compose **2.24.4 lub nowszym**. Plik nadpisujący używa znaczników `!reset` i `!override`, opisanych w [dokumentacji Docker Compose](https://docs.docker.com/reference/compose-file/merge/).
- Bash 3.2 lub nowszy, `curl`, `jq` oraz `uuidgen`.
- Dostęp do rejestru obrazów i repozytorium zależności Maven przy pierwszej kompilacji.
- Wystarczające zasoby Dockera do jednoczesnego uruchomienia aplikacji, Kafka i PostgreSQL.

Java i Maven na hoście nie są wymagane: Dockerfile kompiluje aplikację na Javie 21 i uruchamia ją na JRE 21.

## Uruchomienie skryptu

W katalogu głównym repozytorium wykonaj:

```bash
bash scripts/smoke-test.sh
```

Skrypt tworzy projekt Compose o unikalnej nazwie `nordea-smoke-...`, buduje obraz aplikacji i przydziela jej wolny port na `127.0.0.1`. PostgreSQL i Kafka są dostępne dla aplikacji w sieci tego projektu. Dzięki plikowi `docker-compose.smoke.yml` ich porty nie są publikowane na hoście. Możesz więc pozostawić uruchomione swoje zwykłe środowisko developerskie.

Po sukcesie zobaczysz `SMOKE TEST PASSED`, a kod zakończenia wyniesie `0`. Błąd daje niezerowy kod zakończenia, opis nieudanego kroku, stan kontenerów oraz ostatnie logi usług. Skrypt usuwa utworzone kontenery, sieć i wolumeny również po błędzie albo obsłużonym przerwaniu sygnałem INT/TERM. Obrazy i cache budowania pozostają w Dockerze.

Aby zachować środowisko po teście, użyj:

```bash
bash scripts/smoke-test.sh --keep
```

Skrypt wypisze URL aplikacji, nazwę projektu i kompletne polecenie późniejszego sprzątania. `--keep` działa również przy niepowodzeniu, co pozwala obejrzeć stan aplikacji i bazy.

Możesz zmienić port i limity czasu:

```bash
SMOKE_PORT=18080 SMOKE_STARTUP_TIMEOUT=240 SMOKE_PROCESS_TIMEOUT=90 \
  bash scripts/smoke-test.sh --keep
```

| Zmienna | Domyślnie | Znaczenie |
| --- | --- | --- |
| `SMOKE_PORT` | `0` | Port HTTP na hoście; `0` oznacza automatyczny przydział. |
| `SMOKE_STARTUP_TIMEOUT` | `180` | Limit w sekundach dla gotowości usług Compose oraz, osobno, dla każdego endpointu health. |
| `SMOKE_PROCESS_TIMEOUT` | `60` | Limit oczekiwania na Event w API odczytu. |
| `SMOKE_HTTP_TIMEOUT` | `15` | Limit pojedynczego żądania HTTP. |

Budowanie obrazu i pobieranie zależności odbywa się przed oczekiwaniem na gotowość kontenerów i nie jest objęte `SMOKE_STARTUP_TIMEOUT`.

## Ręczne wykonanie krok po kroku

Wykonuj poniższe polecenia w **jednej sesji Bash**, zaczynając w katalogu głównym repozytorium. Zmienne z wcześniejszych kroków są używane dalej. Na macOS możesz wejść do Bash poleceniem `bash`, jeśli korzystasz na co dzień z Zsh.

### 1. Utwórz osobne środowisko Compose

Sprawdź narzędzia i dostępność Dockera:

```bash
docker compose version
docker info >/dev/null
command -v curl jq uuidgen
```

Każde polecenie powinno zakończyć się poprawnie. Następnie przygotuj unikalną nazwę projektu i wspólne argumenty Compose:

```bash
export SMOKE_PROJECT_NAME="nordea-smoke-manual-$(date -u +%Y%m%d%H%M%S)-$$"
export SMOKE_PORT=0
smoke_compose=(docker compose --project-name "$SMOKE_PROJECT_NAME"
  --file docker-compose.yml --file docker-compose.smoke.yml)

"${smoke_compose[@]}" config --quiet
"${smoke_compose[@]}" up --detach --build --wait --wait-timeout 180
"${smoke_compose[@]}" ps
```

`config --quiet` sprawdza połączoną konfigurację. `up` buduje obraz i uruchamia wszystkie trzy usługi. PostgreSQL i Kafka powinny mieć stan `healthy`, a aplikacja `running`. Aplikacja nie ma kontenerowego healthchecka, więc jej endpointy sprawdzisz w następnym kroku.

Ustal rzeczywisty port API:

```bash
smoke_base_url="http://$("${smoke_compose[@]}" port app 8080)"
printf 'API: %s\nProjekt: %s\n' "$smoke_base_url" "$SMOKE_PROJECT_NAME"
```

Otrzymasz adres podobny do `http://127.0.0.1:49152`. Używaj wypisanego portu, ponieważ może być inny przy każdym uruchomieniu.

### 2. Sprawdź health, liveness i readiness

```bash
for smoke_path in /actuator/health /actuator/health/liveness /actuator/health/readiness; do
  printf '\n%s\n' "$smoke_path"
  curl --silent --show-error --retry 30 --retry-delay 2 --retry-connrefused \
    --retry-max-time 180 --max-time 5 --include "$smoke_base_url$smoke_path"
done
```

Każdy endpoint powinien odpowiedzieć **HTTP 200** i treścią:

```json
{"status":"UP"}
```

- `/actuator/health` pokazuje zbiorczy stan skonfigurowanych wskaźników, w tym połączenia z bazą.
- `/actuator/health/liveness` odpowiada sondzie sprawdzającej stan życia aplikacji.
- `/actuator/health/readiness` odpowiada sondzie sprawdzającej gotowość aplikacji do obsługi ruchu.

Te endpointy są publiczne. Domyślna grupa readiness nie sprawdza dostępności Kafka ani PostgreSQL. Pełny przepływ sprawdzany w kolejnych krokach potwierdzi dostępność obu zależności. Jeśli którykolwiek endpoint po ponowieniach nadal nie zwraca HTTP 200 i `UP`, zatrzymaj procedurę i sprawdź logi opisane na końcu instrukcji.

### 3. Sprawdź odrzucanie braku tokenu i błędnego tokenu

```bash
curl --silent --show-error --max-time 15 --include \
  "$smoke_base_url/api/v1/messages"

curl --silent --show-error --max-time 15 --include \
  --header 'Authorization: Bearer invalid-smoke-token' \
  "$smoke_base_url/api/v1/messages"
```

Oba żądania powinny zwrócić **HTTP 401 Unauthorized**. Publiczne sondy nie oznaczają publicznego dostępu do API wiadomości.

### 4. Przygotuj i opublikuj Event

Wygeneruj nowy UUID i treść Eventu:

```bash
smoke_message_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
smoke_timestamp="$(date -u +%Y-%m-%dT%H:%M:%S)"
smoke_event="$(jq -n --arg id "$smoke_message_id" \
  --arg occurredAt "$smoke_timestamp.123456789Z" \
  '{messageId: $id, type: "smoke.created", schemaVersion: "1", occurredAt: $occurredAt,
    payload: {customerId: ("smoke-" + $id)}}')"
printf '%s\n' "$smoke_event" | jq .
```

Nowy UUID pozwala powtarzać procedurę. Celowo używamy nanosekund w `occurredAt`, aby sprawdzić również poprawność obsługi precyzji czasu w PostgreSQL.

Najpierw spróbuj publikacji bez uwierzytelnienia:

```bash
curl --silent --show-error --max-time 15 --include \
  --header 'Content-Type: application/json' --data-binary "$smoke_event" \
  "$smoke_base_url/api/v1/events"
```

Oczekiwane: **HTTP 401**. Następnie opublikuj ten sam Event z lokalnym tokenem:

```bash
curl --silent --show-error --max-time 15 --include \
  --header 'Authorization: Bearer local-dev-token' \
  --header 'Content-Type: application/json' --data-binary "$smoke_event" \
  "$smoke_base_url/api/v1/events"
```

Oczekiwane: **HTTP 202 Accepted** i odpowiedź `{"messageId":"..."}` z dokładnie tym UUID, który zawiera `$smoke_message_id`. HTTP 202 oznacza potwierdzenie przyjęcia do Kafka; przetwarzanie i zapis do bazy odbywają się asynchronicznie.

### 5. Poczekaj na przetworzenie i odczytaj Event

Poniższa pętla wykonuje odczyt do momentu znalezienia Eventu, maksymalnie przez około 60 sekund plus czas ostatniego żądania:

```bash
smoke_deadline=$((SECONDS + 60))
while true; do
  if smoke_read="$(curl --fail --silent --show-error --connect-timeout 3 --max-time 15 \
      --header 'Authorization: Bearer local-dev-token' \
      "$smoke_base_url/api/v1/messages?page=0&size=100")" &&
      jq -e --arg id "$smoke_message_id" 'any(.content[]; .messageId == $id)' \
        >/dev/null 2>&1 <<<"$smoke_read"; then
    printf '%s\n' "$smoke_read" | jq .
    break
  fi
  if (( SECONDS >= smoke_deadline )); then
    printf 'FAIL: Event nie pojawił się w API w wymaganym czasie\n' >&2
    break
  fi
  sleep 1
done
```

Jeśli zobaczysz `FAIL`, zakończ sprawdzanie i przejdź do diagnostyki. Prawidłowy wynik powinien zawierać:

- `totalElements: 1`, `page: 0`, `size: 100` i jeden element `content` — to nowa, osobna baza testowa.
- `messageId` zgodny z `$smoke_message_id`.
- `type: "smoke.created"`, `schemaVersion: "1"` i oryginalny `payload.customerId`.
- `occurredAt` z końcówką `.123457Z`, ponieważ PostgreSQL zaokrągla nanosekundy do mikrosekund.
- Uzupełnione `receivedAt`, oznaczające czas przyjęcia do read modelu.

Pojawienie się Eventu w API potwierdza przepływ przez Kafka i zapis w PostgreSQL.

### 6. Sprawdź idempotencję i trwały zapis

Wyślij ponownie **identyczną** treść:

```bash
curl --silent --show-error --max-time 15 --include \
  --header 'Authorization: Bearer local-dev-token' \
  --header 'Content-Type: application/json' --data-binary "$smoke_event" \
  "$smoke_base_url/api/v1/events"

curl --fail --silent --show-error --max-time 15 \
  --header 'Authorization: Bearer local-dev-token' \
  "$smoke_base_url/api/v1/messages?page=0&size=100" | jq .
```

Publikacja powinna ponownie zwrócić **HTTP 202** z tym samym `messageId`. Odczyt powinien nadal pokazywać jeden Processed message, `totalElements: 1` i oryginalną treść.

Sprawdź również bezpośrednio liczbę zapisów w testowym PostgreSQL:

```bash
"${smoke_compose[@]}" exec -T postgres psql -X -U message_service -d messages -Atc \
  "SELECT count(*) FROM processed_messages WHERE message_id = '$smoke_message_id';"
```

Oczekiwany wynik: **`1`**. Polecenie wykonuje wyłącznie odczyt i nie wymaga publikowania portu bazy na hoście.

### 7. Sprawdź konflikt treści pod tym samym UUID

Zmień `customerId`, pozostawiając ten sam `messageId`:

```bash
smoke_conflict="$(jq '.payload.customerId = "smoke-conflict"' <<<"$smoke_event")"
curl --silent --show-error --max-time 15 --include \
  --header 'Authorization: Bearer local-dev-token' \
  --header 'Content-Type: application/json' --data-binary "$smoke_conflict" \
  "$smoke_base_url/api/v1/events"

curl --fail --silent --show-error --max-time 15 \
  --header 'Authorization: Bearer local-dev-token' \
  "$smoke_base_url/api/v1/messages?page=0&size=100" | jq .
```

Publikacja powinna zwrócić **HTTP 409 Conflict**. Odczyt powinien nadal pokazywać pojedynczy Event z oryginalnym `customerId` równym `smoke-<UUID>`. Zmieniona treść nie powinna nadpisać zapisanego Eventu.

### 8. Usuń utworzone środowisko

Po sprawdzeniu wszystkich wyników zatrzymaj projekt i usuń jego kontenery, sieć oraz wolumeny:

```bash
"${smoke_compose[@]}" down --volumes --remove-orphans
"${smoke_compose[@]}" ps --all
```

Końcowa lista kontenerów powinna być pusta. Używaj tej samej nazwy projektu, którą ustawiłeś w kroku 1. Wolumeny tego projektu zawierają wyłącznie dane bieżącego smoke testu; `down --volumes` usuwa te dane.

## Diagnostyka niepowodzeń

Przed ręcznym sprzątaniem obejrzyj stan i logi:

```bash
"${smoke_compose[@]}" ps --all
"${smoke_compose[@]}" logs --no-color --tail=100 app kafka postgres
```

| Objaw | Co sprawdzić |
| --- | --- |
| Błąd `!override` lub `!reset` | Wersję Docker Compose; wymagane jest co najmniej 2.24.4. |
| Brak połączenia z Dockerem | Czy Docker Desktop lub daemon jest uruchomiony. |
| Błąd pobierania obrazu lub zależności | Połączenie z rejestrem obrazów i repozytorium Maven; log budowania. |
| Zajęty port | Użyj `SMOKE_PORT=0`, aby Docker wybrał wolny port. |
| Health pozostaje `DOWN` | Logi startu, migracje Flyway i połączenie do PostgreSQL. |
| HTTP 401 z lokalnym tokenem | Czy używasz URL testowego projektu i czy aplikacja wystartowała z profilem `local`. |
| Publikacja zwraca HTTP 503 | Dostępność Kafka i logi producenta. |
| HTTP 202, ale brak Eventu w odczycie | Logi konsumenta, bazy i publikacji do DLQ. |
| Po ponowieniu pojawiają się duplikaty lub konflikt | Czy wysłano identyczną treść i czy UUID nie został wcześniej użyty dla innego Eventu. |

Przy automatycznym uruchomieniu użyj `--keep`, jeśli potrzebujesz obejrzeć środowisko po zakończeniu skryptu. Jeżeli terminal z ręcznej procedury został zamknięty, odtwórz zmienną `SMOKE_PROJECT_NAME` z wcześniej wypisanej nazwy i tablicę `smoke_compose` z kroku 1; projekt pozostaje uruchomiony do wykonania `down`.
