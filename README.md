# esame_sql

Questo repository contiene il progetto per l'esame di SQL, focalizzato sulla gestione di un database relazionale tramite PostgreSQL containerizzato con Docker.

## Struttura del Progetto

```text
esame_sql/
├── compose_e_normalizzazione/
│   ├── docker-compose.yml  # Copia per la correzione
│   └── diagramma_er.png    # Generato da Beekeeper
├── postgres/
│   └── [Dati del container e DB]
├── analytical_query.sql    # CTE complessa per analisi reparti e orari di punta
├── app.py                  # Interfaccia Streamlit per la visualizzazione dei dati
├── create_table.py         # Script per la creazione tabelle
├── load.py                 # Script per caricamento dati
├── queries.py              # Raccolta di query SQL
├── requirements.txt        # Dipendenze Python
├── instacart.csv           # Dataset originale
└── README.md               # Documentazione del progetto
```

## Tecnologie Utilizzate

- **PostgreSQL**: Database relazionale per la persistenza dei dati.
- **Docker**: Per la containerizzazione e l'avvio rapido dell'ambiente.
- **Beekeeper Studio / pgAdmin**: Strumenti GUI per la visualizzazione e gestione locale del database.
- **Streamlit**: Framework Python utilizzato nel file `app.py` per la renderizzazione grafica e la visualizzazione dei dati.
- **Pandas**: Libreria utilizzata per la pulizia e la manipolazione del dataset CSV.
- **Psycopg2**: Adapter PostgreSQL per Python utilizzato per la connessione, la gestione delle query e il caricamento dei dati nelle tabelle.

## Logica del Main
Il file principale coordina l'intero workflow. Se il database non è ancora popolato, richiama le funzioni di inizializzazione e caricamento; in caso contrario, queste possono essere commentate per velocizzare l'avvio:
```python
from create_table import create_database
from load import load_csv
import sys
import subprocess

#richiamo le funzioni per la creazione del db e caricamento dati
#(Se il db è già caricato commenta le due funzioni seguenti)
create_database()
load_csv()

#esegue automaticamente l'interfaccia streamlit
subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
```

## Creazione Schema (create_table.py)
Lo script `create_table.py` automatizza la creazione del database relazionale. Si connette a PostgreSQL tramite `psycopg2` e definisce lo schema in **3NF** (Terza Forma Normale), includendo:
- Tabelle per `users`, `departments`, `aisles`, `products`, `orders` e `order_details`.
- Vincoli di integrità referenziale (`FOREIGN KEY`) con `ON DELETE CASCADE`.
- Vincoli di controllo (`CHECK`) per validare orari, giorni e quantità.

## Funzionamento Query Analitica
Il file `analytical_query.sql` utilizza delle Common Table Expressions (CTE) per un'analisi avanzata:
1. **Riepilogo Reparti**: Il primo blocco crea un riassunto delle vendite generali per ogni singolo reparto, contando le vendite totali e calcolando la percentuale di riordino.
2. **Orari di Punta**: Il secondo blocco serve per capire quando le persone comprano di più, raggruppando le vendite per ora del giorno e creando una classifica basata sul volume.
3. **Unione Dati**: Nel terzo blocco si fondono i dati:
   - Prende il riassunto delle vendite dal primo blocco.
   - Incastra i dati con la classifica delle vendite, filtrando per prendere solo la posizione numero 1 (l'orario con più vendite assoluto).
   - Ordina i risultati finali dal reparto che ha venduto di più a quello che ha venduto di meno.

## Gestione Query (queries.py)
Il file `queries.py` funge da repository centrale per tutte le interazioni SQL dell'applicazione, permettendo una gestione pulita e modulare:
- **Analisi e Statistiche**: Include query per calcolare metriche principali (ordini totali, tasso di riordino, frequenza acquisti) e distribuzioni temporali (ordini per ora e per giorno).
- **Ricerca Avanzata**: Gestisce la ricerca filtrata dei prodotti per nome, reparto e fasce orarie.
- **Operazioni CRUD**: Contiene tutte le istruzioni per l'inserimento, la modifica e l'eliminazione di prodotti, corsie e reparti.
- **Manutenzione Sequenze**: Include la logica per sincronizzare i contatori degli ID (`setval`) con il valore massimo presente in tabella, garantendo la coerenza dei dati dopo importazioni massive.
- **Integrità Dati**: Implementa controlli preventivi, come il conteggio dei prodotti collegati prima dell'eliminazione di una categoria.

## Gestione del Database
Il database è gestito localmente. Per visualizzare i dati e la struttura delle tabelle, è possibile collegarsi all'istanza PostgreSQL tramite client SQL come **pgAdmin** o **Beekeeper Studio** utilizzando le credenziali definite nel file di configurazione Docker.

## Istruzioni Generali

### 1. Configurazione e Avvio
Assicurati di avere Docker installato. Prima di procedere, configura il file `.env` nella root del progetto con le credenziali necessarie (`HOST`, `PORT`, `DATABASE_NAME`, `USER`, `PASSWORD`).

Quindi esegui:
```bash
docker-compose up -d
```

### 2. Esecuzione del Progetto
Per inizializzare il database, caricare i dati e avviare l'interfaccia grafica, è sufficiente eseguire il file principale:
```bash
python main.py
```
