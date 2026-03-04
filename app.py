# app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
import queries

#.env
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

COLLEGAMENTO_DB_APP_PY = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}"

# Configurazione pagina
st.set_page_config(
    page_title="Instacart Shop",
    layout="wide",
    page_icon="🌶️",
)

@st.cache_resource
def ottieni_motore_database():
    return create_engine(
        COLLEGAMENTO_DB_APP_PY,
        pool_pre_ping=True,
    )


def reimposta_sequenze_id():
    """
    Sincronizza le sequenze SERIAL con MAX(id) reale.
    Necessario dopo ogni import da CSV con IDs espliciti.
    Chiamata ad ogni avvio (non in cache) per sicurezza.
    """
    try:
        with motore_database.begin() as connessione:
            for nome_tabella, nome_colonna, nome_sequenza in [
                ("aisles",      "aisle_id",      "aisles_aisle_id_seq"),
                ("departments", "department_id", "departments_department_id_seq"),
                ("products",    "product_id",    "products_product_id_seq"),
            ]:
                # Sostituito con riferimento da queries.py
                query_imposta_sequenza = queries.QUERY_IMPOSTA_SEQUENZA.format(
                    sequenza=nome_sequenza,
                    colonna=nome_colonna,
                    tabella=nome_tabella
                )
                connessione.execute(text(query_imposta_sequenza))
    except Exception:
        pass

try:
    motore_database = ottieni_motore_database()
    reimposta_sequenze_id()
except Exception as errore_connessione:
    st.error(errore_connessione)
    st.stop()


def leggi_dati(query_sql: str, parametri_query: dict | None = None) -> pd.DataFrame:
    """SELECT DataFrame. AUTOCOMMIT = vede sempre i aggiornati."""
    with motore_database.connect().execution_options(isolation_level="AUTOCOMMIT") as connessione:
        return pd.read_sql_query(text(query_sql), connessione, params=parametri_query or {})


def scrivi_dati(query_sql: str, parametri_query: dict | None = None) -> None:
    """INSERT / UPDATE / DELETE."""
    with motore_database.begin() as connessione:
        connessione.execute(text(query_sql), parametri_query or {})


def inserisci_e_ritorna_id(query_inserimento: str, parametri_query: dict) -> int:
    """INSERT ... RETURNING id → int."""
    with motore_database.begin() as connessione:
        risultato = connessione.execute(text(query_inserimento), parametri_query)
        return risultato.scalar()


def carica_ed_esegui_query_analitica(percorso_file: str) -> pd.DataFrame:
    if not os.path.exists(percorso_file):
        raise FileNotFoundError("File '{percorso}' non trovato.".format(percorso=percorso_file))
    with open(percorso_file, "r", encoding="utf-8") as file_sql:
        query_sql = file_sql.read()
    return leggi_dati(query_sql)


def ottieni_dizionario_corsie() -> dict:
    dataframe_corsie = leggi_dati(queries.QUERY_DIZIONARIO_CORSIE)
    return dict(zip(dataframe_corsie["aisle_id"], dataframe_corsie["aisle"]))

def ottieni_dizionario_reparti() -> dict:
    dataframe_reparti = leggi_dati(queries.QUERY_DIZIONARIO_REPARTI)
    return dict(zip(dataframe_reparti["department_id"], dataframe_reparti["department"]))

ETICHETTE_GIORNI_SETTIMANA = {0:"Dom", 1:"Lun", 2:"Mar", 3:"Mer", 4:"Gio", 5:"Ven", 6:"Sab"}


# Interfaccia Utente
st.title("🛒 Instacart Analytics")
st.caption("**Esplora ordini**: Analisi completa di prodotti e tendenze nel marketplace Instacart ")
st.divider()

schede_interfaccia = st.tabs([
    "Dashboard ordini",
    "Cerca prodotti",
    "Catalogo",
    "Aggiungi prodotto",
    "Modifica prodotto",
    "Query analitica",
])
scheda_dashboard, scheda_ricerca, scheda_catalogo, scheda_aggiungi, scheda_modifica, scheda_analisi = schede_interfaccia


# Dashboard ordini
with scheda_dashboard:
    st.subheader("Panoramica Instacart Marketplace")
    st.markdown("Statistiche aggregate estratte direttamente dal database Instacart.")
    st.divider()

    try:
        colonna_metrica_1, colonna_metrica_2, colonna_metrica_3, colonna_metrica_4 = st.columns(4)
        risultato_metriche = leggi_dati(queries.QUERY_METRICHE_PRINCIPALI).iloc[0]
        
        colonna_metrica_1.metric("Ordini totali",         f"{int(risultato_metriche['numero_totale_ordini']):,}")
        colonna_metrica_2.metric("Prodotti a catalogo",   f"{int(risultato_metriche['numero_totale_prodotti']):,}")
        colonna_metrica_3.metric("Tasso di riordino",     f"{float(risultato_metriche['percentuale_media_riordino']):.1f}%")
        colonna_metrica_4.metric("Giorni medi tra ordini",f"{float(risultato_metriche['media_giorni_tra_ordini']):.1f}")

        st.divider()

        colonna_grafico_1, colonna_grafico_2 = st.columns(2)

        # Ordini per ora del giorno
        with colonna_grafico_1:
            st.markdown("#### Ordini per ora del giorno")
            dataframe_ordini_per_ora = leggi_dati(queries.QUERY_ORDINI_PER_ORA)
            st.bar_chart(dataframe_ordini_per_ora.set_index("ora_del_giorno")["totale_ordini"])

        # Ordini per giorno della settimana
        with colonna_grafico_2:
            st.markdown("#### Ordini per giorno della settimana")
            dataframe_ordini_per_giorno = leggi_dati(queries.QUERY_ORDINI_PER_GIORNO)
            dataframe_ordini_per_giorno["etichetta_giorno"] = dataframe_ordini_per_giorno["giorno_della_settimana_numerico"].map(ETICHETTE_GIORNI_SETTIMANA)
            st.bar_chart(dataframe_ordini_per_giorno.set_index("etichetta_giorno")["totale_ordini"])

        st.divider()

        colonna_grafico_3, colonna_grafico_4 = st.columns(2)

        # Top 10 reparti per prodotti ordinati
        with colonna_grafico_3:
            st.markdown("#### Top 10 reparti per prodotti ordinati")
            dataframe_prodotti_per_reparto = leggi_dati(queries.QUERY_TOP_REPARTI)
            st.bar_chart(dataframe_prodotti_per_reparto.set_index("nome_reparto")["quantita_ordinata"])

        # Tasso di riordino per reparto
        with colonna_grafico_4:
            st.markdown("#### Tasso di riordino (%) per reparto")
            dataframe_riordini_per_reparto = leggi_dati(queries.QUERY_TASSO_RIORDINO_REPARTI)
            st.bar_chart(dataframe_riordini_per_reparto.set_index("nome_reparto")["percentuale_di_riordino"])

    except Exception as errore_dashboard:
        st.error(f"Errore nel caricamento dashboard: {errore_dashboard}")


# Cerca prodotti
with scheda_ricerca:
    st.subheader("Cerca Prodotti")
    st.markdown("Pagina dedicata alla ricerca dei prodotti. Filtra per nome, reparto, corsia. **I prodotti senza ordini vengono comunque mostrati.**")
    st.divider()

    colonna_filtro_testo, colonna_filtro_reparto, colonna_filtro_orario = st.columns([2, 1, 1])
    with colonna_filtro_testo:
        testo_ricerca_prodotto = st.text_input("Nome prodotto:", placeholder="es. Banana, Latte…")
    with colonna_filtro_reparto:
        opzioni_reparti = ["Tutti i reparti"] + sorted(ottieni_dizionario_reparti().values())
        reparto_selezionato_ricerca = st.selectbox("Reparto:", opzioni_reparti)
    with colonna_filtro_orario:
        fascia_oraria_selezionata = st.slider("Fascia oraria ordini:", 0, 23, (0, 23))

    if st.button("Cerca", type="primary", use_container_width=True):
        clausola_reparto_sql = "" if reparto_selezionato_ricerca == "Tutti i reparti" else "AND departments.department = :parametro_nome_reparto"

        query_ricerca_prodotti = queries.QUERY_RICERCA_PRODOTTI.format(clausola_reparto=clausola_reparto_sql)

        parametri_ricerca_sql = {
            "parametro_testo_ricerca": "%" + testo_ricerca_prodotto + "%", 
            "parametro_ora_inizio": fascia_oraria_selezionata[0], 
            "parametro_ora_fine": fascia_oraria_selezionata[1]
        }
        
        if reparto_selezionato_ricerca != "Tutti i reparti":
            parametri_ricerca_sql["parametro_nome_reparto"] = reparto_selezionato_ricerca

        try:
            dataframe_risultati_ricerca = leggi_dati(query_ricerca_prodotti, parametri_ricerca_sql)
            
            if dataframe_risultati_ricerca.empty:
                st.warning("Nessun risultato trovato")
            else:
                st.success(f"Ricerca completata: **{len(dataframe_risultati_ricerca)}** prodotti trovati")
                st.dataframe(
                    dataframe_risultati_ricerca, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "ID Prodotto": st.column_config.NumberColumn(format="%d"),
                        "Totale Riordini": st.column_config.ProgressColumn(
                            help="Volume di riordini nel periodo selezionato",
                            format="%d",
                            min_value=0,
                            max_value=int(dataframe_risultati_ricerca["Totale Riordini"].max()) if not dataframe_risultati_ricerca.empty else 100
                        )
                    }
                )
        except Exception as errore_ricerca:
            st.error(errore_ricerca)


# Catalogo (Corsie e Reparti)
with scheda_catalogo:
    st.subheader("Corsie e reparti")
    st.markdown("Aggiungi, rinomina ed elimina nuove corsie e reparti")
    st.divider()

    colonna_gestione_corsie, colonna_gestione_reparti = st.columns(2)

    # Corsie
    with colonna_gestione_corsie:
        st.markdown("### Corsie")
        with st.form("form_aggiunta_corsia", clear_on_submit=True):
            nome_nuova_corsia_input = st.text_input("Nome nuova corsia:", placeholder="es. Energy drink")
            pulsante_conferma_aggiunta_corsia = st.form_submit_button("AGGIUNGI", use_container_width=True, type="primary")

        if pulsante_conferma_aggiunta_corsia:
            if not nome_nuova_corsia_input.strip():
                st.error("Il nome è obbligatorio!")
            else:
                try:
                    id_nuova_corsia = inserisci_e_ritorna_id(
                        queries.QUERY_INSERISCI_CORSIA,
                        {"parametro_nome_corsia": nome_nuova_corsia_input.strip()}
                    )
                    st.success(f"Corsia '{nome_nuova_corsia_input.strip()}' aggiunta (ID {id_nuova_corsia})")
                except Exception as errore_inserimento_corsia:
                    messaggio_errore_corsia = str(errore_inserimento_corsia).lower()
                    if "unique" in messaggio_errore_corsia or "duplicate" in messaggio_errore_corsia:
                        dataframe_corsia_esistente = leggi_dati(queries.QUERY_VERIFICA_CORSIA, {"parametro_nome_corsia": nome_nuova_corsia_input.strip()})
                        if not dataframe_corsia_esistente.empty:
                            st.warning(f"La corsia '{nome_nuova_corsia_input.strip()}' esiste già (ID {dataframe_corsia_esistente.iloc[0,0]}).")
                        else:
                            reimposta_sequenze_id()
                            try:
                                id_nuova_corsia_dopo_reset = inserisci_e_ritorna_id(
                                    queries.QUERY_INSERISCI_CORSIA,
                                    {"parametro_nome_corsia": nome_nuova_corsia_input.strip()}
                                )
                                st.success(f"Corsia '{nome_nuova_corsia_input.strip()}' aggiunta (ID {id_nuova_corsia_dopo_reset})")
                            except Exception as errore_post_reset_corsia:
                                st.error(errore_post_reset_corsia)
                    else:
                        st.error(f"Errore: {errore_inserimento_corsia}")

        st.markdown("#### Corsie esistenti")
        try:
            dataframe_elenco_corsie = leggi_dati(queries.QUERY_ELENCO_CORSIE)
            st.dataframe(dataframe_elenco_corsie, width="stretch", hide_index=True, height=320)
        except Exception as errore_elenco_corsie:
            st.error(errore_elenco_corsie)

        st.markdown("#### Rinomina corsia")
        try:
            dizionario_corsie_da_rinominare = ottieni_dizionario_corsie()
            with st.form("form_rinomina_corsia", clear_on_submit=True):
                id_corsia_da_rinominare_selezionata = st.selectbox(
                    "Corsia da rinominare:",
                    options=list(dizionario_corsie_da_rinominare.keys()),
                    format_func=lambda identificatore: dizionario_corsie_da_rinominare.get(identificatore, str(identificatore)),
                    key="selezione_rinomina_corsia",
                )
                nuovo_nome_corsia_input = st.text_input("Nuovo nome:", placeholder="es. Alcolici e spirits")
                pulsante_conferma_rinomina_corsia = st.form_submit_button("Aggiorna nome corsia", use_container_width=True, type="primary")
            if pulsante_conferma_rinomina_corsia:
                if not nuovo_nome_corsia_input.strip():
                    st.error("Il nuovo nome è obbligatorio.")
                else:
                    try:
                        scrivi_dati(
                            queries.QUERY_RINOMINA_CORSIA,
                            {"parametro_nuovo_nome": nuovo_nome_corsia_input.strip(), "parametro_id_corsia": id_corsia_da_rinominare_selezionata},
                        )
                        st.success(f"Corsia rinominata in '{nuovo_nome_corsia_input.strip()}'!")
                    except Exception as errore_aggiornamento_corsia:
                        st.error(f"Errore: {errore_aggiornamento_corsia}")
        except Exception as errore_generale_rinomina_corsia:
            st.error(errore_generale_rinomina_corsia)

        st.markdown("#### Elimina corsia")
        try:
            dizionario_corsie_da_eliminare = ottieni_dizionario_corsie()
            with st.form("form_elimina_corsia", clear_on_submit=True):
                id_corsia_da_eliminare_selezionata = st.selectbox(
                    "Corsia da eliminare:",
                    options=list(dizionario_corsie_da_eliminare.keys()),
                    format_func=lambda identificatore: dizionario_corsie_da_eliminare.get(identificatore, str(identificatore)),
                    key="selezione_elimina_corsia",
                )
                checkbox_conferma_eliminazione_corsia = st.checkbox(" Confermo l'eliminazione")
                pulsante_conferma_eliminazione_corsia = st.form_submit_button("Elimina corsia", type="secondary", use_container_width=True)
            if pulsante_conferma_eliminazione_corsia:
                if not checkbox_conferma_eliminazione_corsia:
                    st.error("Spunta la casella di conferma prima di eliminare!")
                else:
                    numero_prodotti_in_corsia = leggi_dati(queries.QUERY_CONTA_PRODOTTI_CORSIA, {"parametro_id_corsia": id_corsia_da_eliminare_selezionata}).iloc[0]["totale_prodotti_collegati"]
                    if numero_prodotti_in_corsia > 0:
                        st.warning(f"La corsia ha {int(numero_prodotti_in_corsia)} prodotti collegati. Spostali prima di eliminarla.")
                    else:
                        try:
                            scrivi_dati(queries.QUERY_ELIMINA_CORSIA, {"parametro_id_corsia": id_corsia_da_eliminare_selezionata})
                            st.success("Corsia eliminata!")
                        except Exception as errore_cancellazione_corsia:
                            st.error(f"Errore: {errore_cancellazione_corsia}")
        except Exception as errore_generale_eliminazione_corsia:
            st.error(errore_generale_eliminazione_corsia)

    # Reparti
    with colonna_gestione_reparti:
        st.markdown("### Reparti")
        with st.form("form_aggiunta_reparto", clear_on_submit=True):
            nome_nuovo_reparto_input = st.text_input("Nome nuovo reparto:", placeholder="es. Bevande")
            pulsante_conferma_aggiunta_reparto = st.form_submit_button("Aggiungi reparto", use_container_width=True, type="primary")

        if pulsante_conferma_aggiunta_reparto:
            if not nome_nuovo_reparto_input.strip():
                st.error("Il nome è obbligatorio!")
            else:
                try:
                    id_nuovo_reparto = inserisci_e_ritorna_id(
                        queries.QUERY_INSERISCI_REPARTO,
                        {"parametro_nome_reparto": nome_nuovo_reparto_input.strip()}
                    )
                    st.success(f"Reparto '{nome_nuovo_reparto_input.strip()}' aggiunto (ID {id_nuovo_reparto})")
                except Exception as errore_inserimento_reparto:
                    messaggio_errore_reparto = str(errore_inserimento_reparto).lower()
                    if "unique" in messaggio_errore_reparto or "duplicate" in messaggio_errore_reparto:
                        dataframe_reparto_esistente = leggi_dati(queries.QUERY_VERIFICA_REPARTO, {"parametro_nome_reparto": nome_nuovo_reparto_input.strip()})
                        if not dataframe_reparto_esistente.empty:
                            st.warning(f"Il reparto '{nome_nuovo_reparto_input.strip()}' esiste già (ID {dataframe_reparto_esistente.iloc[0,0]}).")
                        else:
                            reimposta_sequenze_id()
                            try:
                                id_nuovo_reparto_dopo_reset = inserisci_e_ritorna_id(
                                    queries.QUERY_INSERISCI_REPARTO,
                                    {"parametro_nome_reparto": nome_nuovo_reparto_input.strip()}
                                )
                                st.success(f"Reparto '{nome_nuovo_reparto_input.strip()}' aggiunto (ID {id_nuovo_reparto_dopo_reset})")
                            except Exception as errore_post_reset_reparto:
                                st.error(f"Errore dopo reset sequenza: {errore_post_reset_reparto}")
                    else:
                        st.error(f"Errore: {errore_inserimento_reparto}")

        st.markdown("#### Reparti esistenti")
        try:
            dataframe_elenco_reparti = leggi_dati(queries.QUERY_ELENCO_REPARTI)
            st.dataframe(dataframe_elenco_reparti, width="stretch", hide_index=True, height=320)
        except Exception as errore_elenco_reparti:
            st.error(errore_elenco_reparti)

        st.markdown("#### Rinomina reparto")
        try:
            dizionario_reparti_da_rinominare = ottieni_dizionario_reparti()
            with st.form("form_rinomina_reparto", clear_on_submit=True):
                id_reparto_da_rinominare_selezionato = st.selectbox(
                    "Reparto da rinominare:",
                    options=list(dizionario_reparti_da_rinominare.keys()),
                    format_func=lambda identificatore: dizionario_reparti_da_rinominare.get(identificatore, str(identificatore)),
                    key="selezione_rinomina_reparto",
                )
                nuovo_nome_reparto_input = st.text_input("Nuovo nome:", placeholder="es. Insaccati e Frutta")
                pulsante_conferma_rinomina_reparto = st.form_submit_button("Aggiorna nome reparto", use_container_width=True, type="primary")
            if pulsante_conferma_rinomina_reparto:
                if not nuovo_nome_reparto_input.strip():
                    st.error("Il nuovo nome è obbligatorio.")
                else:
                    try:
                        scrivi_dati(
                            queries.QUERY_RINOMINA_REPARTO,
                            {"parametro_nuovo_nome": nuovo_nome_reparto_input.strip(), "parametro_id_reparto": id_reparto_da_rinominare_selezionato},
                        )
                        st.success(f"Reparto rinominato in '{nuovo_nome_reparto_input.strip()}'!")
                    except Exception as errore_aggiornamento_reparto:
                        st.error(f"Errore: {errore_aggiornamento_reparto}")
        except Exception as errore_generale_rinomina_reparto:
            st.error(errore_generale_rinomina_reparto)

        st.markdown("#### Elimina reparto")
        try:
            dizionario_reparti_da_eliminare = ottieni_dizionario_reparti()
            with st.form("form_elimina_reparto", clear_on_submit=True):
                id_reparto_da_eliminare_selezionato = st.selectbox(
                    "Reparto da eliminare:",
                    options=list(dizionario_reparti_da_eliminare.keys()),
                    format_func=lambda identificatore: dizionario_reparti_da_eliminare.get(identificatore, str(identificatore)),
                    key="selezione_elimina_reparto",
                )
                checkbox_conferma_eliminazione_reparto = st.checkbox("Confermo l'eliminazione")
                pulsante_conferma_eliminazione_reparto = st.form_submit_button("Elimina reparto", type="secondary", use_container_width=True)
            if pulsante_conferma_eliminazione_reparto:
                if not checkbox_conferma_eliminazione_reparto:
                    st.error("Spunta la casella di conferma prima di eliminare!")
                else:
                    numero_prodotti_nel_reparto = leggi_dati(queries.QUERY_CONTA_PRODOTTI_REPARTO, {"parametro_id_reparto": id_reparto_da_eliminare_selezionato}).iloc[0]["totale_prodotti_collegati"]
                    if numero_prodotti_nel_reparto > 0:
                        st.warning(f"Il reparto ha {int(numero_prodotti_nel_reparto)} prodotti collegati. Spostali prima di eliminarlo.")
                    else:
                        try:
                            scrivi_dati(queries.QUERY_ELIMINA_REPARTO, {"parametro_id_reparto": id_reparto_da_eliminare_selezionato})
                            st.success("Reparto eliminato!")
                        except Exception as errore_cancellazione_reparto:
                            st.error(f"Errore: {errore_cancellazione_reparto}")
        except Exception as errore_generale_eliminazione_reparto:
            st.error(errore_generale_eliminazione_reparto)

# Aggiungi prodotto
with scheda_aggiungi:
    st.subheader("Aggiungi un nuovo prodotto")
    st.markdown(
        "Seleziona corsia e reparto dall'elenco. "
        "Se non esistono ancora, creali prima nella scheda **Catalogo**."
    )
    st.divider()

    dizionario_completo_corsie = ottieni_dizionario_corsie()
    dizionario_completo_reparti  = ottieni_dizionario_reparti()

    with st.form("form_aggiunta_prodotto", clear_on_submit=True):
        nome_nuovo_prodotto_input = st.text_input("Nome prodotto *", max_chars=255, placeholder="es. Banana")
        colonna_selezione_corsia, colonna_selezione_reparto = st.columns(2)
        with colonna_selezione_corsia:
            id_corsia_scelta_per_inserimento = st.selectbox(
                "Corsia *",
                options=list(dizionario_completo_corsie.keys()),
                format_func=lambda identificatore: dizionario_completo_corsie.get(identificatore, str(identificatore)),
            )
        with colonna_selezione_reparto:
            id_reparto_scelto_per_inserimento = st.selectbox(
                "Reparto *",
                options=list(dizionario_completo_reparti.keys()),
                format_func=lambda identificatore: dizionario_completo_reparti.get(identificatore, str(identificatore)),
            )
        pulsante_conferma_inserimento_prodotto = st.form_submit_button("Inserisci prodotto", type="primary", use_container_width=True)

    if pulsante_conferma_inserimento_prodotto:
        if not nome_nuovo_prodotto_input.strip():
            st.error("Il nome del prodotto è obbligatorio!")
        else:
            try:
                scrivi_dati(
                    queries.QUERY_INSERISCI_PRODOTTO,
                    {"parametro_nome_prodotto": nome_nuovo_prodotto_input.strip(), "parametro_id_corsia": id_corsia_scelta_per_inserimento, "parametro_id_reparto": id_reparto_scelto_per_inserimento},
                )
                st.success(f"Prodotto **'{nome_nuovo_prodotto_input.strip()}'** aggiunto con successo!")
            except Exception as errore_inserimento_prodotto:
                st.error(f"Errore: {errore_inserimento_prodotto}")


# Modifica prodotto
with scheda_modifica:
    st.subheader("Modifica un prodotto **esistente**.")
    st.markdown("Scegli prima reparto e corsia per filtrare i prodotti, poi seleziona quello da modificare, inoltre se non più presente è possibile eliminarlo in fondo alla pagina.")
    st.divider()

    dizionario_completo_reparti_modifica = ottieni_dizionario_reparti()
    dizionario_completo_corsie_modifica = ottieni_dizionario_corsie()

    # Filtri
    st.markdown("#### Filtra per reparto e corsia")
    colonna_filtro_modifica_reparto, colonna_filtro_modifica_corsia = st.columns(2)
    with colonna_filtro_modifica_reparto:
        opzioni_filtro_reparti_modifica = {"": "— Tutti i reparti —"} | {str(chiave): valore for chiave, valore in dizionario_completo_reparti_modifica.items()}
        id_reparto_filtrato_selezionato = st.selectbox(
            "Reparto:",
            options=list(opzioni_filtro_reparti_modifica.keys()),
            format_func=lambda identificatore: opzioni_filtro_reparti_modifica[identificatore],
            key="filtro_reparto_modifica",
        )
    with colonna_filtro_modifica_corsia:
        if id_reparto_filtrato_selezionato:
            dataframe_corsie_filtrate_per_reparto = leggi_dati(
                queries.QUERY_CORSIE_PER_REPARTO,
                {"parametro_id_reparto": int(id_reparto_filtrato_selezionato)},
            )
            opzioni_filtro_corsie_modifica = {"": "— Tutte le corsie —"} | dict(zip(dataframe_corsie_filtrate_per_reparto["aisle_id"].astype(str), dataframe_corsie_filtrate_per_reparto["aisle"]))
        else:
            opzioni_filtro_corsie_modifica = {"": "— Tutte le corsie —"} | {str(chiave): valore for chiave, valore in dizionario_completo_corsie_modifica.items()}
        id_corsia_filtrata_selezionata = st.selectbox(
            "Corsia:",
            options=list(opzioni_filtro_corsie_modifica.keys()),
            format_func=lambda identificatore: opzioni_filtro_corsie_modifica[identificatore],
            key="filtro_corsia_modifica",
        )

    # Lista prodotti filtrata
    st.markdown("#### Seleziona il prodotto")
    lista_clausole_where_sql = []
    dizionario_parametri_query_prodotti = {}
    
    if id_reparto_filtrato_selezionato:
        lista_clausole_where_sql.append("products.department_id = :parametro_id_reparto")
        dizionario_parametri_query_prodotti["parametro_id_reparto"] = int(id_reparto_filtrato_selezionato)
    if id_corsia_filtrata_selezionata:
        lista_clausole_where_sql.append("products.aisle_id = :parametro_id_corsia")
        dizionario_parametri_query_prodotti["parametro_id_corsia"] = int(id_corsia_filtrata_selezionata)
        
    stringa_where_completa_sql = ("WHERE " + " AND ".join(lista_clausole_where_sql)) if lista_clausole_where_sql else ""

    try:
        query_ricerca_prodotti_da_modificare = queries.QUERY_RICERCA_MODIFICA_PRODOTTO.format(stringa_where=stringa_where_completa_sql)
        dataframe_prodotti_filtrati_per_modifica = leggi_dati(
            query_ricerca_prodotti_da_modificare,
            dizionario_parametri_query_prodotti,
        )
    except Exception as errore_recupero_prodotti_modifica:
        st.error(f"Errore: {errore_recupero_prodotti_modifica}")
        dataframe_prodotti_filtrati_per_modifica = pd.DataFrame()

    if dataframe_prodotti_filtrati_per_modifica.empty:
        st.info("Nessun prodotto trovato con i filtri selezionati.")
    else:
        st.caption(f"{len(dataframe_prodotti_filtrati_per_modifica)} prodotti trovati")
        dizionario_prodotti_estratti = dataframe_prodotti_filtrati_per_modifica.set_index("product_id").to_dict(orient="index")
        id_prodotto_selezionato_per_modifica = st.selectbox(
            "Prodotto:",
            options=list(dizionario_prodotti_estratti.keys()),
            format_func=lambda identificatore: dizionario_prodotti_estratti[identificatore]["product_name"],
            key="selezione_prodotto_modifica",
        )
        dati_prodotto_attualmente_selezionato = dizionario_prodotti_estratti[id_prodotto_selezionato_per_modifica]

        # Modifica prodotto
        st.markdown("#### Nuovi valori")
        st.divider()
        colonna_aggiornamento_nome, colonna_aggiornamento_corsia, colonna_aggiornamento_reparto = st.columns(3)
        with colonna_aggiornamento_nome:
            nome_prodotto_aggiornato_input = st.text_input("Nome prodotto", value=dati_prodotto_attualmente_selezionato["product_name"])
        with colonna_aggiornamento_corsia:
            lista_id_corsie_disponibili = list(dizionario_completo_corsie_modifica.keys())
            indice_corsia_attuale = lista_id_corsie_disponibili.index(dati_prodotto_attualmente_selezionato["aisle_id"]) if dati_prodotto_attualmente_selezionato["aisle_id"] in lista_id_corsie_disponibili else 0
            id_corsia_aggiornata_selezionata = st.selectbox(
                "Sposta in corsia:",
                options=lista_id_corsie_disponibili, index=indice_corsia_attuale,
                format_func=lambda identificatore: dizionario_completo_corsie_modifica.get(identificatore, str(identificatore)),
            )
        with colonna_aggiornamento_reparto:
            lista_id_reparti_disponibili = list(dizionario_completo_reparti_modifica.keys())
            indice_reparto_attuale = lista_id_reparti_disponibili.index(dati_prodotto_attualmente_selezionato["department_id"]) if dati_prodotto_attualmente_selezionato["department_id"] in lista_id_reparti_disponibili else 0
            id_reparto_aggiornato_selezionato = st.selectbox(
                "Sposta in reparto:",
                options=lista_id_reparti_disponibili, index=indice_reparto_attuale,
                format_func=lambda identificatore: dizionario_completo_reparti_modifica.get(identificatore, str(identificatore)),
            )

        if st.button("Salva modifiche", type="primary"):
            if not nome_prodotto_aggiornato_input.strip():
                st.error("Il nome non può essere vuoto!")
            else:
                try:
                    scrivi_dati(
                        queries.QUERY_AGGIORNA_PRODOTTO,
                        {"parametro_nuovo_nome": nome_prodotto_aggiornato_input.strip(), "parametro_nuova_corsia": id_corsia_aggiornata_selezionata, "parametro_nuovo_reparto": id_reparto_aggiornato_selezionato, "parametro_id_prodotto": id_prodotto_selezionato_per_modifica},
                    )
                    st.success(f"'{nome_prodotto_aggiornato_input.strip()}' aggiornato!")
                except Exception as errore_aggiornamento_prodotto:
                    st.error(f"Errore: {errore_aggiornamento_prodotto}")

        st.divider()
        st.markdown("#### Elimina prodotto")
        checkbox_conferma_eliminazione_prodotto = st.checkbox(
            f"Confermo l'eliminazione di **'{dati_prodotto_attualmente_selezionato['product_name']}'**",
            key="checkbox_conferma_eliminazione_singolo_prodotto",
        )
        if st.button("Elimina prodotto", type="secondary", disabled=not checkbox_conferma_eliminazione_prodotto):
            try:
                scrivi_dati(queries.QUERY_ELIMINA_ORDINI_PRODOTTO, {"parametro_id_prodotto": id_prodotto_selezionato_per_modifica})
                scrivi_dati(queries.QUERY_ELIMINA_PRODOTTO, {"parametro_id_prodotto": id_prodotto_selezionato_per_modifica})
                st.success("Prodotto eliminato (rimosso anche dagli ordini).")
            except Exception as errore_cancellazione_prodotto:
                st.error(f"Errore: {errore_cancellazione_prodotto}")


# CTE Analitica Avanzata
with scheda_analisi:
    st.subheader("Query analitica avanzata")
    st.markdown(
        "Esegue il file `analytical_query.sql`. "
        "Il risultato viene mostrato come tabella interattiva e grafico."
    )
    st.divider()

    if st.button("Esegui query analitica", type="primary", use_container_width=True):
        with st.spinner("Esecuzione in corso…"):
            try:
                dataframe_risultato_query_analitica = carica_ed_esegui_query_analitica("analytical_query.sql")
                st.success(f"Query completata con {len(dataframe_risultato_query_analitica)} righe restituite.")
                st.dataframe(dataframe_risultato_query_analitica, width="stretch", hide_index=True)
                
                lista_colonne_numeriche = dataframe_risultato_query_analitica.select_dtypes(include="number").columns.tolist()
                lista_colonne_testuali = dataframe_risultato_query_analitica.select_dtypes(exclude="number").columns.tolist()
                
                if lista_colonne_numeriche and lista_colonne_testuali:
                    st.markdown("#### Grafico")
                    st.bar_chart(dataframe_risultato_query_analitica.set_index(lista_colonne_testuali[0])[lista_colonne_numeriche[0]])
            except FileNotFoundError as errore_file_non_trovato:
                st.error(errore_file_non_trovato)
            except Exception as errore_esecuzione_analitica:
                st.error(errore_esecuzione_analitica)