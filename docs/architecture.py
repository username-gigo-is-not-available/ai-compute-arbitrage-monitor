from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, Looker
from diagrams.gcp.storage import GCS
from diagrams.gcp.network import VirtualPrivateCloud, Router, NAT, FirewallRules
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.analytics import Dbt, Spark
from diagrams.onprem.network import Internet
from diagrams.programming.language import Python

graph_attr = {
    "fontsize": "13",
    "bgcolor": "white",
    "pad": "1.5",
    "splines": "curved",
    "nodesep": "0.6",
    "ranksep": "1.2",
    "rankdir": "LR",
}

with Diagram(
        "GPU Compute Arbitrage — Architecture",
        filename="gpu_arbitrage_architecture",
        graph_attr=graph_attr,
        show=False,
):
    # ── Col 1: External sources ────────────────────────────────────────
    with Cluster("External Sources"):
        vast_api = Internet("Vast.AI API\n@hourly")
        exchange_api = Internet("ExchangeRate API\n@daily")
        erc_web = Internet("ERC erc.org.mk\n@daily")
        evn_web = Internet("EVN evn.mk\n@daily")

    # ── Col 2: Docker Compose ─────────────────────────────────────────
    with Cluster("Local — Docker Compose"):
        with Cluster("Airflow 3 (LocalExecutor)"):
            pg = PostgreSQL("Metastore")
            airflow = Airflow("Airflow")
            pg >> Edge(style="dashed", color="gray") >> airflow

        with Cluster("Ingest Layer — PythonOperator"):
            vast_ingest = Python("VastAISource\nasync")
            exchange_ingest = Python("ExchangeRateSource\nasync")
            erc_ingest = Python("TariffPricesSeed\nsync")
            evn_ingest = Python("TariffScheduleSeed\nsync")

    # External sources → ingest apps
    vast_api >>  vast_ingest
    exchange_api >>  exchange_ingest
    erc_web >> erc_ingest
    evn_web >> evn_ingest

    # Airflow triggers ingest apps
    airflow >> Edge() >> vast_ingest
    airflow >> Edge() >> exchange_ingest
    airflow >> Edge() >> erc_ingest
    airflow >> Edge() >> evn_ingest

    # ── Col 3: GCS Bronze ─────────────────────────────────────────────
    bronze = GCS("Bronze\nraw parquet\nGCS")

    vast_ingest >> Edge() >> bronze
    exchange_ingest >> bronze
    erc_ingest >> bronze
    evn_ingest >> bronze

    # ── Col 4: GCP — VPC + Dataproc ───────────────────────────────────
    with Cluster("GCP — europe-west3"):
        with Cluster("VPC — 10.0.0.0/24"):
            vpc = VirtualPrivateCloud("VPC")
            with Cluster("Subnet — 10.0.0.0/24"):
                fw = FirewallRules("Firewall\nallow-internal")
                router = Router("Cloud Router")
                nat = NAT("Cloud NAT")
                router >> nat
                with Cluster("Dataproc\ngpu-arbitrage-cluster"):
                    spark = Spark("PySpark\nClean · Cast · Dedup")
                fw - Edge(style="invis") - spark

    silver = GCS("Silver\ncleaned parquet\nGCS")

    airflow >> Edge() >> spark
    bronze >> Edge() >> spark
    spark >> Edge() >> silver
    bq = BigQuery("BigQuery\nDataset")

    # ── Col 5: Per-source pipeline DAG — dbt ──────────────────────────
    with Cluster("Per-source Pipeline DAG (x4)\nBashOperator — dbt + BigQuery"):
        dbt_ext_tbl = Dbt("dbt run-operation\n stage_external_sources")
        dbt_stg = Dbt("dbt run\n stg_*")
        dbt_int = Dbt("dbt run\nint_*")
        dbt_wh = Dbt("dbt run\nfct_* · dim_*")
        dbt_test = Dbt("dbt test\n→ publish Asset")
        dbt_ext_tbl >> dbt_stg >> dbt_int >> dbt_wh >> dbt_test
        dbt_ext_tbl >> Edge() >> bq

    airflow >> Edge() >> dbt_stg
    airflow >> Edge() >> dbt_ext_tbl
    silver >> Edge() >> dbt_ext_tbl
    dbt_wh >> Edge() >> bq
    bq >> Edge(reverse=True) >> dbt_stg

    # ── Col 6: Marts DAG — triggered by Asset from dbt test ───────────
    with Cluster("Marts DAG\ngpu_arbitrage__marts"):
        marts_run = Dbt("dbt run\ntag:marts")
        marts_test = Dbt("dbt test\ntag:marts")
        marts_run >> marts_test

    # dbt test publishes Asset → triggers Marts DAG (AssetAny)
    dbt_test >> Edge(
        label="Asset published\n(AssetAny trigger)",
        style="dashed",
        color="orange",
    ) >> marts_run

    marts_test >> Edge(reverse=True) >> bq

    looker = Looker("Looker\nDashboards")
    bq >> looker
