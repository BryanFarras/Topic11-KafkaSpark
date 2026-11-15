"""
Generate Architecture Diagram v0.3 for Topic 11 (Ingestion & Streaming Pipeline)
Kelompok 6 - NASA FIRMS VIIRS Hotspot Monitoring in Indonesia
Creates both report/diagram_v03.svg and report/diagram_v03.png
"""
import os
import xml.etree.ElementTree as ET

def create_svg():
    width = 1200
    height = 700
    
    # SVG canvas with styles
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}" style="background-color: #0f172a; font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Helvetica, Arial, sans-serif;">
  <defs>
    <!-- Gradients -->
    <linearGradient id="headerGrad" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#1e293b" />
      <stop offset="100%" stop-color="#0f172a" />
    </linearGradient>
    <linearGradient id="streamCardGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1e293b" />
      <stop offset="100%" stop-color="#111827" />
    </linearGradient>
    <linearGradient id="kafkaGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#312e81" />
      <stop offset="100%" stop-color="#1e1b4b" />
    </linearGradient>
    <linearGradient id="sparkGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#831843" />
      <stop offset="100%" stop-color="#500724" />
    </linearGradient>
    <linearGradient id="storageGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#064e3b" />
      <stop offset="100%" stop-color="#022c22" />
    </linearGradient>
    <linearGradient id="servingGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#701a75" />
      <stop offset="100%" stop-color="#4a044e" />
    </linearGradient>

    <!-- Markers -->
    <marker id="arrowStream" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#38bdf8" />
    </marker>
    <marker id="arrowBatch" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#f59e0b" />
    </marker>
    <marker id="arrowCheckpoint" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#a855f7" />
    </marker>

    <!-- Drop Shadows -->
    <filter id="shadow" x="-5%" y="-5%" width="110%" height="110%">
      <feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="#000000" flood-opacity="0.5"/>
    </filter>
  </defs>

  <!-- Title & Header -->
  <rect x="20" y="20" width="1160" height="70" rx="10" fill="url(#headerGrad)" stroke="#334155" stroke-width="1.5" filter="url(#shadow)"/>
  <text x="45" y="52" fill="#f8fafc" font-size="20" font-weight="700">DIAGRAM ARSITEKTUR v0.3 — END-TO-END BIG DATA PIPELINE</text>
  <text x="45" y="74" fill="#94a3b8" font-size="13">Kasus: Sistem Pemantauan Titik Api (Hotspot) Kebakaran Hutan Indonesia (NASA FIRMS VIIRS) · Kelompok 6</text>

  <!-- Legend -->
  <g transform="translate(820, 36)">
    <rect x="0" y="0" width="340" height="42" rx="6" fill="#1e293b" stroke="#475569" stroke-width="1"/>
    <!-- Stream Legend -->
    <line x1="15" y1="21" x2="45" y2="21" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>
    <text x="55" y="25" fill="#e2e8f0" font-size="11" font-weight="600">Aliran Streaming (Real-Time)</text>
    <!-- Batch Legend -->
    <line x1="205" y1="21" x2="235" y2="21" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrowBatch)"/>
    <text x="245" y="25" fill="#e2e8f0" font-size="11" font-weight="600">Aliran Batch</text>
  </g>

  <!-- ==================== LAYER 1: DATA SOURCES ==================== -->
  <g transform="translate(20, 110)">
    <rect x="0" y="0" width="190" height="550" rx="10" fill="url(#streamCardGrad)" stroke="#334155" stroke-width="1.5" filter="url(#shadow)"/>
    <text x="20" y="32" fill="#38bdf8" font-size="13" font-weight="700">1. DATA SOURCES</text>
    <line x1="20" y1="42" x2="170" y2="42" stroke="#334155" stroke-width="1"/>

    <!-- Source A: VIIRS NASA FIRMS Stream -->
    <g transform="translate(15, 60)">
      <rect x="0" y="0" width="160" height="120" rx="8" fill="#1e293b" stroke="#0284c7" stroke-width="1.5"/>
      <text x="12" y="22" fill="#bae6fd" font-size="11" font-weight="700">NASA FIRMS VIIRS</text>
      <text x="12" y="38" fill="#94a3b8" font-size="9.5">Satelit Suomi-NPP / NOAA</text>
      <text x="12" y="54" fill="#94a3b8" font-size="9.5">Deteksi Titik Panas NRT</text>
      <rect x="12" y="65" width="136" height="20" rx="4" fill="#0369a1"/>
      <text x="18" y="79" fill="#ffffff" font-size="9" font-weight="600">Latency: 1-5 Menit</text>
      <text x="12" y="104" fill="#38bdf8" font-size="9" font-weight="600">[STREAM INGESTION]</text>
    </g>

    <!-- Source B: User Clickstream / Telemetry -->
    <g transform="translate(15, 200)">
      <rect x="0" y="0" width="160" height="120" rx="8" fill="#1e293b" stroke="#0284c7" stroke-width="1.5"/>
      <text x="12" y="22" fill="#bae6fd" font-size="11" font-weight="700">Web App &amp; Telemetri</text>
      <text x="12" y="38" fill="#94a3b8" font-size="9.5">Topic 11 Clickstream</text>
      <text x="12" y="54" fill="#94a3b8" font-size="9.5">Event Klik &amp; Langganan</text>
      <rect x="12" y="65" width="136" height="20" rx="4" fill="#0369a1"/>
      <text x="18" y="79" fill="#ffffff" font-size="9" font-weight="600">JSON (user_id, kategori)</text>
      <text x="12" y="104" fill="#38bdf8" font-size="9" font-weight="600">[STREAM INGESTION]</text>
    </g>

    <!-- Source C: Historical Peatland & Weather Batch -->
    <g transform="translate(15, 340)">
      <rect x="0" y="0" width="160" height="175" rx="8" fill="#1e293b" stroke="#d97706" stroke-width="1.5"/>
      <text x="12" y="22" fill="#fde68a" font-size="11" font-weight="700">Data Historis &amp; Geografis</text>
      <text x="12" y="38" fill="#94a3b8" font-size="9.5">• Peta Lahan Gambut KLHK</text>
      <text x="12" y="54" fill="#94a3b8" font-size="9.5">• 239 Grid Provinsi (CSV)</text>
      <text x="12" y="70" fill="#94a3b8" font-size="9.5">• Arsip VIIRS 10k/100k</text>
      <text x="12" y="86" fill="#94a3b8" font-size="9.5">• Data Cuaca BMKG Harian</text>
      <rect x="12" y="100" width="136" height="20" rx="4" fill="#b45309"/>
      <text x="18" y="114" fill="#ffffff" font-size="9" font-weight="600">Latency: 24 Jam (Batch)</text>
      <text x="12" y="140" fill="#fbbf24" font-size="9" font-weight="600">[BATCH INGESTION]</text>
    </g>
  </g>

  <!-- ==================== LAYER 2: INGESTION (KAFKA) ==================== -->
  <g transform="translate(235, 110)">
    <rect x="0" y="0" width="220" height="550" rx="10" fill="url(#kafkaGrad)" stroke="#4f46e5" stroke-width="1.5" filter="url(#shadow)"/>
    <text x="20" y="32" fill="#a5b4fc" font-size="13" font-weight="700">2. INGESTION (KAFKA)</text>
    <line x1="20" y1="42" x2="200" y2="42" stroke="#4f46e5" stroke-width="1"/>

    <!-- Ingestion Dispatcher / Producer -->
    <g transform="translate(15, 60)">
      <rect x="0" y="0" width="190" height="90" rx="8" fill="#1e1b4b" stroke="#6366f1" stroke-width="1.5"/>
      <text x="12" y="22" fill="#c7d2fe" font-size="11" font-weight="700">Python Producer</text>
      <text x="12" y="38" fill="#94a3b8" font-size="9.5">producer.py (Laju 100-10k)</text>
      <text x="12" y="54" fill="#94a3b8" font-size="9.5">Key: UTF-8 str(user_id)</text>
      <text x="12" y="72" fill="#38bdf8" font-size="9" font-weight="600">Deterministic Partitioning</text>
    </g>

    <!-- Kafka KRaft Broker Container -->
    <g transform="translate(15, 175)">
      <rect x="0" y="0" width="190" height="340" rx="8" fill="#1e1b4b" stroke="#6366f1" stroke-width="1.5"/>
      <text x="12" y="24" fill="#c7d2fe" font-size="11" font-weight="700">Kafka Broker 4.3.1 (KRaft)</text>
      <text x="12" y="40" fill="#818cf8" font-size="9.5">Docker Container :9092</text>
      <text x="12" y="56" fill="#94a3b8" font-size="9">ZooKeeper-Free Architecture</text>

      <!-- Topic klik Box -->
      <g transform="translate(10, 70)">
        <rect x="0" y="0" width="170" height="185" rx="6" fill="#312e81" stroke="#818cf8" stroke-width="1.2"/>
        <text x="10" y="20" fill="#ffffff" font-size="10.5" font-weight="700">Topic: "klik" (Hotspot Stream)</text>
        <text x="10" y="35" fill="#a5b4fc" font-size="8.5">Retention: 168 Jam (7 Hari)</text>

        <!-- Partition 0 -->
        <g transform="translate(10, 48)">
          <rect x="0" y="0" width="150" height="36" rx="4" fill="#1e1b4b" stroke="#6366f1" stroke-width="1"/>
          <text x="8" y="16" fill="#c7d2fe" font-size="9" font-weight="700">Partisi 0 (Offset Log)</text>
          <text x="8" y="29" fill="#94a3b8" font-size="8">Hash(key) % 3 == 0</text>
        </g>

        <!-- Partition 1 -->
        <g transform="translate(10, 92)">
          <rect x="0" y="0" width="150" height="36" rx="4" fill="#1e1b4b" stroke="#6366f1" stroke-width="1"/>
          <text x="8" y="16" fill="#c7d2fe" font-size="9" font-weight="700">Partisi 1 (Offset Log)</text>
          <text x="8" y="29" fill="#94a3b8" font-size="8">Hash(key) % 3 == 1</text>
        </g>

        <!-- Partition 2 -->
        <g transform="translate(10, 136)">
          <rect x="0" y="0" width="150" height="36" rx="4" fill="#1e1b4b" stroke="#6366f1" stroke-width="1"/>
          <text x="8" y="16" fill="#c7d2fe" font-size="9" font-weight="700">Partisi 2 (Offset Log)</text>
          <text x="8" y="29" fill="#94a3b8" font-size="8">Hash(key) % 3 == 2</text>
        </g>
      </g>

      <text x="14" y="280" fill="#a5b4fc" font-size="9" font-weight="600">Durabilitas Tinggi &amp; Replay</text>
      <text x="14" y="295" fill="#94a3b8" font-size="8.5">• Menampung backlog saat crash</text>
      <text x="14" y="310" fill="#94a3b8" font-size="8.5">• Zero data loss terverifikasi M3</text>
    </g>
  </g>

  <!-- ==================== LAYER 3: PROCESSING ==================== -->
  <g transform="translate(480, 110)">
    <rect x="0" y="0" width="240" height="550" rx="10" fill="url(#sparkGrad)" stroke="#db2777" stroke-width="1.5" filter="url(#shadow)"/>
    <text x="20" y="32" fill="#fbcfe8" font-size="13" font-weight="700">3. PROCESSING LAYER</text>
    <line x1="20" y1="42" x2="220" y2="42" stroke="#db2777" stroke-width="1"/>

    <!-- Engine A: PySpark 4.2.0 Structured Streaming (Topic 11) -->
    <g transform="translate(15, 60)">
      <rect x="0" y="0" width="210" height="255" rx="8" fill="#500724" stroke="#f43f5e" stroke-width="1.5"/>
      <text x="12" y="22" fill="#ffe4e6" font-size="11" font-weight="700">Spark Structured Streaming</text>
      <text x="12" y="37" fill="#f43f5e" font-size="9.5">PySpark 4.2.0 (stream_job.py)</text>

      <g transform="translate(10, 48)">
        <rect x="0" y="0" width="190" height="30" rx="4" fill="#831843"/>
        <text x="8" y="19" fill="#ffffff" font-size="8.5" font-weight="600">1. Deserialisasi JSON &amp; Cast Timestamp</text>
      </g>

      <g transform="translate(10, 85)">
        <rect x="0" y="0" width="190" height="30" rx="4" fill="#831843"/>
        <text x="8" y="19" fill="#ffffff" font-size="8.5" font-weight="600">2. Watermark: waktu, 2 Minutes</text>
      </g>

      <g transform="translate(10, 122)">
        <rect x="0" y="0" width="190" height="30" rx="4" fill="#831843"/>
        <text x="8" y="19" fill="#ffffff" font-size="8.5" font-weight="600">3. Window: 1 Minute Tumbling</text>
      </g>

      <g transform="translate(10, 159)">
        <rect x="0" y="0" width="190" height="30" rx="4" fill="#831843"/>
        <text x="8" y="19" fill="#ffffff" font-size="8.5" font-weight="600">4. Group By window, kategori</text>
      </g>

      <!-- Dual Sink Modes inside Spark -->
      <g transform="translate(10, 198)">
        <rect x="0" y="0" width="88" height="42" rx="4" fill="#1e1b4b" stroke="#38bdf8" stroke-width="1"/>
        <text x="6" y="16" fill="#38bdf8" font-size="8.5" font-weight="700">Console Sink</text>
        <text x="6" y="30" fill="#94a3b8" font-size="7.5">Mode: "update"</text>
      </g>
      <g transform="translate(102, 198)">
        <rect x="0" y="0" width="98" height="42" rx="4" fill="#022c22" stroke="#10b981" stroke-width="1"/>
        <text x="6" y="16" fill="#34d399" font-size="8.5" font-weight="700">Parquet Sink</text>
        <text x="6" y="30" fill="#94a3b8" font-size="7.5">Mode: "append"</text>
      </g>
    </g>

    <!-- Engine B: Spark SQL & Hive Batch (Topic 10) -->
    <g transform="translate(15, 335)">
      <rect x="0" y="0" width="210" height="195" rx="8" fill="#500724" stroke="#f59e0b" stroke-width="1.5"/>
      <text x="12" y="22" fill="#fef3c7" font-size="11" font-weight="700">Batch Processing (Topic 10)</text>
      <text x="12" y="37" fill="#f59e0b" font-size="9.5">Spark SQL 4.2.0 &amp; Hive 4.2.1</text>
      <text x="12" y="55" fill="#94a3b8" font-size="9">• Broadcast Join 239 Grid Provinsi</text>
      <text x="12" y="70" fill="#94a3b8" font-size="9">• Partition Pruning (acq_date)</text>
      <text x="12" y="85" fill="#94a3b8" font-size="9">• Agregasi Harian &amp; Analitik FRP</text>
      <text x="12" y="100" fill="#94a3b8" font-size="9">• Apache Pig 0.18.0 ETL Pipeline</text>

      <rect x="12" y="115" width="186" height="60" rx="4" fill="#1e293b" stroke="#f59e0b" stroke-width="1"/>
      <text x="18" y="132" fill="#fbbf24" font-size="9" font-weight="700">Katalog Metastore:</text>
      <text x="18" y="148" fill="#e2e8f0" font-size="8">Apache Hive Metastore (Tez)</text>
      <text x="18" y="162" fill="#94a3b8" font-size="8">Melayani query JDBC/ODBC analitis</text>
    </g>
  </g>

  <!-- ==================== LAYER 4: STORAGE ==================== -->
  <g transform="translate(745, 110)">
    <rect x="0" y="0" width="210" height="550" rx="10" fill="url(#storageGrad)" stroke="#059669" stroke-width="1.5" filter="url(#shadow)"/>
    <text x="20" y="32" fill="#a7f3d0" font-size="13" font-weight="700">4. STORAGE LAYER</text>
    <line x1="20" y1="42" x2="190" y2="42" stroke="#059669" stroke-width="1"/>

    <!-- Parquet Sink Dataset -->
    <g transform="translate(15, 60)">
      <rect x="0" y="0" width="180" height="110" rx="8" fill="#022c22" stroke="#10b981" stroke-width="1.5"/>
      <text x="12" y="22" fill="#6ee7b7" font-size="11" font-weight="700">Parquet Data Lake</text>
      <text x="12" y="38" fill="#34d399" font-size="9.5">hasil/klik_per_menit/</text>
      <text x="12" y="54" fill="#94a3b8" font-size="9">Snappy Compressed Parquet</text>
      <text x="12" y="68" fill="#94a3b8" font-size="9">Metadata: _spark_metadata</text>
      <text x="12" y="85" fill="#a7f3d0" font-size="9" font-weight="600">Finalized Window Aggregations</text>
    </g>

    <!-- State Store & Checkpoint chk/klik -->
    <g transform="translate(15, 185)">
      <rect x="0" y="0" width="180" height="135" rx="8" fill="#022c22" stroke="#a855f7" stroke-width="1.5"/>
      <text x="12" y="22" fill="#e9d5ff" font-size="11" font-weight="700">Checkpoint (chk/klik)</text>
      <text x="12" y="38" fill="#c084fc" font-size="9.5">Write-Ahead Log (WAL) &amp; State</text>
      <text x="12" y="55" fill="#94a3b8" font-size="8.5">• offsets/ (Batch 0..N)</text>
      <text x="12" y="68" fill="#94a3b8" font-size="8.5">• commits/ (Committed Batches)</text>
      <text x="12" y="81" fill="#94a3b8" font-size="8.5">• sources/0/0 (Kafka Source Metadata)</text>
      <text x="12" y="94" fill="#94a3b8" font-size="8.5">• state/0/{0,1,2}/ (State Deltas)</text>
      <text x="12" y="118" fill="#a855f7" font-size="9" font-weight="600">Exactly-Once Recovery Guarantee</text>
    </g>

    <!-- Operational Storage HBase (Topic 9) -->
    <g transform="translate(15, 335)">
      <rect x="0" y="0" width="180" height="100" rx="8" fill="#022c22" stroke="#10b981" stroke-width="1.5"/>
      <text x="12" y="22" fill="#6ee7b7" font-size="11" font-weight="700">Apache HBase 2.6.7</text>
      <text x="12" y="38" fill="#34d399" font-size="9.5">Tabel: hotspot (Topic 9)</text>
      <text x="12" y="53" fill="#94a3b8" font-size="8.5">Rowkey: grid#tgl_rev#jam#id</text>
      <text x="12" y="68" fill="#94a3b8" font-size="8.5">Get titik: 0.073s | Scan: 0.967s</text>
      <text x="12" y="86" fill="#a7f3d0" font-size="9" font-weight="600">Operational Point-Lookup</text>
    </g>

    <!-- Parquet Partitioned Warehouse (Topic 10) -->
    <g transform="translate(15, 450)">
      <rect x="0" y="0" width="180" height="85" rx="8" fill="#022c22" stroke="#10b981" stroke-width="1.5"/>
      <text x="12" y="20" fill="#6ee7b7" font-size="10.5" font-weight="700">Parquet Warehouse (HDFS)</text>
      <text x="12" y="36" fill="#34d399" font-size="9">hotspot_part (Topic 10)</text>
      <text x="12" y="50" fill="#94a3b8" font-size="8.5">PARTITIONED BY (acq_date)</text>
      <text x="12" y="68" fill="#a7f3d0" font-size="8.5" font-weight="600">Analytical Archive (10k/100k)</text>
    </g>
  </g>

  <!-- ==================== LAYER 5: SERVING & APPLICATION ==================== -->
  <g transform="translate(980, 110)">
    <rect x="0" y="0" width="200" height="550" rx="10" fill="url(#servingGrad)" stroke="#a21caf" stroke-width="1.5" filter="url(#shadow)"/>
    <text x="20" y="32" fill="#f5d0fe" font-size="13" font-weight="700">5. SERVING &amp; UI</text>
    <line x1="20" y1="42" x2="180" y2="42" stroke="#a21caf" stroke-width="1"/>

    <!-- Serving A: Early Warning & Live Notification Engine -->
    <g transform="translate(15, 60)">
      <rect x="0" y="0" width="170" height="120" rx="8" fill="#4a044e" stroke="#e879f9" stroke-width="1.5"/>
      <text x="12" y="22" fill="#fae8ff" font-size="11" font-weight="700">Early Warning Alerts</text>
      <text x="12" y="38" fill="#f0abfc" font-size="9.5">Push Notification Notif</text>
      <text x="12" y="55" fill="#94a3b8" font-size="9">• Deteksi lonjakan titik api</text>
      <text x="12" y="70" fill="#94a3b8" font-size="9">• Alert FRP &gt;= 10 MW</text>
      <text x="12" y="85" fill="#94a3b8" font-size="9">• Window-based anomaly</text>
      <text x="12" y="104" fill="#e879f9" font-size="9" font-weight="600">Driven by Spark Stream</text>
    </g>

    <!-- Serving B: REST API & GIS Web Server -->
    <g transform="translate(15, 205)">
      <rect x="0" y="0" width="170" height="145" rx="8" fill="#4a044e" stroke="#e879f9" stroke-width="1.5"/>
      <text x="12" y="22" fill="#fae8ff" font-size="11" font-weight="700">FastAPI GIS Backend</text>
      <text x="12" y="38" fill="#f0abfc" font-size="9.5">REST API Services</text>
      <text x="12" y="55" fill="#94a3b8" font-size="9">• Query per Grid (HBase)</text>
      <text x="12" y="70" fill="#94a3b8" font-size="9">• GeoJSON Titik Hotspot</text>
      <text x="12" y="85" fill="#94a3b8" font-size="9">• Stream aggregation API</text>
      <text x="12" y="100" fill="#94a3b8" font-size="9">• Latensi sub-detik</text>
      <rect x="12" y="112" width="146" height="20" rx="4" fill="#86198f"/>
      <text x="18" y="126" fill="#ffffff" font-size="8.5" font-weight="600">GeoServer / Map API</text>
    </g>

    <!-- Serving C: BI & Operational Dashboard -->
    <g transform="translate(15, 375)">
      <rect x="0" y="0" width="170" height="150" rx="8" fill="#4a044e" stroke="#e879f9" stroke-width="1.5"/>
      <text x="12" y="22" fill="#fae8ff" font-size="11" font-weight="700">Dashboard Interaktif</text>
      <text x="12" y="38" fill="#f0abfc" font-size="9.5">Streamlit / Superset</text>
      <text x="12" y="55" fill="#94a3b8" font-size="9">• Peta Sebaran Panas</text>
      <text x="12" y="70" fill="#94a3b8" font-size="9">• Tren Hotspot Mingguan</text>
      <text x="12" y="85" fill="#94a3b8" font-size="9">• Top 10 Provinsi Rawan</text>
      <text x="12" y="100" fill="#94a3b8" font-size="9">• Monitoring Klik Real-Time</text>
      <rect x="12" y="115" width="146" height="22" rx="4" fill="#86198f"/>
      <text x="22" y="130" fill="#ffffff" font-size="8.5" font-weight="600">BMKG &amp; KLHK Command</text>
    </g>
  </g>

  <!-- ==================== FLOW ARROWS & DATA PATHS ==================== -->
  <!-- Stream 1: VIIRS Source -> Python Producer -->
  <path d="M 175 230 L 250 215" fill="none" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>

  <!-- Stream 2: Producer -> Kafka Broker Topic klik -->
  <path d="M 345 260 L 345 345" fill="none" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>

  <!-- Stream 3: Kafka Topic klik Partitions -> PySpark Structured Streaming -->
  <path d="M 435 380 C 460 380, 460 250, 495 250" fill="none" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>

  <!-- Stream 4: PySpark Streaming -> Parquet Sink -->
  <path d="M 705 280 C 725 280, 725 210, 760 210" fill="none" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>

  <!-- Stream 4b: PySpark Streaming State -> Checkpoint chk/klik (Bi-directional) -->
  <path d="M 705 325 C 725 325, 735 340, 760 340" fill="none" stroke="#a855f7" stroke-width="2" stroke-dasharray="4,3" marker-end="url(#arrowCheckpoint)"/>

  <!-- Stream 5: PySpark Streaming -> Early Warning Alert -->
  <path d="M 705 200 C 730 180, 940 180, 995 180" fill="none" stroke="#38bdf8" stroke-width="2.5" marker-end="url(#arrowStream)"/>

  <!-- Batch 1: Historical Data Source -> Topic 10 Batch Processing (Spark SQL / Hive) -->
  <path d="M 175 530 C 350 530, 350 530, 495 530" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrowBatch)"/>

  <!-- Batch 2: Batch Processing -> Parquet Warehouse & HBase -->
  <path d="M 705 470 C 725 470, 735 485, 760 485" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrowBatch)"/>
  <path d="M 705 490 C 725 490, 735 420, 760 420" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrowBatch)"/>

  <!-- Serving 1: Parquet & HBase -> REST API & Dashboard -->
  <path d="M 940 210 C 960 210, 960 270, 995 270" fill="none" stroke="#38bdf8" stroke-width="2" marker-end="url(#arrowStream)"/>
  <path d="M 940 420 C 960 420, 960 340, 995 340" fill="none" stroke="#10b981" stroke-width="2" marker-end="url(#arrowStream)"/>
  <path d="M 940 500 C 960 500, 960 470, 995 470" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrowBatch)"/>

  <!-- Footer Caption -->
  <text x="30" y="680" fill="#64748b" font-size="11">Diagram Arsitektur v0.3: Mengintegrasikan Sumber Data, Ingestion Kafka (KRaft), Processing (Spark Streaming &amp; Spark SQL/Hive), Storage (Parquet, Checkpoint WAL, HBase), dan Serving (Alerts, API, BI).</text>
</svg>"""
    return svg

def main():
    os.makedirs("report", exist_ok=True)
    svg_content = create_svg()
    svg_path = os.path.join("report", "diagram_v03.svg")
    with open(svg_path, "w", encoding="utf-8") as f:
        f.write(svg_content)
    print(f"Generated SVG: {svg_path} ({len(svg_content)} bytes)")

    # Also generate high-resolution PNG using reportlab or pillow / cairosvg if available
    # Or render via matplotlib / PIL
    try:
        from reportlab.graphics import renderPM
        from svglib.svglib import svg2rlg
        drawing = svg2rlg(svg_path)
        png_path = os.path.join("report", "diagram_v03.png")
        renderPM.drawToFile(drawing, png_path, fmt="PNG")
        print(f"Generated PNG via svglib: {png_path}")
    except Exception as e:
        print("svglib not available, trying matplotlib or PIL:", e)
        try:
            # Let's create high quality raster diagram with matplotlib as companion
            import matplotlib.pyplot as plt
            import matplotlib.patches as patches
            
            fig, ax = plt.subplots(figsize=(15, 8.5), facecolor='#0f172a')
            ax.set_facecolor('#0f172a')
            ax.set_xlim(0, 1200)
            ax.set_ylim(0, 700)
            ax.invert_yaxis()
            ax.axis('off')
            
            # Title
            title_box = patches.FancyBboxPatch((20, 20), 1160, 70, boxstyle="round,pad=5,rounding_size=10",
                                               ec="#334155", fc="#1e293b", lw=1.5)
            ax.add_patch(title_box)
            ax.text(45, 52, "DIAGRAM ARSITEKTUR v0.3 — END-TO-END BIG DATA PIPELINE", 
                    color="#f8fafc", fontsize=14, weight='bold')
            ax.text(45, 74, "Kasus: Sistem Pemantauan Titik Api (Hotspot) Kebakaran Hutan Indonesia (NASA FIRMS VIIRS) · Kelompok 6", 
                    color="#94a3b8", fontsize=10)
            
            # Legend
            legend_box = patches.FancyBboxPatch((820, 35), 340, 42, boxstyle="round,pad=3,rounding_size=6",
                                                ec="#475569", fc="#1e293b", lw=1)
            ax.add_patch(legend_box)
            ax.annotate("", xy=(860, 56), xytext=(835, 56),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2.5))
            ax.text(870, 60, "Aliran Streaming (Real-Time)", color="#e2e8f0", fontsize=8.5, weight='bold')
            ax.annotate("", xy=(1030, 56), xytext=(1005, 56),
                        arrowprops=dict(arrowstyle="->", color="#f59e0b", lw=2, linestyle="--"))
            ax.text(1040, 60, "Aliran Batch", color="#e2e8f0", fontsize=8.5, weight='bold')

            # 5 Layer Boxes
            layers = [
                ("1. DATA SOURCES", 20, 190, "#1e293b", "#0284c7", [
                    ("NASA FIRMS VIIRS\nSatelit Suomi-NPP/NOAA\n[Stream Hotspot NRT]", 60, 110, "#0369a1"),
                    ("Web App & Telemetri\nTopic 11 Clickstream\n[JSON (user_id, kategori)]", 190, 110, "#0369a1"),
                    ("Data Historis KLHK/BMKG\n239 Grid Provinsi (CSV)\n[Batch Ingestion: 24h]", 320, 150, "#b45309")
                ]),
                ("2. INGESTION (KAFKA)", 235, 220, "#1e1b4b", "#6366f1", [
                    ("Python Producer\nproducer.py (Laju 100-10k)\nKey: UTF-8 user_id", 60, 90, "#312e81"),
                    ("Kafka Broker 4.3.1 (KRaft)\nTopic 'klik' (3 Partisi)\n• Partisi 0 (Offset Log)\n• Partisi 1 (Offset Log)\n• Partisi 2 (Offset Log)\nRetention: 7 Hari (168 Jam)", 170, 290, "#312e81")
                ]),
                ("3. PROCESSING LAYER", 480, 240, "#500724", "#db2777", [
                    ("Spark Structured Streaming\nPySpark 4.2.0 (stream_job.py)\n• Deserialisasi JSON (waktu)\n• Watermark 2 Menit\n• 1-Min Tumbling Window\n• Sinks: Console & Parquet", 60, 220, "#831843"),
                    ("Batch Processing (Topic 10)\nSpark SQL 4.2.0 & Hive 4.2.1\n• Broadcast Join 239 Grid\n• Partition Pruning acq_date\n• Metastore & Apache Tez", 300, 180, "#831843")
                ]),
                ("4. STORAGE LAYER", 745, 210, "#022c22", "#059669", [
                    ("Parquet Data Lake\nhasil/klik_per_menit/\n• Snappy Parquet (Append)\n• Finalized Windows", 60, 100, "#064e3b"),
                    ("Checkpoint (chk/klik)\nWrite-Ahead Log (WAL)\n• offsets/ & commits/\n• state/0/{0,1,2}/ (Deltas)", 175, 110, "#4a044e"),
                    ("Apache HBase 2.6.7\nTabel: hotspot (Topic 9)\nRowkey: grid#tgl_rev#jam\nPoint Lookup < 0.1s", 300, 95, "#064e3b"),
                    ("Parquet Warehouse\nhotspot_part (Topic 10)\nPartitioned by acq_date", 410, 80, "#064e3b")
                ]),
                ("5. SERVING & UI", 980, 200, "#4a044e", "#a21caf", [
                    ("Early Warning Alerts\nPush Notifications\n• FRP >= 10 MW Alerts\n• Driven by Spark Stream", 60, 110, "#701a75"),
                    ("FastAPI GIS Backend\nREST API & GeoServer\n• Query Grid HBase\n• Stream Analytics API", 190, 130, "#701a75"),
                    ("Dashboard Interaktif\nStreamlit / Superset\n• Peta Hotspot Real-Time\n• BMKG & KLHK Command", 340, 140, "#701a75")
                ])
            ]

            for title, x, w, bg, ec, subitems in layers:
                box = patches.FancyBboxPatch((x, 110), w, 550, boxstyle="round,pad=4,rounding_size=10",
                                             ec=ec, fc=bg, lw=1.5)
                ax.add_patch(box)
                ax.text(x + 15, 138, title, color="#f8fafc", fontsize=10, weight='bold')
                ax.plot([x + 15, x + w - 15], [148, 148], color=ec, lw=1)
                
                for text, sy, sh, sbg in subitems:
                    sbox = patches.FancyBboxPatch((x + 10, sy + 50), w - 20, sh,
                                                  boxstyle="round,pad=3,rounding_size=6",
                                                  ec=ec, fc=sbg, lw=1)
                    ax.add_patch(sbox)
                    ax.text(x + 18, sy + 70, text, color="#f8fafc", fontsize=7.8, linespacing=1.3)

            # Draw Connecting Arrows
            # Stream: VIIRS -> Kafka Producer
            ax.annotate("", xy=(245, 160), xytext=(195, 160),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2.5))
            # Stream: Kafka Topic -> Spark Streaming
            ax.annotate("", xy=(490, 200), xytext=(445, 300),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2.5))
            # Stream: Spark Streaming -> Parquet Sink
            ax.annotate("", xy=(755, 160), xytext=(710, 200),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2.5))
            # Stream: Spark Streaming -> Checkpoint WAL
            ax.annotate("", xy=(755, 280), xytext=(710, 240),
                        arrowprops=dict(arrowstyle="->", color="#a855f7", lw=2, linestyle=":"))
            # Stream: Spark Streaming -> Early Warning
            ax.annotate("", xy=(990, 160), xytext=(710, 150),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2))
            # Batch: Data Source -> Spark SQL/Hive
            ax.annotate("", xy=(490, 480), xytext=(195, 480),
                        arrowprops=dict(arrowstyle="->", color="#f59e0b", lw=2, linestyle="--"))
            # Batch: Spark SQL/Hive -> Parquet Warehouse & HBase
            ax.annotate("", xy=(755, 420), xytext=(710, 480),
                        arrowprops=dict(arrowstyle="->", color="#f59e0b", lw=2, linestyle="--"))
            ax.annotate("", xy=(755, 500), xytext=(710, 500),
                        arrowprops=dict(arrowstyle="->", color="#f59e0b", lw=2, linestyle="--"))
            # Serving: Storage -> REST API & Dashboard
            ax.annotate("", xy=(990, 310), xytext=(945, 160),
                        arrowprops=dict(arrowstyle="->", color="#38bdf8", lw=2))
            ax.annotate("", xy=(990, 450), xytext=(945, 420),
                        arrowprops=dict(arrowstyle="->", color="#10b981", lw=2))

            # Footer
            ax.text(25, 680, "Diagram Arsitektur v0.3: Mengintegrasikan Sumber Data, Ingestion Kafka (KRaft), Processing (Spark Streaming & Spark SQL/Hive), Storage (Parquet, WAL, HBase), dan Serving (Alerts, API, BI).",
                    color="#64748b", fontsize=8.5)

            png_path = os.path.join("report", "diagram_v03.png")
            plt.savefig(png_path, dpi=200, bbox_inches='tight', facecolor='#0f172a')
            plt.close()
            print(f"Generated PNG via matplotlib: {png_path}")
        except Exception as ex2:
            print("matplotlib PNG export error:", ex2)

if __name__ == "__main__":
    main()
