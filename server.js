const express = require('express');
const mysql   = require('mysql2/promise');
const cors    = require('cors');
const path    = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Pool de conexão MySQL ────────────────────────────────────────────────
const pool = mysql.createPool({
  host:            process.env.DB_HOST || '159.89.243.28',
  port:            process.env.DB_PORT || 3306,
  user:            process.env.DB_USER || 'trucadao',
  password:        process.env.DB_PASS || 'trucadao',
  database:        process.env.DB_NAME || 'trucadao',
  waitForConnections: true,
  connectionLimit: 10,
  timezone:        '-03:00',
});

// ── QUERIES (Ajustadas para coluna 'pessoa') ──────────────────────────────

// Revendas ativas (status = 1) separadas por pessoa J/F
const SQL_REVENDAS = `
  SELECT
    COUNT(*)                                            AS total_ativas,
    SUM(CASE WHEN pessoa = 'J' THEN 1 ELSE 0 END)        AS recorrentes,
    SUM(CASE WHEN pessoa = 'F' THEN 1 ELSE 0 END)        AS pagamento_unico,
    COUNT(*) * 99.90                                    AS mrr_bruto,
    SUM(CASE WHEN pessoa = 'J' THEN 99.90 ELSE 0 END)    AS mrr_recorrente
  FROM revendas
  WHERE status = 1
`;

// Anúncios por status
const SQL_ANUNCIOS = `
  SELECT
    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS ativos,
    SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS inativos,
    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS vendidos,
    COUNT(*)                                     AS total
  FROM anuncios
`;

// Evolução mensal de revendas ativas
const SQL_REVENDAS_MES = `
  SELECT
    DATE_FORMAT(created_at, '%b/%y')  AS mes,
    DATE_FORMAT(created_at, '%Y-%m')  AS mes_ordem,
    COUNT(*)                          AS novas,
    SUM(CASE WHEN pessoa = 'J' THEN 1 ELSE 0 END) AS novas_j,
    SUM(CASE WHEN pessoa = 'F' THEN 1 ELSE 0 END) AS novas_f
  FROM revendas
  WHERE created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(created_at, '%Y-%m'), DATE_FORMAT(created_at, '%b/%y')
  ORDER BY mes_ordem ASC
  LIMIT 12
`;

// Evolução mensal de anúncios publicados
const SQL_ANUNCIOS_MES = `
  SELECT
    DATE_FORMAT(created_at, '%b/%y') AS mes,
    DATE_FORMAT(created_at, '%Y-%m') AS mes_ordem,
    COUNT(*)                         AS publicados,
    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS vendidos
  FROM anuncios
  WHERE created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(created_at, '%Y-%m'), DATE_FORMAT(created_at, '%b/%y')
  ORDER BY mes_ordem ASC
  LIMIT 12
`;

// Cancelamentos mensais (churn)
const SQL_CHURN_MES = `
  SELECT
    DATE_FORMAT(updated_at, '%b/%y') AS mes,
    DATE_FORMAT(updated_at, '%Y-%m') AS mes_ordem,
    COUNT(*)                         AS cancelamentos
  FROM revendas
  WHERE status = 2
    AND updated_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(updated_at, '%Y-%m'), DATE_FORMAT(updated_at, '%b/%y')
  ORDER BY mes_ordem ASC
  LIMIT 12
`;

// ── ENDPOINT PRINCIPAL ───────────────────────────────────────────────────
app.get('/api/stats', async (req, res) => {
  let conn;
  try {
    conn = await pool.getConnection();

    const [[revendas]]    = await conn.query(SQL_REVENDAS);
    const [[anuncios]]    = await conn.query(SQL_ANUNCIOS);
    const [revendasMes]   = await conn.query(SQL_REVENDAS_MES);
    const [anunciosMes]   = await conn.query(SQL_ANUNCIOS_MES);
    const [churnMes]      = await conn.query(SQL_CHURN_MES);

    res.json({
      timestamp: new Date().toISOString(),
      revendas: {
        total_ativas:    Number(revendas.total_ativas)    || 0,
        recorrentes:     Number(revendas.recorrentes)     || 0,
        pagamento_unico: Number(revendas.pagamento_unico)|| 0,
        mrr_recorrente:  Number(revendas.mrr_recorrente) || 0,
        mrr_bruto:       Number(revendas.mrr_bruto)      || 0,
      },
      anuncios: {
        ativos:   Number(anuncios.ativos)   || 0,
        inativos: Number(anuncios.inativos) || 0,
        vendidos: Number(anuncios.vendidos) || 0,
        total:    Number(anuncios.total)    || 0,
      },
      historico: {
        revendas_mes:  revendasMes,
        anuncios_mes:  anunciosMes,
        churn_mes:     churnMes,
      },
    });

  } catch (err) {
    console.error('[API ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao consultar banco de dados', detail: err.message });
  } finally {
    if (conn) conn.release();
  }
});

// ── HEALTH CHECK ─────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// ── FALLBACK → index.html ────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── INICIALIZAÇÃO ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Trucadao API] rodando na porta ${PORT}`);
});
