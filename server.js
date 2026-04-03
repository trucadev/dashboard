const express = require('express');
const mysql   = require('mysql2/promise');
const cors    = require('cors');
const path    = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

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

function montarUrlImagem(id, filename) {
  if (!id || !filename) return null;
  const name = filename.replace(/\.jpg$/, '');
  const padded = String(id).padStart(9, '0');
  const p1 = padded.slice(0, 3);
  const p2 = padded.slice(3, 6);
  const p3 = padded.slice(6, 9);
  return 'https://d2o450bmsmjkde.cloudfront.net/system/imagens_anuncios/imagems/' + p1 + '/' + p2 + '/' + p3 + '/normal/' + name + '.jpg';
}

const SQL_REVENDAS = `
  SELECT COUNT(*) AS total_ativas,
    SUM(CASE WHEN pessoa = 'J' THEN 1 ELSE 0 END) AS recorrentes,
    SUM(CASE WHEN pessoa = 'F' THEN 1 ELSE 0 END) AS pagamento_unico,
    COUNT(*) * 99.90 AS mrr_bruto,
    SUM(CASE WHEN pessoa = 'J' THEN 99.90 ELSE 0 END) AS mrr_recorrente
  FROM revendas WHERE status = 1`;

const SQL_ANUNCIOS = `
  SELECT SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS ativos,
    SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS inativos,
    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS vendidos,
    COUNT(*) AS total FROM anuncios`;

const SQL_REVENDAS_MES = `
  SELECT DATE_FORMAT(created_at, '%b/%y') AS mes, DATE_FORMAT(created_at, '%Y-%m') AS mes_ordem,
    COUNT(*) AS novas, SUM(CASE WHEN pessoa = 'J' THEN 1 ELSE 0 END) AS novas_j,
    SUM(CASE WHEN pessoa = 'F' THEN 1 ELSE 0 END) AS novas_f
  FROM revendas WHERE created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(created_at, '%Y-%m'), DATE_FORMAT(created_at, '%b/%y')
  ORDER BY mes_ordem ASC LIMIT 12`;

const SQL_ANUNCIOS_MES = `
  SELECT DATE_FORMAT(created_at, '%b/%y') AS mes, DATE_FORMAT(created_at, '%Y-%m') AS mes_ordem,
    COUNT(*) AS publicados, SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS vendidos
  FROM anuncios WHERE created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(created_at, '%Y-%m'), DATE_FORMAT(created_at, '%b/%y')
  ORDER BY mes_ordem ASC LIMIT 12`;

const SQL_CHURN_MES = `
  SELECT DATE_FORMAT(updated_at, '%b/%y') AS mes, DATE_FORMAT(updated_at, '%Y-%m') AS mes_ordem,
    COUNT(*) AS cancelamentos
  FROM revendas WHERE status = 2 AND updated_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(updated_at, '%Y-%m'), DATE_FORMAT(updated_at, '%b/%y')
  ORDER BY mes_ordem ASC LIMIT 12`;

async function salvarLeadHubSpot(lead) {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) throw new Error('HUBSPOT_TOKEN nao configurado');
  const body = {
    properties: {
      firstname: lead.nome || '', phone: lead.telefone || '', city: lead.cidade || '',
      hs_lead_status: 'NEW', lifecyclestage: 'lead',
      veiculo_interesse: lead.veiculo_interesse || '', servico_veiculo: lead.servico || '',
      urgencia_compra: lead.urgencia || '', precisa_financiamento: lead.financiamento || '',
      origem_lead: 'Trucadao Bot',
    },
  };
  const res = await fetch('https://api.hubapi.com/crm/v3/objects/contacts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.message || 'Erro HubSpot');
  return data;
}

// ── STATS ─────────────────────────────────────────────────────────────
app.get('/api/stats', async (req, res) => {
  let conn;
  try {
    conn = await pool.getConnection();
    const [[revendas]]  = await conn.query(SQL_REVENDAS);
    const [[anuncios]]  = await conn.query(SQL_ANUNCIOS);
    const [revendasMes] = await conn.query(SQL_REVENDAS_MES);
    const [anunciosMes] = await conn.query(SQL_ANUNCIOS_MES);
    const [churnMes]    = await conn.query(SQL_CHURN_MES);
    res.json({
      timestamp: new Date().toISOString(),
      revendas: {
        total_ativas: Number(revendas.total_ativas) || 0,
        recorrentes: Number(revendas.recorrentes) || 0,
        pagamento_unico: Number(revendas.pagamento_unico) || 0,
        mrr_recorrente: Number(revendas.mrr_recorrente) || 0,
        mrr_bruto: Number(revendas.mrr_bruto) || 0,
      },
      anuncios: {
        ativos: Number(anuncios.ativos) || 0,
        inativos: Number(anuncios.inativos) || 0,
        vendidos: Number(anuncios.vendidos) || 0,
        total: Number(anuncios.total) || 0,
      },
      historico: { revendas_mes: revendasMes, anuncios_mes: anunciosMes, churn_mes: churnMes },
    });
  } catch (err) {
    console.error('[STATS ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao consultar banco', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── FIPE ──────────────────────────────────────────────────────────────
app.get('/api/fipe/marcas', async (req, res) => {
  try {
    const resp = await fetch('https://brasilapi.com.br/api/fipe/marcas/v1/caminhoes');
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Erro ao consultar FIPE', detail: err.message });
  }
});

app.get('/api/fipe/preco/:codigo', async (req, res) => {
  try {
    const resp = await fetch('https://brasilapi.com.br/api/fipe/preco/v1/' + req.params.codigo);
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Erro ao consultar FIPE', detail: err.message });
  }
});

app.get('/api/fipe/modelos/:marca', async (req, res) => {
  try {
    const resp = await fetch('https://brasilapi.com.br/api/fipe/veiculos/v1/caminhoes/' + req.params.marca);
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Erro ao consultar FIPE', detail: err.message });
  }
});

// ── VEICULOS POR FILTROS ──────────────────────────────────────────────
app.get('/api/veiculos', async (req, res) => {
  const { marca, modelo, tipo, estado, preco_min, preco_max, subcategoria_id, limit = 10 } = req.query;
  let where = ['a.status = 1', 'a.deleted = 0'];
  let params = [];
  if (marca)           { where.push('ma.slug LIKE ?');        params.push('%' + marca + '%'); }
  if (modelo)          { where.push('mo.slug LIKE ?');        params.push('%' + modelo + '%'); }
  if (tipo)            { where.push('t.slug LIKE ?');         params.push('%' + tipo + '%'); }
  if (estado)          { where.push('e.sigla LIKE ?');        params.push('%' + estado + '%'); }
  if (preco_min)       { where.push('a.preco >= ?');          params.push(Number(preco_min)); }
  if (preco_max)       { where.push('a.preco <= ?');          params.push(Number(preco_max)); }
  if (subcategoria_id) { where.push('a.subcategoria_id = ?'); params.push(Number(subcategoria_id)); }
  const sql = `
    SELECT a.id, a.ano_modelo, a.preco, a.km, a.cidade, a.observacao, a.status,
      ma.nome AS marca, ma.slug AS marca_slug, mo.nome AS modelo, mo.slug AS modelo_slug,
      t.nome AS tipo, t.slug AS tipo_slug, e.nome AS estado, e.sigla AS estado_sigla,
      img.id AS img_id, img.imagem_file_name AS img_filename,
      CONCAT('https://www.trucadao.com.br/', t.slug, '/', ma.slug, '/', LOWER(e.sigla), '/', mo.slug, '/', a.id) AS url
    FROM anuncios a
    LEFT JOIN marcas ma ON ma.id = a.marca_id
    LEFT JOIN modelos mo ON mo.id = a.modelo_id
    LEFT JOIN tipos t ON t.id = a.tipo_id
    LEFT JOIN estados e ON e.id = a.estado_id
    LEFT JOIN imagens_anuncios img ON img.anuncio_id = a.id AND img.destaque = 1
    WHERE ${where.join(' AND ')}
    ORDER BY a.destaque DESC, a.created_at DESC LIMIT ?`;
  params.push(Number(limit));
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query(sql, params);
    const veiculos = rows.map(function(v) {
      return Object.assign({}, v, { imagem: montarUrlImagem(v.img_id, v.img_filename) });
    });
    res.json({ total: veiculos.length, veiculos });
  } catch (err) {
    console.error('[VEICULOS ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao buscar veiculos', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── VEICULOS RECENTES ─────────────────────────────────────────────────
app.get('/api/veiculos/recentes', async (req, res) => {
  const { horas = 24, limit = 5 } = req.query;
  const sql = `
    SELECT a.id, a.ano_modelo, a.preco, a.km, a.cidade, a.observacao, a.created_at,
      ma.nome AS marca, mo.nome AS modelo, t.nome AS tipo, t.slug AS tipo_slug,
      e.sigla AS estado_sigla,
      img.id AS img_id, img.imagem_file_name AS img_filename,
      CONCAT('https://www.trucadao.com.br/', t.slug, '/', ma.slug, '/', LOWER(e.sigla), '/', mo.slug, '/', a.id) AS url
    FROM anuncios a
    LEFT JOIN marcas ma ON ma.id = a.marca_id
    LEFT JOIN modelos mo ON mo.id = a.modelo_id
    LEFT JOIN tipos t ON t.id = a.tipo_id
    LEFT JOIN estados e ON e.id = a.estado_id
    LEFT JOIN imagens_anuncios img ON img.anuncio_id = a.id AND img.destaque = 1
    WHERE a.status = 1 AND a.deleted = 0
      AND a.created_at >= DATE_SUB(NOW(), INTERVAL ? HOUR)
    ORDER BY a.created_at DESC LIMIT ?`;
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query(sql, [Number(horas), Number(limit)]);
    const veiculos = rows.map(function(v) {
      return Object.assign({}, v, { imagem: montarUrlImagem(v.img_id, v.img_filename) });
    });
    res.json({ total: veiculos.length, veiculos });
  } catch (err) {
    console.error('[RECENTES ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao buscar recentes', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── VEICULO POR ID ────────────────────────────────────────────────────
app.get('/api/veiculos/:id', async (req, res) => {
  const { id } = req.params;
  const sql = `
    SELECT a.id, a.ano_modelo, a.preco, a.km, a.cidade, a.observacao, a.status, a.deleted,
      ma.nome AS marca, mo.nome AS modelo, t.nome AS tipo, t.slug AS tipo_slug,
      e.nome AS estado, e.sigla AS estado_sigla, ma.slug AS marca_slug, mo.slug AS modelo_slug,
      img.id AS img_id, img.imagem_file_name AS img_filename,
      CONCAT('https://www.trucadao.com.br/', t.slug, '/', ma.slug, '/', LOWER(e.sigla), '/', mo.slug, '/', a.id) AS url
    FROM anuncios a
    LEFT JOIN marcas ma ON ma.id = a.marca_id
    LEFT JOIN modelos mo ON mo.id = a.modelo_id
    LEFT JOIN tipos t ON t.id = a.tipo_id
    LEFT JOIN estados e ON e.id = a.estado_id
    LEFT JOIN imagens_anuncios img ON img.anuncio_id = a.id AND img.destaque = 1
    WHERE a.id = ? LIMIT 1`;
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query(sql, [id]);
    if (rows.length === 0) {
      return res.status(404).json({ disponivel: false, motivo: 'nao_encontrado', mensagem: 'Anuncio nao encontrado no Patio Digital.' });
    }
    const v = rows[0];
    const veiculo = Object.assign({}, v, { imagem: montarUrlImagem(v.img_id, v.img_filename) });
    if (v.status === 2 || v.status === 3 || v.deleted === 1) {
      return res.json({
        disponivel: false,
        motivo: v.status === 3 ? 'vendido' : 'inativo',
        mensagem: v.status === 3 ? 'Este veiculo ja foi vendido.' : 'Este anuncio esta inativo no momento.',
        veiculo: { marca: v.marca, modelo: v.modelo, tipo: v.tipo, ano_modelo: v.ano_modelo, imagem: veiculo.imagem },
      });
    }
    res.json({ disponivel: true, veiculo });
  } catch (err) {
    console.error('[VEICULO ID ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao buscar veiculo', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── STATS DE REVENDA ──────────────────────────────────────────────────
app.get('/api/revendas/:id/stats', async (req, res) => {
  const { id } = req.params;
  const sql = `
    SELECT r.id, r.nome,
      COUNT(a.id) AS total_anuncios,
      SUM(CASE WHEN a.status = 1 THEN 1 ELSE 0 END) AS anuncios_ativos,
      SUM(CASE WHEN a.status = 3 THEN 1 ELSE 0 END) AS anuncios_vendidos,
      COALESCE(SUM(ce_views.total_views), 0) AS total_views,
      COALESCE(SUM(ce_wpp.total_wpp), 0) AS total_whatsapp
    FROM revendas r
    LEFT JOIN anuncios a ON a.revenda_id = r.id AND a.deleted = 0
    LEFT JOIN (
      SELECT anuncio_id, SUM(count) AS total_views
      FROM contador_estatisticas WHERE tipo = 1
      GROUP BY anuncio_id
    ) ce_views ON ce_views.anuncio_id = a.id
    LEFT JOIN (
      SELECT anuncio_id, SUM(count) AS total_wpp
      FROM contador_estatisticas WHERE tipo = 10
      GROUP BY anuncio_id
    ) ce_wpp ON ce_wpp.anuncio_id = a.id
    WHERE r.id = ?
    GROUP BY r.id, r.nome`;
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query(sql, [id]);
    if (rows.length === 0) return res.status(404).json({ error: 'Revenda nao encontrada' });
    res.json(rows[0]);
  } catch (err) {
    console.error('[REVENDA STATS ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao buscar stats da revenda', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── SALVAR LEAD ───────────────────────────────────────────────────────
app.post('/api/leads', async (req, res) => {
  const { nome, telefone, cidade, veiculo_interesse, servico, urgencia, financiamento } = req.body;
  if (!telefone) return res.status(400).json({ error: 'Telefone e obrigatorio' });
  try {
    const hubspot = await salvarLeadHubSpot({ nome, telefone, cidade, veiculo_interesse, servico, urgencia, financiamento });
    res.json({ sucesso: true, mensagem: 'Lead salvo com sucesso!', hubspot_id: hubspot.id });
  } catch (err) {
    console.error('[LEAD ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao salvar lead', detail: err.message });
  }
});

// ── BLOG ──────────────────────────────────────────────────────────────
app.get('/api/blog/recente', async (req, res) => {
  const { limit = 1 } = req.query;
  const sql = `
    SELECT id, name, slug, seo_description, read_time, created_at
    FROM blog_posts
    ORDER BY created_at DESC
    LIMIT ?`;
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query(sql, [Number(limit)]);
    const posts = rows.map(function(p) {
      return Object.assign({}, p, {
        url: 'https://www.trucadao.com.br/blog/' + p.slug
      });
    });
    res.json({ total: posts.length, posts });
  } catch (err) {
    console.error('[BLOG ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao buscar blog', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── RELATORIO SEMANAL REVENDA ─────────────────────────────────────────
app.get('/api/revendas/:id/relatorio', async (req, res) => {
  const { id } = req.params;
  const sql = `
    SELECT
      r.id, r.nome, r.telefone,
      COUNT(a.id) AS total_anuncios,
      SUM(CASE WHEN a.status = 1 THEN 1 ELSE 0 END) AS ativos,
      SUM(CASE WHEN a.status = 3 THEN 1 ELSE 0 END) AS vendidos_semana,
      COALESCE(SUM(ce_views.views_semana), 0) AS views_semana,
      COALESCE(SUM(ce_wpp.wpp_semana), 0) AS whatsapp_semana
    FROM revendas r
    LEFT JOIN anuncios a ON a.revenda_id = r.id AND a.deleted = 0
    LEFT JOIN (
      SELECT anuncio_id, SUM(count) AS views_semana
      FROM contador_estatisticas
      WHERE tipo = 1
        AND YEARWEEK(STR_TO_DATE(CONCAT(ano, '-', mes, '-01'), '%Y-%m-%d'), 1) = YEARWEEK(NOW(), 1)
      GROUP BY anuncio_id
    ) ce_views ON ce_views.anuncio_id = a.id
    LEFT JOIN (
      SELECT anuncio_id, SUM(count) AS wpp_semana
      FROM contador_estatisticas
      WHERE tipo = 10
        AND YEARWEEK(STR_TO_DATE(CONCAT(ano, '-', mes, '-01'), '%Y-%m-%d'), 1) = YEARWEEK(NOW(), 1)
      GROUP BY anuncio_id
    ) ce_wpp ON ce_wpp.anuncio_id = a.id
    WHERE r.id = ?
    GROUP BY r.id, r.nome, r.telefone`;

  const sqlTopAnuncios = `
    SELECT
      a.id, mo.nome AS modelo, ma.nome AS marca, a.preco, a.ano_modelo,
      COALESCE(SUM(ce.count), 0) AS views_semana,
      CONCAT('https://www.trucadao.com.br/', t.slug, '/', ma.slug, '/', LOWER(e.sigla), '/', mo.slug, '/', a.id) AS url
    FROM anuncios a
    LEFT JOIN marcas ma ON ma.id = a.marca_id
    LEFT JOIN modelos mo ON mo.id = a.modelo_id
    LEFT JOIN tipos t ON t.id = a.tipo_id
    LEFT JOIN estados e ON e.id = a.estado_id
    LEFT JOIN contador_estatisticas ce ON ce.anuncio_id = a.id AND ce.tipo = 1
      AND YEARWEEK(STR_TO_DATE(CONCAT(ce.ano, '-', ce.mes, '-01'), '%Y-%m-%d'), 1) = YEARWEEK(NOW(), 1)
    WHERE a.revenda_id = ? AND a.status = 1 AND a.deleted = 0
    GROUP BY a.id, mo.nome, ma.nome, a.preco, a.ano_modelo, t.slug, ma.slug, e.sigla, mo.slug
    ORDER BY views_semana DESC
    LIMIT 3`;

  const sqlSemViews = `
    SELECT COUNT(*) AS sem_views
    FROM anuncios a
    LEFT JOIN contador_estatisticas ce ON ce.anuncio_id = a.id AND ce.tipo = 1
      AND ce.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
    WHERE a.revenda_id = ? AND a.status = 1 AND a.deleted = 0
    GROUP BY a.id
    HAVING SUM(COALESCE(ce.count, 0)) = 0`;

  let conn;
  try {
    conn = await pool.getConnection();
    const [[revenda]] = await conn.query(sql, [id]);
    if (!revenda) return res.status(404).json({ error: 'Revenda nao encontrada' });
    const [topAnuncios] = await conn.query(sqlTopAnuncios, [id]);
    const [semViews] = await conn.query(sqlSemViews, [id]);
    res.json({
      revenda: {
        id: revenda.id,
        nome: revenda.nome,
        telefone: revenda.telefone,
      },
      semana: {
        views: Number(revenda.views_semana) || 0,
        whatsapp: Number(revenda.whatsapp_semana) || 0,
        vendidos: Number(revenda.vendidos_semana) || 0,
        total_anuncios: Number(revenda.total_anuncios) || 0,
        taxa_conversao: revenda.views_semana > 0
          ? ((revenda.whatsapp_semana / revenda.views_semana) * 100).toFixed(1)
          : '0.0',
        anuncios_sem_views: semViews.length,
      },
      top_anuncios: topAnuncios,
    });
  } catch (err) {
    console.error('[RELATORIO ERROR]', err.message);
    res.status(500).json({ error: 'Erro ao gerar relatorio', detail: err.message });
  } finally { if (conn) conn.release(); }
});

// ── HEALTH ────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// ── STATIC + FALLBACK ─────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, '0.0.0.0', () => {
  console.log('[Trucadao API] rodando na porta ' + PORT);
});
