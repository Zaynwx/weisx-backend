// =====================================================
//  WEISX PRO - Servidor (contas, planos e PIX)
// =====================================================
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const {
  MONGODB_URI,          // link do banco MongoDB Atlas
  JWT_SECRET,           // uma senha grande e aleatória só sua
  MP_ACCESS_TOKEN,      // Access Token de PRODUÇÃO do Mercado Pago
  PUBLIC_URL,           // ex: https://weisx-pro-backend.onrender.com
  PORT = 3000,
} = process.env;

// ---------- PLANOS (mude os preços no Render, sem mexer no código) ----------
// PRECO_TESTE = 0 deixa o teste de 1 dia grátis (só 1 vez por conta)
const PLANOS = {
  teste:  { nome: 'Teste 1 dia', dias: 1,  preco: Number(process.env.PRECO_TESTE ?? 0)  },
  dias15: { nome: '15 dias',     dias: 15, preco: Number(process.env.PRECO_15    ?? 15) },
  dias30: { nome: '30 dias',     dias: 30, preco: Number(process.env.PRECO_30    ?? 25) },
};

// ---------- BANCO DE DADOS ----------
const Usuario = mongoose.model('Usuario', new mongoose.Schema({
  usuario:   { type: String, required: true, unique: true, lowercase: true, trim: true },
  email:     { type: String, required: true, unique: true, lowercase: true, trim: true },
  senhaHash: { type: String, required: true },
  acessoAte: { type: Date, default: null },
  usouTeste: { type: Boolean, default: false },
  criadoEm:  { type: Date, default: Date.now },
}));

const Pagamento = mongoose.model('Pagamento', new mongoose.Schema({
  mpId:      { type: String, required: true, unique: true },
  usuarioId: { type: mongoose.Schema.Types.ObjectId, required: true },
  plano:     { type: String, required: true },
  valor:     Number,
  status:    { type: String, default: 'pending' },
  creditado: { type: Boolean, default: false },
  criadoEm:  { type: Date, default: Date.now },
}));

// ---------- FUNÇÕES ----------
const DIA = 24 * 60 * 60 * 1000;

function gerarToken(u) {
  // Token dura 1 ano e é renovado toda vez que o app abre = login "pra sempre"
  return jwt.sign({ id: u._id.toString() }, JWT_SECRET, { expiresIn: '365d' });
}

function resumo(u) {
  const agora = Date.now();
  const ativo = !!u.acessoAte && u.acessoAte.getTime() > agora;
  return {
    usuario: u.usuario,
    email: u.email,
    acessoAte: u.acessoAte,
    ativo,
    diasRestantes: ativo ? Math.ceil((u.acessoAte.getTime() - agora) / DIA) : 0,
    usouTeste: u.usouTeste,
  };
}

// Se ainda tem dias, soma em cima. Se venceu, conta a partir de agora.
function liberar(u, dias) {
  const base = u.acessoAte && u.acessoAte.getTime() > Date.now() ? u.acessoAte.getTime() : Date.now();
  u.acessoAte = new Date(base + dias * DIA);
}

async function auth(req, res, next) {
  try {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    const { id } = jwt.verify(token, JWT_SECRET);
    req.user = await Usuario.findById(id);
    if (!req.user) throw new Error();
    next();
  } catch {
    res.status(401).json({ erro: 'Sessão expirada. Faça login de novo.' });
  }
}

async function verificarPagamento(mpId) {
  const pag = await Pagamento.findOne({ mpId: String(mpId) });
  if (!pag || pag.creditado) return pag;

  const r = await fetch(`https://api.mercadopago.com/v1/payments/${mpId}`, {
    headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` },
  });
  if (!r.ok) return pag;
  const mp = await r.json();
  pag.status = mp.status;

  if (mp.status === 'approved') {
    // Trava pra nunca creditar o mesmo pagamento 2 vezes
    const travado = await Pagamento.findOneAndUpdate(
      { mpId: pag.mpId, creditado: false },
      { creditado: true, status: 'approved' },
      { new: true }
    );
    if (travado) {
      const u = await Usuario.findById(pag.usuarioId);
      if (u) {
        liberar(u, PLANOS[pag.plano].dias);
        if (pag.plano === 'teste') u.usouTeste = true;
        await u.save();
      }
      return travado;
    }
  } else {
    await pag.save();
  }
  return pag;
}

// ---------- ROTAS ----------
const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (req, res) => res.send('WEISX PRO online'));

app.post('/cadastro', async (req, res) => {
  const usuario = String(req.body.usuario || '').trim().toLowerCase();
  const email = String(req.body.email || '').trim().toLowerCase();
  const senha = String(req.body.senha || '');

  if (!/^[a-z0-9_.]{3,20}$/.test(usuario))
    return res.status(400).json({ erro: 'Usuário: 3 a 20 letras/números, sem espaço.' });
  if (!/^\S+@\S+\.\S+$/.test(email))
    return res.status(400).json({ erro: 'E-mail inválido.' });
  if (senha.length < 6)
    return res.status(400).json({ erro: 'A senha precisa ter pelo menos 6 caracteres.' });

  if (await Usuario.findOne({ usuario })) return res.status(409).json({ erro: 'Esse usuário já existe.' });
  if (await Usuario.findOne({ email }))   return res.status(409).json({ erro: 'Esse e-mail já tem conta.' });

  const u = await Usuario.create({ usuario, email, senhaHash: await bcrypt.hash(senha, 10) });
  res.json({ token: gerarToken(u), conta: resumo(u) });
});

app.post('/login', async (req, res) => {
  const usuario = String(req.body.usuario || '').trim().toLowerCase();
  const senha = String(req.body.senha || '');
  const u = await Usuario.findOne({ $or: [{ usuario }, { email: usuario }] });
  if (!u || !(await bcrypt.compare(senha, u.senhaHash)))
    return res.status(401).json({ erro: 'Usuário ou senha incorretos.' });
  res.json({ token: gerarToken(u), conta: resumo(u) });
});

app.get('/me', auth, (req, res) => {
  res.json({ token: gerarToken(req.user), conta: resumo(req.user) });
});

app.get('/planos', (req, res) => {
  res.json({ planos: Object.entries(PLANOS).map(([id, p]) => ({ id, ...p })) });
});

app.post('/comprar', auth, async (req, res) => {
  const plano = String(req.body.plano || '');
  const p = PLANOS[plano];
  if (!p) return res.status(400).json({ erro: 'Plano inválido.' });
  if (plano === 'teste' && req.user.usouTeste)
    return res.status(400).json({ erro: 'Você já usou o teste de 1 dia.' });

  // Teste grátis: libera na hora
  if (p.preco <= 0) {
    liberar(req.user, p.dias);
    if (plano === 'teste') req.user.usouTeste = true;
    await req.user.save();
    return res.json({ liberado: true, conta: resumo(req.user) });
  }

  // Pago: gera PIX no Mercado Pago
  const r = await fetch('https://api.mercadopago.com/v1/payments', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${MP_ACCESS_TOKEN}`,
      'Content-Type': 'application/json',
      'X-Idempotency-Key': crypto.randomUUID(),
    },
    body: JSON.stringify({
      transaction_amount: p.preco,
      description: `WEISX PRO - ${p.nome}`,
      payment_method_id: 'pix',
      payer: { email: req.user.email },
      notification_url: `${PUBLIC_URL}/webhook/mercadopago`,
      external_reference: `${req.user._id}:${plano}`,
    }),
  });
  const mp = await r.json();
  if (!r.ok) {
    console.error('Erro Mercado Pago:', mp);
    return res.status(500).json({ erro: 'Não foi possível gerar o PIX. Tente de novo.' });
  }

  await Pagamento.create({
    mpId: String(mp.id), usuarioId: req.user._id, plano, valor: p.preco, status: mp.status,
  });

  const pix = mp.point_of_interaction.transaction_data;
  res.json({
    pagamentoId: String(mp.id),
    valor: p.preco,
    qrCode: pix.qr_code,
    qrCodeBase64: pix.qr_code_base64,
  });
});

app.get('/pagamento/:id', auth, async (req, res) => {
  const pag = await Pagamento.findOne({ mpId: req.params.id, usuarioId: req.user._id });
  if (!pag) return res.status(404).json({ erro: 'Pagamento não encontrado.' });
  const atual = await verificarPagamento(pag.mpId);
  const u = await Usuario.findById(req.user._id);
  res.json({ status: atual.status, conta: resumo(u) });
});

// Mercado Pago avisa aqui quando o PIX é pago
app.post('/webhook/mercadopago', async (req, res) => {
  res.sendStatus(200);
  const id = req.query['data.id'] || req.body?.data?.id || req.query.id;
  if (id) verificarPagamento(id).catch(e => console.error('Webhook:', e));
});

// ---------- INICIAR ----------
mongoose.connect(MONGODB_URI)
  .then(() => app.listen(PORT, () => console.log('WEISX PRO rodando na porta', PORT)))
  .catch(e => { console.error('Erro ao conectar no banco:', e); process.exit(1); });
