// Stage 3B.2 — shared services for the contract lab (mock + proxy lifecycle).
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const MOCK_PORT = Number(process.env.S3B1_MOCK_PORT || 18771);
const PROXY_PORT = Number(process.env.S3B1_PROXY_PORT || 18773);

async function startServices() {
  // The CONTRACT-FAITHFUL mock (replaces the Stage 3B.1 permissive mock).
  const mock = spawn(process.execPath, [path.join(__dirname, '..', 'mock', 'contract_server.js')], {
    env: Object.assign({}, process.env, {
      S3B1_MOCK_PORT: String(MOCK_PORT),
      S3B1_WORLD: process.env.S3B1_WORLD || ''
    }),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  const proxy = spawn(process.execPath, [path.join(__dirname, '..', 'mock', 'proxy.js')], {
    env: Object.assign({}, process.env, { S3B1_PROXY_PORT: String(PROXY_PORT), S3B1_MOCK_PORT: String(MOCK_PORT) }),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let mockOut = '', proxyOut = '';
  mock.stdout.on('data', d => { mockOut += d; });
  mock.stderr.on('data', d => { mockOut += d; });
  proxy.stdout.on('data', d => { proxyOut += d; });
  proxy.stderr.on('data', d => { proxyOut += d; });

  const net = require('net');
  const up = port => new Promise(resolve => {
    const s = net.connect({ host: '127.0.0.1', port }, () => { s.destroy(); resolve(true); });
    s.on('error', () => resolve(false));
    s.setTimeout(1000, () => { s.destroy(); resolve(false); });
  });
  let ready = false;
  for (let i = 0; i < 40; i++) {
    if (await up(MOCK_PORT) && await up(PROXY_PORT)) { ready = true; break; }
    await sleep(150);
  }
  if (!ready) {
    try { mock.kill(); } catch (e) {}
    try { proxy.kill(); } catch (e) {}
    throw new Error('contract mock/proxy did not start\nmock: ' + mockOut + '\nproxy: ' + proxyOut);
  }
  return {
    mock, proxy, mockOut: () => mockOut, proxyOut: () => proxyOut,
    stop() { try { mock.kill(); } catch (e) {} try { proxy.kill(); } catch (e) {} }
  };
}

module.exports = { startServices, MOCK_PORT, PROXY_PORT };
