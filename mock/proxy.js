// Stage 3B.1 — fail-closed network proxy (defence layer 1).
//
// Chrome is launched with:
//     --proxy-server=http://127.0.0.1:<PROXY_PORT>
//     --proxy-bypass-list=<-loopback>     (removes Chrome's implicit loopback bypass)
// so EVERY request — including to 127.0.0.1 — is routed through this proxy.
//
// This proxy allows exactly one origin (the isolated mock) and refuses
// everything else with a 403, before any socket to the target is opened.
//
// This is a raw TCP tunnel: the first request bytes are inspected, the
// destination is validated, and only then is a socket opened. It therefore
// covers plain HTTP and the WebSocket upgrade uniformly.

const net = require('net');
const http = require('http');

const PROXY_PORT = Number(process.env.S3B1_PROXY_PORT || 18773);
const HOST = '127.0.0.1';
const MOCK_PORT = Number(process.env.S3B1_MOCK_PORT || 18771);

// The single allowed origin.
const ALLOWED = new Set(['127.0.0.1:' + MOCK_PORT]);

const attempts = [];
const refused = [];
const allowed = [];

function record(entry) { attempts.push(Object.assign({ t: Date.now() }, entry)); }

function parseTarget(head) {
  // head = the first line(s) of the request
  const line = head.split('\r\n')[0] || '';
  const m = line.match(/^(\w+)\s+(\S+)\s+HTTP\//);
  if (!m) return null;
  const method = m[1];
  const target = m[2];
  if (method === 'CONNECT') {
    // CONNECT host:port  (used by Chrome for WebSocket-through-proxy)
    const i = target.lastIndexOf(':');
    const host = i >= 0 ? target.slice(0, i) : target;
    const port = i >= 0 ? parseInt(target.slice(i + 1), 10) : 443;
    return { method, host, port, path: '', isConnect: true };
  }
  try {
    const u = new URL(target);
    return { method, host: u.hostname, port: Number(u.port || (u.protocol === 'https:' ? 443 : 80)), path: u.pathname + u.search, scheme: u.protocol };
  } catch (e) {
    return null;
  }
}

const server = net.createServer(client => {
  let buf = Buffer.alloc(0);
  let decided = false;

  const onData = chunk => {
    if (decided) return;
    buf = Buffer.concat([buf, chunk]);
    // Need at least the request line plus headers to be safe.
    const headerEnd = buf.indexOf('\r\n\r\n');
    if (headerEnd < 0) {
      if (buf.length > 64 * 1024) { client.destroy(); }
      return;
    }
    decided = true;
    client.removeListener('data', onData);

    const head = buf.slice(0, headerEnd).toString('latin1');
    const t = parseTarget(head);
    const key = t ? (t.host + ':' + t.port) : 'unknown';
    const isAllowed = !!t && ALLOWED.has(key);

    record({ method: t ? t.method : '?', target: t ? (t.path || key) : head.split('\r\n')[0], host: t ? t.host : '?', port: t ? t.port : 0, allowed: isAllowed });

    if (!isAllowed) {
      refused.push({ target: head.split('\r\n')[0], host: t ? t.host : '?', port: t ? t.port : 0 });
      const body = 'PROXY BLOCKED (fail-closed): destination not allowed';
      client.write(
        'HTTP/1.1 403 Forbidden\r\n' +
        'Content-Type: text/plain; charset=utf-8\r\n' +
        'Content-Length: ' + Buffer.byteLength(body) + '\r\n' +
        'Connection: close\r\n\r\n' + body);
      client.end();
      return;
    }

    allowed.push({ method: t.method, host: t.host, port: t.port, path: t.path });

    // Tunnel to the allowed origin.
    //
    // Destination semantics: as in any HTTP proxy, the destination is fixed by
    // the FIRST request on a connection, and every subsequent request on that
    // connection (keep-alive) is forwarded to the SAME validated host:port.
    // A tunnel therefore cannot be re-pointed at another destination, which is
    // the security property that matters. Scanning later requests is neither
    // necessary (the destination cannot change) nor safe (it would corrupt
    // binary WebSocket frames), so the tunnel forwards raw bytes.
    const up = net.connect(t.port, t.host, () => {
      if (t.isConnect) {
        // CONNECT tunnel (used for WebSocket-through-proxy): acknowledge, then
        // forward raw bytes in both directions.
        client.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        if (buf.length > headerEnd + 4) up.write(buf.slice(headerEnd + 4));
        return;
      }
      // Rewrite the request line to origin-form ("METHOD /path HTTP/1.1") and
      // forward the headers unchanged.
      const rawLines = head.split('\r\n');
      const reqLine = rawLines[0];
      const parts = reqLine.split(' ');
      const method = parts[0];
      const proto = parts[parts.length - 1];
      const pathOnly = (t.path && t.path.length) ? t.path : '/';
      const newFirstLine = method + ' ' + pathOnly + ' ' + proto;
      const headerBlock = rawLines.slice(1).filter(l => !/^proxy-connection:/i.test(l)).join('\r\n');
      const rest = buf.slice(headerEnd + 4);
      up.write(newFirstLine + '\r\n' + headerBlock + '\r\n\r\n');
      if (rest.length) up.write(rest);
    });
    up.on('error', () => { try { client.destroy(); } catch (e) {} });
    client.on('error', () => { try { up.destroy(); } catch (e) {} });
    client.pipe(up); up.pipe(client);
  };

  client.on('data', onData);
  client.on('error', () => {});
});

server.listen(PROXY_PORT, HOST, () => {
  console.log('[proxy] fail-closed proxy listening on ' + HOST + ':' + PROXY_PORT);
  console.log('[proxy] allowed origin: 127.0.0.1:' + MOCK_PORT);
  console.log('[proxy] every other destination is refused with 403 before any socket is opened');
});

// A tiny control surface for the test to read the proxy's own record.
const ctl = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ attempts, refused, allowed }));
});
ctl.listen(PROXY_PORT + 1, HOST, () => {
  console.log('[proxy] control surface on ' + HOST + ':' + (PROXY_PORT + 1));
});

process.on('SIGTERM', () => { server.close(); ctl.close(); process.exit(0); });
process.on('SIGINT', () => { server.close(); ctl.close(); process.exit(0); });
