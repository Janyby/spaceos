let mode = 'tree';

document.getElementById('btnTree').onclick = () => { mode = 'tree'; render(); };
document.getElementById('btnGraph').onclick = () => { mode = 'graph'; render(); };

async function load() {
  const r = await fetch('/api/world');
  return await r.json();
}

function nameOf(o) {
  return (o.properties && o.properties.name) ? o.properties.name : (o.external_id || o.id);
}

function buildTree(w) {
  const objects = w.objects || [];
  const spaces = w.spaces || [];
  const rels = w.relations || [];
  const spaceName = spaces.length ? spaces[0].name : '(no space)';
  const contained = new Set(rels.filter(r => r.relation_type === 'contains').map(r => r.target_object_id));
  const lines = [];
  lines.push(spaceName);
  const projects = objects.filter(o => o.type === 'Project');
  const top = objects.filter(o => !contained.has(o.id));
  for (const o of top) {
    lines.push(`├── ${o.type}: ${nameOf(o)}`);
    const children = rels.filter(r => r.source_object_id === o.id || (r.relation_type==='contains' && r.source_object_id===o.id))
      .map(r => objects.find(x => x.id === r.target_object_id)).filter(Boolean);
    for (const c of children) lines.push(`│   ├── ${c.type}: ${nameOf(c)}`);
  }
  return lines.join('\n');
}

function buildGraph(w) {
  const objects = w.objects || [];
  const rels = w.relations || [];
  const lines = [];
  for (const r of rels) {
    const s = objects.find(o => o.id === r.source_object_id);
    const t = objects.find(o => o.id === r.target_object_id);
    lines.push(`${s ? nameOf(s) : r.source_object_id} ──${r.relation_type}──> ${t ? nameOf(t) : r.target_object_id}`);
  }
  if (!lines.length) lines.push('(no relations)');
  return lines.join('\n');
}

async function render() {
  const w = await load();
  document.getElementById('view').textContent = mode === 'tree' ? buildTree(w) : buildGraph(w);
}

render();
setInterval(render, 2000);
