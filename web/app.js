const { createApp, defineComponent, ref, computed, reactive, onMounted, onUnmounted } = Vue;

const CONFIG_PATH     = '../config/nodes.json';
const WS_RECONNECT_MS = 3000;
const CMD_TIMEOUT_MS  = 8000;  // max espera per resposta d'un READ

const LAYER_META = {
  core: { id: 'l0', label: 'Layer 0 (Core)', order: 3 },
  l1:   { id: 'l1', label: 'Layer 1',        order: 2 },
  l2:   { id: 'l2', label: 'Layer 2',        order: 1 },
};

const LAYER_BY_INDEX = { 0: 'l0', 1: 'l1', 2: 'l2' };

function buildLayers(config) {
  const layers = [];
  for (const [key, meta] of Object.entries(LAYER_META)) {
    const nodes = config[key] ?? [];
    layers.push({
      id:     meta.id,
      label:  meta.label,
      order:  meta.order,
      servers: nodes.map((node, i) => ({
        id:     `${meta.id}s${i}`,
        name:   node.name,
        role:   key === 'core' ? 'Core' : (i === 0 ? 'Main' : 'Backup'),
        wsPort: node.port + 10000,
      })),
    });
  }
  return layers.sort((a, b) => a.order - b.order);
}

function parseCommands(content) {
  const sessions = [];
  let current = null;

  const TOKEN_RE = /b[012]?|c|r\(\d+\)|w\(\d+\s*,\s*\d+\)/g;

  for (const line of content.split('\n')) {
    for (const part of (line.match(TOKEN_RE) || [])) {
      if (part === 'b' || part === 'b0') {
        current = { layerIndex: 0, cmds: [] };
      } else if (part === 'b1') {
        current = { layerIndex: 1, cmds: [] };
      } else if (part === 'b2') {
        current = { layerIndex: 2, cmds: [] };
      } else if (part === 'c') {
        if (current) { sessions.push(current); current = null; }
      } else if (current) {
        const rMatch = part.match(/^r\((\d+)\)$/);
        const wMatch = part.match(/^w\((\d+)\s*,\s*(\d+)\)$/);
        if (rMatch) {
          current.cmds.push({ type: 'READ',  key: parseInt(rMatch[1]) });
        } else if (wMatch) {
          current.cmds.push({ type: 'WRITE', key: parseInt(wMatch[1]), value: parseInt(wMatch[2]) });
        }
      }
    }
  }
  return sessions;
}

const ServerTable = defineComponent({
  name: 'ServerTable',
  props: {
    server: { type: Object, required: true },
    data:   { type: Map,    default: () => new Map() },
  },
  setup(props) {
    const sortedRows = computed(() => {
      const entries = Array.from(props.data.entries());
      entries.sort((a, b) => {
        const na = Number(a[0]), nb = Number(b[0]);
        return (!isNaN(na) && !isNaN(nb)) ? na - nb : String(a[0]).localeCompare(String(b[0]));
      });
      return entries.map(([key, item]) => ({ key, value: item.value, version: item.version }));
    });
    return { sortedRows };
  },
  template: `
    <div class="server-card">
      <div class="server-card-header">
        <span class="server-name">{{ server.name }}</span>
        <span class="server-role">{{ server.role }}</span>
      </div>
      <div class="table-wrapper">
        <table class="server-table">
          <thead><tr><th>Key</th><th>Value</th><th>Version</th></tr></thead>
          <tbody>
            <tr v-if="sortedRows.length === 0" class="empty-row">
              <td colspan="3">No data</td>
            </tr>
            <tr v-for="row in sortedRows" :key="row.key">
              <td>{{ row.key }}</td><td>{{ row.value }}</td><td>{{ row.version }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `,
});

const LayerSection = defineComponent({
  name: 'LayerSection',
  components: { ServerTable },
  props: {
    layer:       { type: Object, required: true },
    serversData: { type: Object, required: true },
  },
  setup(props) {
    const getServerData = (id) => props.serversData[id] ?? new Map();
    return { getServerData };
  },
  template: `
    <section class="layer-section">
      <h2 class="layer-title">{{ layer.label }}</h2>
      <div class="server-rack">
        <server-table
          v-for="server in layer.servers"
          :key="server.id"
          :server="server"
          :data="getServerData(server.id)"
        ></server-table>
      </div>
    </section>
  `,
});

createApp({
  components: { LayerSection },

  setup() {
    const layers          = ref([]);
    const configError     = ref('');
    const serversData     = reactive({});
    const wsStatuses      = reactive({});
    const connections     = {};
    const reconnectTimers = {};

    const pendingReads  = {};   // serverId → [resolve, ...]
    const pendingWrites = {};   // serverId → [resolve, ...]

    async function loadConfig() {
      try {
        const res = await fetch(CONFIG_PATH);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const config = await res.json();
        const built = buildLayers(config);

        built.forEach(layer =>
          layer.servers.forEach(s => {
            serversData[s.id]   = new Map();
            wsStatuses[s.id]    = 'disconnected';
            pendingReads[s.id]  = [];
            pendingWrites[s.id] = [];
          })
        );

        layers.value = built;
        built.forEach(layer => layer.servers.forEach(s => connectServer(s)));
      } catch (e) {
        configError.value = `Could not load config: ${e.message}`;
      }
    }

    function handleMessage(serverId, raw) {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      if (msg.type === 'DATA') {
        const map = serversData[serverId];
        if (!map) return;
        map.set(msg.key, { value: msg.value, version: msg.version });
        serversData[serverId] = new Map(map);

      } else if (msg.type === 'CMD_RESP' || msg.type === 'CMD_UNKNOWN') {
        const queue = pendingReads[serverId];
        if (queue && queue.length > 0) {
          const resolve = queue.shift();
          resolve(msg);
        }

      } else if (msg.type === 'CMD_WRITE_ACK') {
        const queue = pendingWrites[serverId];
        if (queue && queue.length > 0) {
          const resolve = queue.shift();
          resolve(msg);
        }
      }
    }

    function connectServer(server) {
      wsStatuses[server.id] = 'connecting';
      const ws = new WebSocket(`ws://localhost:${server.wsPort}`);
      connections[server.id] = ws;

      ws.onopen    = () => { wsStatuses[server.id] = 'connected'; };
      ws.onmessage = (e) => handleMessage(server.id, e.data);
      ws.onclose   = () => {
        wsStatuses[server.id] = 'disconnected';
        reconnectTimers[server.id] = setTimeout(() => connectServer(server), WS_RECONNECT_MS);
      };
      ws.onerror   = () => ws.close();
    }

    onMounted(loadConfig);
    onUnmounted(() => {
      Object.values(reconnectTimers).forEach(clearTimeout);
      Object.values(connections).forEach(ws => ws.close());
    });

    const anyConnected = computed(() => Object.values(wsStatuses).some(v => v === 'connected'));

    function randomConnectedServer(layerId) {
      const layer = layers.value.find(l => l.id === layerId);
      if (!layer) return null;
      const up = layer.servers.filter(s => wsStatuses[s.id] === 'connected');
      if (!up.length) return null;
      return up[Math.floor(Math.random() * up.length)];
    }

    function sendRead(serverId, key) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const q = pendingReads[serverId];
          const i = q.indexOf(resolve);
          if (i !== -1) q.splice(i, 1);
          reject(new Error('timeout'));
        }, CMD_TIMEOUT_MS);

        pendingReads[serverId].push((msg) => {
          clearTimeout(timer);
          resolve(msg);
        });

        connections[serverId].send(JSON.stringify({ type: 'CMD_READ', key }));
      });
    }

    function sendWrite(serverId, key, value) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const q = pendingWrites[serverId];
          const i = q.indexOf(resolve);
          if (i !== -1) q.splice(i, 1);
          reject(new Error('timeout'));
        }, CMD_TIMEOUT_MS);

        pendingWrites[serverId].push((msg) => {
          clearTimeout(timer);
          resolve(msg);
        });

        connections[serverId].send(JSON.stringify({ type: 'CMD_WRITE', key, value }));
      });
    }

    const fileContent = ref('');
    const executing   = ref(false);

    async function executeSession(session) {
      const layerId = LAYER_BY_INDEX[session.layerIndex];
      const server  = randomConnectedServer(layerId);
      if (!server) return;

      for (const cmd of session.cmds) {
        if (cmd.type === 'WRITE') {
          try { await sendWrite(server.id, cmd.key, cmd.value); } catch {}
        } else if (cmd.type === 'READ') {
          try { await sendRead(server.id, cmd.key); } catch {}
        }
      }
    }

    async function sendFile() {
      if (executing.value || !fileContent.value.trim()) return;
      const sessions = parseCommands(fileContent.value);
      if (!sessions.length) return;

      executing.value = true;
      for (let i = 0; i < sessions.length; i++) {
        await executeSession(sessions[i]);
      }
      executing.value = false;
    }

    const canExecute = computed(() =>
      !executing.value && anyConnected.value && fileContent.value.trim().length > 0
    );

    return {
      layers,
      configError,
      serversData,
      fileContent,
      executing,
      canExecute,
      sendFile,
    };
  },
}).mount('#app');
